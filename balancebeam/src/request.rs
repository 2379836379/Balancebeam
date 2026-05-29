use std::cmp::min;
use std::fmt;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_HEADERS_SIZE: usize = 8000;
const MAX_BODY_SIZE: usize = 10000000;
const MAX_NUM_HEADERS: usize = 32;

#[derive(Debug)]
pub enum Error {
    IncompleteRequest(usize),
    MalformedRequest(httparse::Error),
    InvalidContentLength,
    ContentLengthMismatch,
    RequestBodyTooLarge,
    ConnectionError(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::IncompleteRequest(bytes_read) => {
                write!(f, "incomplete request after reading {} bytes", bytes_read)
            }
            Error::MalformedRequest(err) => write!(f, "malformed request: {}", err),
            Error::InvalidContentLength => write!(f, "invalid Content-Length header"),
            Error::ContentLengthMismatch => {
                write!(f, "request body length did not match Content-Length")
            }
            Error::RequestBodyTooLarge => write!(f, "request body exceeded maximum allowed size"),
            Error::ConnectionError(err) => write!(f, "connection error: {}", err),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::MalformedRequest(err) => Some(err),
            Error::ConnectionError(err) => Some(err),
            _ => None,
        }
    }
}

fn get_content_length(request: &http::Request<Vec<u8>>) -> Result<Option<usize>, Error> {
    if let Some(header_value) = request.headers().get("content-length") {
        Ok(Some(
            header_value
                .to_str()
                .or(Err(Error::InvalidContentLength))?
                .parse::<usize>()
                .or(Err(Error::InvalidContentLength))?,
        ))
    } else {
        Ok(None)
    }
}

pub fn extend_header_value(
    request: &mut http::Request<Vec<u8>>,
    name: &'static str,
    extend_value: &str,
) {
    let new_value = match request.headers().get(name) {
        Some(existing_value) => {
            [existing_value.as_bytes(), b", ", extend_value.as_bytes()].concat()
        }
        None => extend_value.as_bytes().to_owned(),
    };
    request
        .headers_mut()
        .insert(name, http::HeaderValue::from_bytes(&new_value).unwrap());
}

fn parse_request(buffer: &[u8]) -> Result<Option<(http::Request<Vec<u8>>, usize)>, Error> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_NUM_HEADERS];
    let mut req = httparse::Request::new(&mut headers);
    let res = req.parse(buffer).or_else(|err| Err(Error::MalformedRequest(err)))?;

    if let httparse::Status::Complete(len) = res {
        let mut request = http::Request::builder()
            .method(req.method.unwrap())
            .uri(req.path.unwrap())
            .version(http::Version::HTTP_11);
        for header in req.headers {
            request = request.header(header.name, header.value);
        }
        let request = request.body(Vec::new()).unwrap();
        Ok(Some((request, len)))
    } else {
        Ok(None)
    }
}

async fn read_headers(stream: &mut TcpStream) -> Result<http::Request<Vec<u8>>, Error> {
    let mut request_buffer = [0_u8; MAX_HEADERS_SIZE];
    let mut bytes_read = 0;
    loop {
        let new_bytes = stream
            .read(&mut request_buffer[bytes_read..])
            .await
            .or_else(|err| Err(Error::ConnectionError(err)))?;
        if new_bytes == 0 {
            return Err(Error::IncompleteRequest(bytes_read));
        }
        bytes_read += new_bytes;

        if let Some((mut request, headers_len)) = parse_request(&request_buffer[..bytes_read])? {
            request
                .body_mut()
                .extend_from_slice(&request_buffer[headers_len..bytes_read]);
            return Ok(request);
        }
    }
}

async fn read_body(
    stream: &mut TcpStream,
    request: &mut http::Request<Vec<u8>>,
    content_length: usize,
) -> Result<(), Error> {
    while request.body().len() < content_length {
        let mut buffer = vec![0_u8; min(512, content_length)];
        let bytes_read = stream
            .read(&mut buffer)
            .await
            .or_else(|err| Err(Error::ConnectionError(err)))?;

        if bytes_read == 0 {
            log::debug!(
                "Client hung up after sending a body of length {}, even though it said the content length is {}",
                request.body().len(),
                content_length
            );
            return Err(Error::ContentLengthMismatch);
        }

        if request.body().len() + bytes_read > content_length {
            log::debug!("Client sent more bytes than we expected based on the given content length!");
            return Err(Error::ContentLengthMismatch);
        }

        request.body_mut().extend_from_slice(&buffer[..bytes_read]);
    }
    Ok(())
}

pub async fn read_from_stream(stream: &mut TcpStream) -> Result<http::Request<Vec<u8>>, Error> {
    let mut request = read_headers(stream).await?;
    if let Some(content_length) = get_content_length(&request)? {
        if content_length > MAX_BODY_SIZE {
            return Err(Error::RequestBodyTooLarge);
        } else {
            read_body(stream, &mut request, content_length).await?;
        }
    }
    Ok(request)
}

pub async fn write_to_stream(
    request: &http::Request<Vec<u8>>,
    stream: &mut TcpStream,
) -> Result<(), std::io::Error> {
    stream.write_all(&format_request_line(request).into_bytes()).await?;
    stream.write_all(&['\r' as u8, '\n' as u8]).await?;
    for (header_name, header_value) in request.headers() {
        stream
            .write_all(format!("{}: ", header_name).as_bytes())
            .await?;
        stream.write_all(header_value.as_bytes()).await?;
        stream.write_all(&['\r' as u8, '\n' as u8]).await?;
    }
    stream.write_all(&['\r' as u8, '\n' as u8]).await?;
    if !request.body().is_empty() {
        stream.write_all(request.body()).await?;
    }
    Ok(())
}

pub fn format_request_line(request: &http::Request<Vec<u8>>) -> String {
    format!("{} {} {:?}", request.method(), request.uri(), request.version())
}
