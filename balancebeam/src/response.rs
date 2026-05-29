use std::fmt;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_HEADERS_SIZE: usize = 8000;
const MAX_BODY_SIZE: usize = 10000000;
const MAX_NUM_HEADERS: usize = 32;

#[derive(Debug)]
pub enum Error {
    IncompleteResponse,
    MalformedResponse(httparse::Error),
    InvalidContentLength,
    ContentLengthMismatch,
    ResponseBodyTooLarge,
    ConnectionError(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::IncompleteResponse => write!(f, "incomplete response"),
            Error::MalformedResponse(err) => write!(f, "malformed response: {}", err),
            Error::InvalidContentLength => write!(f, "invalid Content-Length header"),
            Error::ContentLengthMismatch => {
                write!(f, "response body length did not match Content-Length")
            }
            Error::ResponseBodyTooLarge => write!(f, "response body exceeded maximum allowed size"),
            Error::ConnectionError(err) => write!(f, "connection error: {}", err),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::MalformedResponse(err) => Some(err),
            Error::ConnectionError(err) => Some(err),
            _ => None,
        }
    }
}

fn get_content_length(response: &http::Response<Vec<u8>>) -> Result<Option<usize>, Error> {
    if let Some(header_value) = response.headers().get("content-length") {
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

fn parse_response(buffer: &[u8]) -> Result<Option<(http::Response<Vec<u8>>, usize)>, Error> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_NUM_HEADERS];
    let mut resp = httparse::Response::new(&mut headers);
    let res = resp
        .parse(buffer)
        .or_else(|err| Err(Error::MalformedResponse(err)))?;

    if let httparse::Status::Complete(len) = res {
        let mut response = http::Response::builder()
            .status(resp.code.unwrap())
            .version(http::Version::HTTP_11);
        for header in resp.headers {
            response = response.header(header.name, header.value);
        }
        let response = response.body(Vec::new()).unwrap();
        Ok(Some((response, len)))
    } else {
        Ok(None)
    }
}

async fn read_headers(stream: &mut TcpStream) -> Result<http::Response<Vec<u8>>, Error> {
    let mut response_buffer = [0_u8; MAX_HEADERS_SIZE];
    let mut bytes_read = 0;
    loop {
        let new_bytes = stream
            .read(&mut response_buffer[bytes_read..])
            .await
            .or_else(|err| Err(Error::ConnectionError(err)))?;
        if new_bytes == 0 {
            return Err(Error::IncompleteResponse);
        }
        bytes_read += new_bytes;

        if let Some((mut response, headers_len)) = parse_response(&response_buffer[..bytes_read])? {
            response
                .body_mut()
                .extend_from_slice(&response_buffer[headers_len..bytes_read]);
            return Ok(response);
        }
    }
}

async fn read_body(
    stream: &mut TcpStream,
    response: &mut http::Response<Vec<u8>>,
) -> Result<(), Error> {
    let content_length = get_content_length(response)?;

    while content_length.is_none() || response.body().len() < content_length.unwrap() {
        let mut buffer = [0_u8; 512];
        let bytes_read = stream
            .read(&mut buffer)
            .await
            .or_else(|err| Err(Error::ConnectionError(err)))?;
        if bytes_read == 0 {
            if content_length.is_none() {
                break;
            } else {
                return Err(Error::ContentLengthMismatch);
            }
        }

        if content_length.is_some() && response.body().len() + bytes_read > content_length.unwrap()
        {
            return Err(Error::ContentLengthMismatch);
        }

        if response.body().len() + bytes_read > MAX_BODY_SIZE {
            return Err(Error::ResponseBodyTooLarge);
        }

        response.body_mut().extend_from_slice(&buffer[..bytes_read]);
    }
    Ok(())
}

pub async fn read_from_stream(
    stream: &mut TcpStream,
    request_method: &http::Method,
) -> Result<http::Response<Vec<u8>>, Error> {
    let mut response = read_headers(stream).await?;
    if !(request_method == http::Method::HEAD
        || response.status().as_u16() < 200
        || response.status() == http::StatusCode::NO_CONTENT
        || response.status() == http::StatusCode::NOT_MODIFIED)
    {
        read_body(stream, &mut response).await?;
    }
    Ok(response)
}

pub async fn write_to_stream(
    response: &http::Response<Vec<u8>>,
    stream: &mut TcpStream,
) -> Result<(), std::io::Error> {
    stream
        .write_all(&format_response_line(response).into_bytes())
        .await?;
    stream.write_all(&['\r' as u8, '\n' as u8]).await?;
    for (header_name, header_value) in response.headers() {
        stream
            .write_all(format!("{}: ", header_name).as_bytes())
            .await?;
        stream.write_all(header_value.as_bytes()).await?;
        stream.write_all(&['\r' as u8, '\n' as u8]).await?;
    }
    stream.write_all(&['\r' as u8, '\n' as u8]).await?;
    if !response.body().is_empty() {
        stream.write_all(response.body()).await?;
    }
    Ok(())
}

pub fn format_response_line(response: &http::Response<Vec<u8>>) -> String {
    format!(
        "{:?} {} {}",
        response.version(),
        response.status().as_str(),
        response.status().canonical_reason().unwrap_or("")
    )
}

pub fn make_http_error(status: http::StatusCode) -> http::Response<Vec<u8>> {
    let body = format!(
        "HTTP {} {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    )
    .into_bytes();
    http::Response::builder()
        .status(status)
        .header("Content-Type", "text/plain")
        .header("Content-Length", body.len().to_string())
        .version(http::Version::HTTP_11)
        .body(body)
        .unwrap()
}
