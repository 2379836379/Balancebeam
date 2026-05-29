use http::{HeaderMap, Response, StatusCode, Version};

use crate::state::ProxyState;

#[derive(Clone)]
pub(crate) struct CachedResponse {
    pub(crate) status: StatusCode,
    pub(crate) version: Version,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Vec<u8>,
}

pub(crate) fn clone_response(response: &Response<Vec<u8>>) -> Response<Vec<u8>> {
    let mut builder = Response::builder()
        .status(response.status())
        .version(response.version());
    for (header_name, header_value) in response.headers() {
        builder = builder.header(header_name, header_value);
    }
    builder.body(response.body().clone()).unwrap()
}

pub(crate) fn cache_key_for_request(request: &http::Request<Vec<u8>>) -> Option<String> {
    if request.method() != http::Method::GET || !request.body().is_empty() {
        return None;
    }
    if request.headers().contains_key("authorization") || request.headers().contains_key("cookie") {
        return None;
    }
    if let Some(cache_control) = request.headers().get("cache-control") {
        if let Ok(value) = cache_control.to_str() {
            let lower = value.to_ascii_lowercase();
            if lower.contains("no-cache") || lower.contains("no-store") {
                return None;
            }
        }
    }
    Some(request.uri().to_string())
}

fn response_is_cacheable(response: &Response<Vec<u8>>) -> bool {
    if response.status() != StatusCode::OK {
        return false;
    }
    if response.headers().contains_key("set-cookie") {
        return false;
    }
    if let Some(cache_control) = response.headers().get("cache-control") {
        if let Ok(value) = cache_control.to_str() {
            let lower = value.to_ascii_lowercase();
            if lower.contains("no-store") || lower.contains("no-cache") || lower.contains("private")
            {
                return false;
            }
        }
    }
    true
}

fn response_to_cached(response: &Response<Vec<u8>>) -> CachedResponse {
    CachedResponse {
        status: response.status(),
        version: response.version(),
        headers: response.headers().clone(),
        body: response.body().clone(),
    }
}

fn cached_to_response(cached: &CachedResponse) -> Response<Vec<u8>> {
    let mut builder = Response::builder()
        .status(cached.status)
        .version(cached.version);
    for (header_name, header_value) in &cached.headers {
        builder = builder.header(header_name, header_value);
    }
    builder.body(cached.body.clone()).unwrap()
}

pub(crate) fn try_get_cached_response(
    state: &ProxyState,
    cache_key: &Option<String>,
) -> Option<Response<Vec<u8>>> {
    let key = cache_key.as_ref()?;
    let mut cache = state.response_cache.lock();
    let cached = cache.as_mut()?.get(key)?.clone();
    Some(cached_to_response(&cached))
}

pub(crate) fn maybe_store_cached_response(
    state: &ProxyState,
    cache_key: &Option<String>,
    response: &Response<Vec<u8>>,
) {
    let Some(key) = cache_key.as_ref() else {
        return;
    };
    if !response_is_cacheable(response) {
        return;
    }
    let mut cache = state.response_cache.lock();
    if let Some(cache) = cache.as_mut() {
        cache.put(key.clone(), response_to_cached(response));
    }
}
