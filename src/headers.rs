use http::request::Builder;
use std::collections::HashMap;

pub enum HeaderField {
    User(String),
    Source(String),
    Catalog(String),
    Schema(String),
    // TimeZone,  // Not yet supported
    // Language,  // Not yet supported
    TraceToken(String),
    Session(HashMap<String, String>),
    // Role,  // Not yet supported
    // PreparedStatement,  // Not yet sypported
    TransactionId(String),
    ClientInfo(String),
    ClientTag(String),
    // ResourceEstimate, // Not yet supported
    // ExtraCredential, // Not yet supported
}

impl HeaderField {
    fn get_key(&self) -> &'static str {
        match self {
            HeaderField::User(_) => "User",
            _ => todo!(),
        }
    }
}

pub struct HeaderBuilder {
    headers: Vec<HeaderField>,
}
impl HeaderBuilder {
    pub fn new() -> Self {
        HeaderBuilder {
            headers: Vec::<HeaderField>::new(),
        }
    }
    pub fn add_header(mut self, field: HeaderField) -> Self {
        self.headers.push(field);
        self
    }
    fn get_prefix() -> &'static str {
        "X-Trino-"
    }
    pub fn set_headers(&self, mut builder: Builder) -> Builder {
        for header in &self.headers {
            let key = format!("{}{}", Self::get_prefix(), header.get_key());
            let val = match header {
                HeaderField::User(val) => val,
                _ => todo!(),
            };
            builder = builder.header(key, val);
        }
        builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;
    use hyper::Request;

    #[test]
    fn test_set_headers_with_user() {
        let builder = Request::builder();
        let r = HeaderBuilder::new()
            .add_header(HeaderField::User("test".to_string()))
            .set_headers(builder)
            .body(())
            .unwrap();
        let header_value: &HeaderValue = r.headers().get("X-Trino-User").unwrap();
        let value = &HeaderValue::from_str("test").unwrap();
        assert_eq!(header_value, value);
    }
}
