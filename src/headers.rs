use crate::Fork;
use crate::PrestinoError;
use log::debug;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Headers {
    fork: Fork,
    headers: HeaderMap,
    session_properties: BTreeMap<String, String>,
}

impl Headers {
    /// Create a Headers instance for Presto
    pub fn presto() -> Self {
        Self {
            fork: Fork::Presto,
            headers: HeaderMap::new(),
            session_properties: BTreeMap::new(),
        }
    }

    /// Create a Headers instance for Trino
    pub fn trino() -> Self {
        Self {
            fork: Fork::Trino,
            headers: HeaderMap::new(),
            session_properties: BTreeMap::new(),
        }
    }

    /// Create a Headers instance with the same fork as this one
    pub fn new_with_fork(&self) -> Self {
        Self {
            fork: self.fork,
            headers: HeaderMap::new(),
            session_properties: BTreeMap::new(),
        }
    }

    /// Update the values in this Head with values from the other Headers.
    ///
    /// This will panic if self and other have different forks.
    pub fn update(&mut self, other: &Headers) {
        // TODO: Make this a generic type so this is caught at compile time.
        if self.fork != other.fork {
            panic!(
                "Can't merge headers with different forms.  self {:?}, other {:?}",
                self.fork, other.fork
            );
        }
        self.headers.extend(other.headers.clone().into_iter());
        self.session_properties
            .extend(other.session_properties.clone().into_iter());
    }

    fn name_for(&self, name: &str) -> HeaderName {
        // Since we control the input, we can ensure that it is always visible ASCII
        HeaderName::try_from(self.fork.name_for(name)).unwrap()
    }

    /// Extract `foo` from `x-presto-foo`, or return None if the name doesn't start with that prefix.
    fn key_from<'a>(&self, name: &'a HeaderName) -> Option<&'a str> {
        let name_str = name.as_str();
        let stripped = name_str.strip_prefix(self.fork.prefix())?;
        stripped.strip_prefix("-")
    }

    /// Convert a value to a lowercase HeaderValue.
    /// Panic if it contains a char that is not a visible ascii char.
    fn to_value(value: &str) -> HeaderValue {
        value.to_ascii_lowercase().parse().unwrap()
    }

    /// Specifies the session user. If not supplied, the session user is
    /// automatically determined via [User mapping](https://trino.io/docs/current/security/user-mapping.html).
    /// The `user` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn set_user(&mut self, user: &str) -> &mut Self {
        self.headers
            .insert(self.name_for("user"), Self::to_value(user));
        self
    }

    /// Specifies the session user. If not supplied, the session user is
    /// automatically determined via [User mapping](https://trino.io/docs/current/security/user-mapping.html).
    /// The `user` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn user(mut self, user: &str) -> Self {
        self.set_user(user);
        self
    }

    pub fn set_source(&mut self, source: &str) -> &mut Self {
        self.headers
            .insert(self.name_for("source"), Self::to_value(source));
        self
    }

    /// For reporting purposes, this supplies the name of the software that
    /// submitted the query.
    /// The `source` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn source(mut self, source: &str) -> Self {
        self.set_source(source);
        self
    }

    /// Sets the default catalog to use if none is supplied.
    /// The `catalog` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn set_catalog(&mut self, catalog: &str) -> &mut Self {
        self.headers
            .insert(self.name_for("catalog"), Self::to_value(catalog));
        self
    }

    /// Supplies the default catalog to use if none is supplied.
    /// The `catalog` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn catalog(mut self, catalog: &str) -> Self {
        self.set_catalog(catalog);
        self
    }

    /// Sets the default schema to use if none is supplied.
    /// The `schema` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn set_schema(&mut self, schema: &str) -> &mut Self {
        self.headers
            .insert(self.name_for("schema"), Self::to_value(schema));
        self
    }

    /// Supplies the default schema to use if none is supplied.
    /// The `schema` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn schema(mut self, schema: &str) -> Self {
        self.set_schema(schema);
        self
    }

    /// Sets the timezone to be used when running the query, which by default is the timezone of the Presto engine.
    /// Example: America/Los_Angeles
    /// The `timezone` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn set_timezone(&mut self, timezone: &str) -> &mut Self {
        self.headers
            .insert(self.name_for("time-zone"), Self::to_value(timezone));
        self
    }

    /// Supplies the timezone to be used when running the query, which by default is the timezone of the Presto engine.
    /// Example: America/Los_Angeles
    /// The `timezone` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn timezone(mut self, timezone: &str) -> Self {
        self.set_timezone(timezone);
        self
    }

    /// Sets the language to be used when running the query and formatting results.
    /// The `language` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn set_language(&mut self, language: &str) -> &mut Self {
        self.headers
            .insert(self.name_for("language"), Self::to_value(language));
        self
    }

    /// Supplies the language to be used when running the query and formatting results.
    /// The `language` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn language(mut self, language: &str) -> Self {
        self.set_language(language);
        self
    }

    pub fn set_trace_token(&mut self, trace_token: &str) -> &mut Self {
        self.headers
            .insert(self.name_for("trace-token"), Self::to_value(trace_token));
        self
    }

    /// Supplies a trace token to the Trino engine to help identify log lines
    /// that originate with this query request.
    /// The `trace_token` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn trace_token(mut self, trace_token: &str) -> Self {
        self.set_trace_token(trace_token);
        self
    }

    /// Adds a single session property.  Multiple invocations will add properties;
    /// if there is a previous property with the same key, it will overwrite it.
    /// The `name` and `value` parameters must only include visible ASCII characters,
    /// otherwise this builder will panic when `build()` is called.
    ///
    /// The names and values will be joined into a comma-separated string of `{name}={value}`
    /// pairs, so if either `name` or `value` contains a `=` or `,` (or whitespace characters)
    /// the server may return an error.  The client does no additional verification and relies on
    /// the server as the source of truth.
    pub fn set_session(&mut self, name: &str, value: &str) -> &mut Self {
        self.session_properties
            .insert(name.to_owned(), value.to_owned());
        self
    }

    pub fn clear_session(&mut self, name: &str) -> &mut Self {
        self.session_properties.remove(name);
        self
    }

    /// Adds a single session property.  Multiple invocations will add properties;
    /// if there is a previous property with the same key, it will overwrite it.
    /// The `name` and `value` parameters must only include visible ASCII characters,
    /// otherwise this builder will panic when `build()` is called.
    ///
    /// The names and values will be joined into a comma-separated string of `{name}={value}`
    /// pairs, so if either `name` or `value` contains a `=` or `,` (or whitespace characters)
    /// the server may return an error.  The client does no additional verification and relies on
    /// the server as the source of truth.
    pub fn session(mut self, name: &str, value: &str) -> Self {
        self.set_session(name, value);
        self
    }

    /// Sets the Role for Query processing. A “role” represents a collection of permissions.
    /// The `role` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn set_role(&mut self, role: &str) -> &mut Self {
        self.headers
            .insert(self.name_for("role"), Self::to_value(role));
        self
    }

    /// Supplies the Role for Query processing. A “role” represents a collection of permissions.
    /// The `role` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn role(mut self, role: &str) -> Self {
        self.set_role(role);
        self
    }

    // TODO: prepared-statement

    /// Sets the transaction ID to use for query processing.
    /// The `transaction-id` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn set_transaction_id(&mut self, transaction_id: &str) -> &mut Self {
        self.headers.insert(
            self.name_for("transaction-id"),
            Self::to_value(transaction_id),
        );
        self
    }

    /// Supplies the transaction ID to use for query processing.
    /// The `transaction-id` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn transaction_id(mut self, transaction_id: &str) -> Self {
        self.set_transaction_id(transaction_id);
        self
    }

    /// Clears the Transaction ID used for query processing.
    pub fn clear_transaction_id(&mut self) -> &mut Self {
        self.headers.remove(self.name_for("transaction-id"));
        self
    }

    pub fn set_client_info(&mut self, client_info: &str) -> &mut Self {
        self.headers
            .insert(self.name_for("client-info"), Self::to_value(client_info));
        self
    }

    /// Contains arbitrary information about the client program submitting the query.
    /// The `client_info` field must only contain visible ASCII characters (32-127);
    /// otherwise this function will panic.
    pub fn client_info(mut self, client_info: &str) -> Self {
        self.set_client_info(client_info);
        self
    }

    // TODO: client-tags
    // TODO: resource-estimate
    // TODO: extra-credential

    pub fn build(&self) -> Result<HeaderMap, PrestinoError> {
        let mut headers = self.headers.clone();
        let session_value_opt: Option<String> = self
            .session_properties
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .reduce(|base, next| base + "," + &next)
            .map(|s| s.to_ascii_lowercase());
        if let Some(session_value) = session_value_opt {
            headers.insert(self.name_for("session"), session_value.parse()?);
        }

        Ok(headers)
    }

    pub fn update_from_response_headers(
        &mut self,
        response_headers: &HeaderMap,
    ) -> Result<(), PrestinoError> {
        for (name, value) in response_headers.iter() {
            self.update_from_response_header(name, value)?;
        }
        Ok(())
    }

    fn update_from_response_header(
        &mut self,
        name: &HeaderName,
        value: &HeaderValue,
    ) -> Result<(), PrestinoError> {
        match self.key_from(name) {
            Some("set-catalog") => {
                let catalog = value.to_str()?;
                debug!("Setting catalog: {catalog}");
                self.set_catalog(catalog);
            }
            Some("set-schema") => {
                let schema = value.to_str()?;
                debug!("Setting schema: {schema}");
                self.set_schema(schema);
            }
            Some("set-session") => match value.to_str()?.split_once('=') {
                None => return Err(PrestinoError::HeaderParseError),
                Some((k, v)) => {
                    debug!("Setting session: {k}={v}");
                    self.set_session(k, v);
                }
            },
            Some("clear-session") => {
                let name = value.to_str()?;
                debug!("Clearing session {name}");
                self.clear_session(name);
            }
            Some("set-role") => {
                let role: &str = value.to_str()?;
                debug!("Setting role {role}");
                self.set_role(role);
            }
            Some("started-transaction-id") => {
                let transaction_id: &str = value.to_str()?;
                debug!("Setting Transaction Id {transaction_id}");
                self.set_transaction_id(transaction_id);
            }
            Some("clear-transaction-id") => {
                debug!("Clearing Transaction Id");
                self.clear_transaction_id();
            }
            // TODO: added-prepare
            // TODO: deallocated-prepare
            Some(_) => debug!("Unprocessed response header: {name:?}"),
            None => (),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_value(header_map: &HeaderMap, name: &str) -> Option<String> {
        let header_value = header_map.get(name);
        header_value.map(|val| val.to_str().unwrap().to_owned())
    }

    #[test]
    fn test_basic_headers() {
        let mut headers = Headers::trino()
            .user("me")
            .source("here")
            .catalog("memory")
            .language("en/us");

        headers.set_schema("database");
        headers.set_timezone("america/chicago");
        headers.set_role("moderator");
        headers.set_transaction_id("abc123");

        let header_map = headers.build().unwrap();
        assert_eq!(
            get_value(&header_map, "x-trino-user"),
            Some("me".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-source"),
            Some("here".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-catalog"),
            Some("memory".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-schema"),
            Some("database".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-language"),
            Some("en/us".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-time-zone"),
            Some("america/chicago".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-role"),
            Some("moderator".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-transaction-id"),
            Some("abc123".to_string())
        );

        headers.set_user("you");
        let header_map = headers.build().unwrap();
        assert_eq!(
            get_value(&header_map, "x-trino-user"),
            Some("you".to_string())
        );
    }

    #[test]
    fn test_session() {
        let headers = Headers::trino()
            .session("b", "2")
            .session("a", "1")
            .session("c", "3");

        let header_map = headers.build().unwrap();
        assert_eq!(
            get_value(&header_map, "x-trino-session"),
            Some("a=1,b=2,c=3".to_string())
        );
    }

    #[test]
    fn test_update_headers() {
        let mut request_headers = Headers::trino()
            .session("a", "1")
            .session("b", "2")
            .session("c", "3");
        let mut response_header_map = HeaderMap::new();

        response_header_map.insert("X-Trino-Set-Catalog", "cat2".parse().unwrap());
        response_header_map.insert("X-Trino-Set-Schema", "schema2".parse().unwrap());
        response_header_map.insert("X-Trino-Set-Session", "b=4".parse().unwrap());
        response_header_map.insert("X-Trino-Clear-Session", "c".parse().unwrap());
        response_header_map.insert("X-Trino-Set-Role", "moderator".parse().unwrap());
        response_header_map.insert("X-Trino-Started-Transaction-Id", "abc123".parse().unwrap());

        request_headers
            .update_from_response_headers(&response_header_map)
            .unwrap();
        let header_map = request_headers.build().unwrap();
        assert_eq!(
            get_value(&header_map, "x-trino-catalog"),
            Some("cat2".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-schema"),
            Some("schema2".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-session"),
            Some("a=1,b=4".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-role"),
            Some("moderator".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-transaction-id"),
            Some("abc123".to_string())
        );

        response_header_map.clear();
        response_header_map.insert("X-Trino-Clear-Transaction-Id", "".parse().unwrap());

        let mut request_headers = Headers::trino().transaction_id("abc123");
        request_headers
            .update_from_response_headers(&response_header_map)
            .unwrap();
        let header_map = request_headers.build().unwrap();
        assert!(!header_map.contains_key("X-Trino-Transaction-Id"));
    }

    #[test]
    fn test_merge() {
        let mut base_headers = Headers::trino()
            .user("me")
            .catalog("memory")
            .session("a", "1")
            .session("b", "2")
            .session("c", "3");

        let new_headers = Headers::trino()
            .user("you")
            .schema("database")
            .session("b", "4");

        base_headers.update(&new_headers);
        let header_map = base_headers.build().unwrap();
        assert_eq!(
            get_value(&header_map, "x-trino-user"),
            Some("you".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-catalog"),
            Some("memory".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-schema"),
            Some("database".to_string())
        );
        assert_eq!(
            get_value(&header_map, "x-trino-session"),
            Some("a=1,b=4,c=3".to_string())
        );
    }
}
