use log::{debug, error};
use std::collections::BTreeMap;

use crate::Fork;
use crate::PrestinoError;

fn validate(value: &str) -> Result<String, PrestinoError> {
    let val = value.to_lowercase();
    if val.is_ascii() {
        Ok(val)
    } else {
        Err(PrestinoError::HeaderParseError)
    }
}

mod names {
    pub const CATALOG: &'static str = "catalog";
    pub const CLIENT_INFO: &'static str = "client-info";
    pub const LANGUAGE: &'static str = "language";
    pub const ROLE: &'static str = "role";
    pub const SCHEMA: &'static str = "schema";
    pub const SESSION: &'static str = "session";
    pub const SOURCE: &'static str = "source";
    pub const TIMEZONE: &'static str = "time-zone";
    pub const TRACE_TOKEN: &'static str = "trace-token";
    pub const TRANSACTION_ID: &'static str = "transaction-id";
    pub const USER: &'static str = "user";
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum HeaderKey {
    /// Sets the default catalog to use if none is supplied. The `catalog` field must only contain
    /// visible ASCII characters (32-127)
    Catalog,
    /// Contains arbitrary information about the client program submitting the query.
    /// The `client_info` field must only contain visible ASCII characters (32-127)
    ClientInfo,
    /// Sets the language to be used when running the query and formatting results.
    /// The `language` field must only contain visible ASCII characters (32-127)
    Language,
    /// Sets the Role for Query processing. A “role” represents a collection of permissions.
    /// The `role` field must only contain visible ASCII characters (32-127)
    Role,
    /// Sets the default schema to use if none is supplied.
    /// The `schema` field must only contain visible ASCII characters (32-127)
    Schema,
    /// Session names and values will be joined into a comma-separated string of `{name}={value}`
    /// pairs, so if either `name` or `value` contains a `=` or `,` (or whitespace characters)
    /// the server may return an error.  The client does no additional verification and relies on
    /// the server as the source of truth.
    Session,
    /// For reporting purposes, this supplies the name of the software that submitted the query.
    Source,
    /// Sets the timezone to be used when running the query, which by default is the timezone of the Presto engine.
    /// Example: America/Los_Angeles
    /// The `timezone` field must only contain visible ASCII characters (32-127)
    Timezone,
    /// Supplies a trace token to the Trino engine to help identify log lines that originate with
    /// this query request. The `trace_token` field must only contain visible ASCII characters
    /// (32-127)
    TraceToken,
    /// Sets the transaction ID to use for query processing.
    /// The `transaction-id` field must only contain visible ASCII characters (32-127)
    TransactionId,
    /// Specifies the session user. If not supplied, the session user is
    /// automatically determined via [User mapping](https://trino.io/docs/current/security/user-mapping.html).
    /// The `user` field must only contain visible ASCII characters (32-127)
    User,
    // TODO: prepared-statement
    // TODO: client-tags
    // TODO: resource-estimate
    // TODO: extra-credential
}

impl HeaderKey {
    pub fn name(&self) -> &'static str {
        use names::*;
        use HeaderKey::*;

        match self {
            Catalog => CATALOG,
            ClientInfo => CLIENT_INFO,
            Language => LANGUAGE,
            Role => ROLE,
            Schema => SCHEMA,
            Session => SESSION,
            Source => SOURCE,
            Timezone => TIMEZONE,
            TraceToken => TRACE_TOKEN,
            TransactionId => TRANSACTION_ID,
            User => USER,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Headers {
    fork: Fork,
    headers: BTreeMap<HeaderKey, String>,
    session_properties: BTreeMap<String, String>,
}

impl Headers {
    fn new(fork: Fork) -> Self {
        Self {
            fork,
            headers: BTreeMap::new(),
            session_properties: BTreeMap::new(),
        }
    }
    /// Create a Headers instance for Presto
    pub fn presto() -> Self {
        Self::new(Fork::Presto)
    }

    /// Create a Headers instance for Trino
    pub fn trino() -> Self {
        Self::new(Fork::Trino)
    }

    pub fn new_with_fork(&self) -> Self {
        Self::new(self.fork)
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

    /// Sets a header, overriding the previous value (if any existed)
    /// Do not use for Session Properties, use `add_session_property` to add an
    /// individual property.  Trying to set "Session" here will return a PrestinoError::HeaderParseError
    /// Setting an unnown
    pub fn set_header(&mut self, key: HeaderKey, value: &str) -> Result<(), PrestinoError> {
        if key == HeaderKey::Session {
            // TODO: Clean up these error messages.
            error!(
                "Cannot set session properties via set_header; use add_session_property instead"
            );
            return Err(PrestinoError::HeaderParseError);
        }
        self.headers.insert(key, validate(value)?);
        Ok(())
    }

    /// Convenience method to call set_header with HeaderKey::User.
    pub fn set_user(&mut self, user: &str) -> Result<(), PrestinoError> {
        self.set_header(HeaderKey::User, user)
    }

    pub fn add_session_property(&mut self, key: &str, value: &str) -> Result<(), PrestinoError> {
        self.session_properties
            .insert(validate(key)?, validate(value)?);
        Ok(())
    }

    pub fn get_headers(&self) -> Vec<(String, String)> {
        let mut values: Vec<_> = self
            .headers
            .iter()
            .map(|(key, value)| (self.fork.name_for(key.name()), value.to_owned()))
            .collect();

        let session_value_opt: Option<String> = self
            .session_properties
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .reduce(|base, next| base + "," + &next)
            .map(|s| s.to_ascii_lowercase());
        if let Some(session_value) = session_value_opt {
            values.push((self.fork.name_for(HeaderKey::Session.name()), session_value));
        }

        values
    }

    /// Parse a response header to update this header.
    /// If the header of the form "X-Presto-FOO: BAR", then
    /// name would be "X-Presto-FOO" and value would be "BAR"
    /// name and value will be downcased.
    pub fn update_from_response_header(&mut self, name: &str, value: &str) -> Result<(), PrestinoError> {
        use HeaderKey::*;

        let name_str = validate(name)?;
        let key = name_str
            .strip_prefix(self.fork.prefix())
            .and_then(|s| s.strip_prefix('-'));
        let value = validate(value)?;
        match key {
            Some("set-catalog") => {
                debug!("Setting catalog: {value}");
                self.headers.insert(Catalog, value);
            }
            Some("set-schema") => {
                debug!("Setting schema: {value}");
                self.headers.insert(Schema, value);
            }
            Some("set-session") => match value.split_once('=') {
                None => return Err(PrestinoError::HeaderParseError),
                Some((k, v)) => {
                    debug!("Setting session: {k}={v}");
                    self.add_session_property(k, v);
                }
            },
            Some("clear-session") => {
                debug!("Clearing session {value}");
                self.session_properties.remove(&value);
            }
            Some("set-role") => {
                debug!("Setting role {value}");
                self.headers.insert(Role, value);
            }
            Some("started-transaction-id") => {
                debug!("Setting Transaction Id {value}");
                self.headers.insert(TransactionId, value);
            }
            Some("clear-transaction-id") => {
                debug!("Clearing Transaction Id");
                self.headers.remove(&TransactionId);
            }
            // TODO: added-prepare
            // TODO: deallocated-prepare
            Some(_) => debug!("Unprocessed response header: {name:?}"),
            None => (),
        }
        Ok(())
    }
}
