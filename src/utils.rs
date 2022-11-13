pub mod opt_uri_serde {
    use http::uri::Uri;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<Uri>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "http_serde::uri")] &'a Uri);

        value.as_ref().map(Helper).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Uri>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "http_serde::uri")] Uri);

        let helper = Option::deserialize(deserializer)?;
        Ok(helper.map(|Helper(external)| external))
    }
}
