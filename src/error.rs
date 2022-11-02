#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("http error")]
    HttpError(#[from] hyper::http::Error),
    #[error("hyper error")]
    HyperError(#[from] hyper::Error),
}
