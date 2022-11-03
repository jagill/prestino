pub enum Header {
    User(String),
    Source(String),
    Catalog(String),
    Schema(String),
    // TimeZone,  // Not yet supported
    // Language,  // Not yet supported
    TraceToken(String),
    Session(String, String),
    // Role,  // Not yet supported
    // PreparedStatement,  // Not yet sypported
    TransactionId(String),
    ClientInfo(String),
    ClientTag(String),
    // ResourceEstimate, // Not yet supported
    // ExtraCredential, // Not yet supported
}