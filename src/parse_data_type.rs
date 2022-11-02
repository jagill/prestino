use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::DataType;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Tokenizer;

const DIALECT: AnsiDialect= AnsiDialect {};

pub fn parse_data_type(data_type: &str) -> Result<DataType, ParserError>  {
    // let dialect = AnsiDialect {};
    let tokens = Tokenizer::new(&DIALECT, data_type)
        .tokenize()
        .map_err(|tok_err| ParserError::TokenizerError(format!("{}", tok_err)))?;
    let mut parser = Parser::new(tokens, &DIALECT);
    return Ok(parser.parse_data_type()?);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic() {
        let parsed = parse_data_type(&"BOOLEAN").unwrap();
        assert_eq!(parsed, DataType::Boolean);
    }
}

