use super::Parser;
use crate::parser::error::{Result, ParseError};
use crate::parser::lexer::Token;
use crate::parser::ast::{Statement, CreateTableStmt, DropTableStmt, CreateViewStmt, DropViewStmt, CreateMaterializedViewStmt, RefreshMaterializedViewStmt, DropMaterializedViewStmt, CreateTriggerStmt, DropTriggerStmt, TriggerTiming, TriggerEvent, TriggerFor, DescribeStmt, ColumnDef, DataType};
use super::select;

pub fn parse_create(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Create)?;
    
    match parser.current_token() {
        Token::Table => parse_create_table(parser),
        Token::View => parse_create_view(parser),
        Token::Materialized => parse_create_materialized_view(parser),
        Token::Trigger => parse_create_trigger(parser),
        _ => Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    }
}

fn parse_create_table(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Table)?;
    
    let table = parser.expect_identifier()?;
    
    parser.expect(Token::LeftParen)?;
    
    let columns = parse_column_defs(parser)?;
    
    parser.expect(Token::RightParen)?;
    
    Ok(Statement::CreateTable(CreateTableStmt {
        table,
        columns,
    }))
}

fn parse_create_view(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::View)?;
    
    let name = parser.expect_identifier()?;
    
    parser.expect(Token::As)?;
    
    let query = select::parse_select_stmt(parser)?;
    
    Ok(Statement::CreateView(CreateViewStmt {
        name,
        query: Box::new(query),
    }))
}

fn parse_create_materialized_view(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Materialized)?;
    parser.expect(Token::View)?;
    
    let name = parser.expect_identifier()?;
    
    parser.expect(Token::As)?;
    
    let query = select::parse_select_stmt(parser)?;
    
    Ok(Statement::CreateMaterializedView(CreateMaterializedViewStmt {
        name,
        query: Box::new(query),
    }))
}

fn parse_create_trigger(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Trigger)?;
    
    let name = parser.expect_identifier()?;
    
    let timing = match parser.current_token() {
        Token::Before => { parser.advance(); TriggerTiming::Before }
        Token::After => { parser.advance(); TriggerTiming::After }
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    
    let event = match parser.current_token() {
        Token::Insert => { parser.advance(); TriggerEvent::Insert }
        Token::Update => { parser.advance(); TriggerEvent::Update }
        Token::Delete => { parser.advance(); TriggerEvent::Delete }
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    
    parser.expect(Token::On)?;
    let table = parser.expect_identifier()?;
    
    parser.expect(Token::For)?;
    parser.expect(Token::Each)?;
    
    let for_each = match parser.current_token() {
        Token::Row => { parser.advance(); TriggerFor::EachRow }
        Token::Statement => { parser.advance(); TriggerFor::EachStatement }
        _ => return Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    };
    
    let when = if parser.current_token() == &Token::When {
        parser.advance();
        parser.expect(Token::LeftParen)?;
        let expr = super::expr::parse_expr(parser)?;
        parser.expect(Token::RightParen)?;
        Some(expr)
    } else {
        None
    };
    
    parser.expect(Token::Begin)?;
    let mut body = Vec::new();
    
    while parser.current_token() != &Token::End {
        body.push(parser.parse()?);
        if parser.current_token() == &Token::Semicolon {
            parser.advance();
        }
    }
    
    parser.expect(Token::End)?;
    
    Ok(Statement::CreateTrigger(CreateTriggerStmt {
        name,
        timing,
        event,
        table,
        for_each,
        when,
        body,
    }))
}

pub fn parse_drop(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Drop)?;
    
    match parser.current_token() {
        Token::Table => parse_drop_table(parser),
        Token::View => parse_drop_view(parser),
        Token::Materialized => parse_drop_materialized_view(parser),
        Token::Trigger => parse_drop_trigger(parser),
        _ => Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    }
}

fn parse_drop_table(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Table)?;
    
    let if_exists = if parser.current_token() == &Token::If {
        parser.advance();
        parser.expect(Token::Exists)?;
        true
    } else {
        false
    };
    
    let table = parser.expect_identifier()?;
    
    Ok(Statement::DropTable(DropTableStmt { table, if_exists }))
}

fn parse_drop_view(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::View)?;
    
    let if_exists = if parser.current_token() == &Token::If {
        parser.advance();
        parser.expect(Token::Exists)?;
        true
    } else {
        false
    };
    
    let name = parser.expect_identifier()?;
    
    Ok(Statement::DropView(DropViewStmt { name, if_exists }))
}

fn parse_drop_materialized_view(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Materialized)?;
    parser.expect(Token::View)?;
    
    let if_exists = if parser.current_token() == &Token::If {
        parser.advance();
        parser.expect(Token::Exists)?;
        true
    } else {
        false
    };
    
    let name = parser.expect_identifier()?;
    
    Ok(Statement::DropMaterializedView(DropMaterializedViewStmt { name, if_exists }))
}

fn parse_drop_trigger(parser: &mut Parser) -> Result<Statement> {
    parser.expect(Token::Trigger)?;
    
    let if_exists = if parser.current_token() == &Token::If {
        parser.advance();
        parser.expect(Token::Exists)?;
        true
    } else {
        false
    };
    
    let name = parser.expect_identifier()?;
    
    Ok(Statement::DropTrigger(DropTriggerStmt { name, if_exists }))
}

pub fn parse_describe(parser: &mut Parser) -> Result<Statement> {
    if parser.current_token() == &Token::Describe || parser.current_token() == &Token::Desc {
        parser.advance();
    }
    
    let table = parser.expect_identifier()?;
    
    Ok(Statement::Describe(DescribeStmt { table }))
}

fn parse_column_defs(parser: &mut Parser) -> Result<Vec<ColumnDef>> {
    let mut columns = vec![parse_column_def(parser)?];
    
    while parser.current_token() == &Token::Comma {
        parser.advance();
        columns.push(parse_column_def(parser)?);
    }
    
    Ok(columns)
}

fn parse_column_def(parser: &mut Parser) -> Result<ColumnDef> {
    let name = parser.expect_identifier()?;
    let data_type = parse_data_type(parser)?;
    
    Ok(ColumnDef { name, data_type })
}

fn parse_data_type(parser: &mut Parser) -> Result<DataType> {
    match parser.current_token() {
        Token::Int => {
            parser.advance();
            Ok(DataType::Int)
        }
        Token::Text => {
            parser.advance();
            Ok(DataType::Text)
        }
        Token::Varchar => {
            parser.advance();
            if parser.current_token() == &Token::LeftParen {
                parser.advance();
                if let Token::Number(n) = parser.current_token() {
                    let size = *n as u32;
                    parser.advance();
                    parser.expect(Token::RightParen)?;
                    Ok(DataType::Varchar(size))
                } else {
                    Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token())))
                }
            } else {
                Ok(DataType::Varchar(255))
            }
        }
        _ => Err(ParseError::UnexpectedToken(format!("{:?}", parser.current_token()))),
    }
}
