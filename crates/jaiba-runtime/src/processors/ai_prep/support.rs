//! Utilidades compartidas para procesadores AI Prep sobre `Records`.
//!
//! Centraliza validación de objetos JSON, coerción numérica y el evaluador
//! aritmético mínimo usado por `ai_compute_fields`.

use serde_json::{Map, Number, Value};

use crate::error::FlowError;

/// Exige que cada elemento del paquete sea un objeto JSON.
pub(crate) fn require_objects(
    records: &[Value],
    processor_id: &str,
) -> Result<(), FlowError> {
    for (index, record) in records.iter().enumerate() {
        if !record.is_object() {
            return Err(FlowError::Processor {
                processor_id: processor_id.to_owned(),
                message: format!("record {index} is not a JSON object"),
            });
        }
    }
    Ok(())
}

/// Vista mutable del objeto; falla si el registro no es un mapa.
pub(crate) fn as_object_mut<'a>(
    record: &'a mut Value,
    processor_id: &str,
) -> Result<&'a mut Map<String, Value>, FlowError> {
    record.as_object_mut().ok_or_else(|| FlowError::Processor {
        processor_id: processor_id.to_owned(),
        message: "expected JSON object records".to_owned(),
    })
}

/// Ausente, `null` o string vacío cuentan como missing (fill / drop_nulls).
pub(crate) fn is_missing(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => true,
        Some(Value::String(text)) if text.trim().is_empty() => true,
        _ => false,
    }
}

/// Coerción laxa a `f64` (número, string parseable o bool → 0/1).
pub(crate) fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse().ok(),
        Value::Bool(flag) => Some(if *flag { 1.0 } else { 0.0 }),
        _ => None,
    }
}

/// Serializa un float a JSON; valores no finitos se convierten en `null`.
pub(crate) fn json_number(value: f64) -> Value {
    Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

/// Clave compuesta para dedupe: valores de `fields` unidos con separador US.
pub(crate) fn field_key(record: &Map<String, Value>, fields: &[String]) -> String {
    fields
        .iter()
        .map(|field| {
            record
                .get(field)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_owned())
        })
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

/// Evaluador aritmético mínimo: literales, campos, `+ - * /` y paréntesis.
///
/// Sin funciones, sin acceso a memoria arbitraria: solo nombres de campo del
/// registro actual. Precedencia estándar (`*`/`/` sobre `+`/`-`).
pub(crate) fn eval_expr(
    expr: &str,
    record: &Map<String, Value>,
) -> Result<f64, String> {
    let tokens = tokenize(expr)?;
    let mut parser = Parser {
        tokens: &tokens,
        index: 0,
        record,
    };
    let value = parser.parse_expr()?;
    if parser.index != tokens.len() {
        return Err("expresión incompleta o tokens sobrantes".to_owned());
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Number(f64),
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut chars = input.chars().peekable();
    let mut tokens = Vec::new();
    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            '+' => {
                chars.next();
                tokens.push(Token::Plus);
            }
            '-' => {
                chars.next();
                tokens.push(Token::Minus);
            }
            '*' => {
                chars.next();
                tokens.push(Token::Star);
            }
            '/' => {
                chars.next();
                tokens.push(Token::Slash);
            }
            '(' => {
                chars.next();
                tokens.push(Token::LParen);
            }
            ')' => {
                chars.next();
                tokens.push(Token::RParen);
            }
            '0'..='9' | '.' => {
                let mut raw = String::new();
                while let Some(&next) = chars.peek() {
                    if next.is_ascii_digit() || next == '.' {
                        raw.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let number: f64 = raw
                    .parse()
                    .map_err(|_| format!("número inválido '{raw}'"))?;
                tokens.push(Token::Number(number));
            }
            'a'..='z' | 'A'..='Z' | '_' => {
                let mut ident = String::new();
                while let Some(&next) = chars.peek() {
                    if next.is_ascii_alphanumeric() || next == '_' || next == '.' {
                        ident.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(Token::Ident(ident));
            }
            other => return Err(format!("carácter inesperado '{other}'")),
        }
    }
    Ok(tokens)
}

/// Parser recursivo descendente sobre la lista de tokens.
struct Parser<'a> {
    tokens: &'a [Token],
    index: usize,
    record: &'a Map<String, Value>,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }

    fn bump(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.index);
        if token.is_some() {
            self.index += 1;
        }
        token
    }

    fn parse_expr(&mut self) -> Result<f64, String> {
        let mut value = self.parse_term()?;
        loop {
            match self.peek() {
                Some(Token::Plus) => {
                    self.bump();
                    value += self.parse_term()?;
                }
                Some(Token::Minus) => {
                    self.bump();
                    value -= self.parse_term()?;
                }
                _ => break,
            }
        }
        Ok(value)
    }

    fn parse_term(&mut self) -> Result<f64, String> {
        let mut value = self.parse_unary()?;
        loop {
            match self.peek() {
                Some(Token::Star) => {
                    self.bump();
                    value *= self.parse_unary()?;
                }
                Some(Token::Slash) => {
                    self.bump();
                    let rhs = self.parse_unary()?;
                    if rhs == 0.0 {
                        return Err("división por cero".to_owned());
                    }
                    value /= rhs;
                }
                _ => break,
            }
        }
        Ok(value)
    }

    fn parse_unary(&mut self) -> Result<f64, String> {
        match self.peek() {
            Some(Token::Minus) => {
                self.bump();
                Ok(-self.parse_unary()?)
            }
            Some(Token::Plus) => {
                self.bump();
                self.parse_unary()
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<f64, String> {
        match self.bump().cloned() {
            Some(Token::Number(number)) => Ok(number),
            Some(Token::Ident(name)) => {
                let value = self
                    .record
                    .get(&name)
                    .ok_or_else(|| format!("campo desconocido '{name}'"))?;
                as_f64(value).ok_or_else(|| format!("campo '{name}' no es numérico"))
            }
            Some(Token::LParen) => {
                let value = self.parse_expr()?;
                match self.bump() {
                    Some(Token::RParen) => Ok(value),
                    _ => Err("se esperaba ')'".to_owned()),
                }
            }
            other => Err(format!("token inesperado {other:?}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn evaluates_simple_feature_expression() {
        let record = json!({"temperature": 35.0, "vibration": 0.8})
            .as_object()
            .cloned()
            .unwrap();
        let value = eval_expr("temperature + vibration * 2", &record).unwrap();
        assert!((value - 36.6).abs() < 1e-9);
    }
}
