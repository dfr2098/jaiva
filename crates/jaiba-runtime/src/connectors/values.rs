//! Conversión de valores de base de datos a JSON compartida por conectores.

use serde_json::{Number, Value};

/// Número decimal en texto → JSON sin perder precisión: entero si cabe en 64
/// bits, número si `f64` lo representa exacto y, si no, el texto completo.
/// Los ceros finales de la parte decimal (`12.50` de un `DECIMAL(10,2)`) no
/// cambian el valor y se ignoran para que una columna no mezcle tipos.
pub(crate) fn number_value(text: &str) -> Value {
    let mut normalized = if let Some(rest) = text.strip_prefix("-.") {
        format!("-0.{rest}")
    } else if let Some(rest) = text.strip_prefix('.') {
        format!("0.{rest}")
    } else {
        text.to_owned()
    };
    if normalized.contains('.') && !normalized.contains(['e', 'E']) {
        let trimmed = normalized.trim_end_matches('0').trim_end_matches('.');
        normalized = match trimmed {
            "-0" | "" => "0".to_owned(),
            other => other.to_owned(),
        };
    }
    if let Ok(integer) = normalized.parse::<i64>() {
        return Value::from(integer);
    }
    if let Ok(integer) = normalized.parse::<u64>() {
        return Value::from(integer);
    }
    if let Ok(float) = normalized.parse::<f64>()
        && float.is_finite()
        && float.to_string() == normalized
        && let Some(number) = Number::from_f64(float)
    {
        return Value::Number(number);
    }
    Value::String(normalized)
}

/// JSON no tiene NaN ni infinito; se conservan visibles como texto.
pub(crate) fn float_value(value: f64) -> Value {
    Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or_else(|| Value::String(value.to_string()))
}

pub(crate) fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let triple = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for position in 0..4 {
            if position <= chunk.len() {
                let index = (triple >> (18 - position * 6)) & 0x3f;
                output.push(char::from(ALPHABET[index as usize]));
            } else {
                output.push('=');
            }
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn numbers_keep_every_digit() {
        assert_eq!(number_value("42"), json!(42));
        assert_eq!(number_value("-7"), json!(-7));
        assert_eq!(
            number_value("18446744073709551615"),
            json!(18_446_744_073_709_551_615_u64)
        );
        assert_eq!(number_value("0.1"), json!(0.1));
        assert_eq!(number_value(".5"), json!(0.5));
        assert_eq!(number_value("-.25"), json!(-0.25));
        assert_eq!(number_value("100000000000000000000"), json!(1e20));
        assert_eq!(
            number_value("12345678901234567890.123456789"),
            json!("12345678901234567890.123456789")
        );
        assert_eq!(
            number_value("0.12345678901234567890"),
            json!("0.1234567890123456789")
        );
    }

    #[test]
    fn trailing_decimal_zeros_do_not_change_the_type() {
        assert_eq!(number_value("12.50"), json!(12.5));
        assert_eq!(number_value("12.00"), json!(12));
        assert_eq!(number_value("-0.00"), json!(0));
        assert_eq!(number_value("0.000"), json!(0));
        assert_eq!(number_value("1200"), json!(1200));
    }

    #[test]
    fn non_finite_floats_stay_visible() {
        assert_eq!(float_value(1.5), json!(1.5));
        assert_eq!(float_value(f64::NAN), json!("NaN"));
        assert_eq!(float_value(f64::INFINITY), json!("inf"));
    }

    #[test]
    fn encodes_base64_with_padding() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(&[0xff, 0x00, 0x7f, 0x80]), "/wB/gA==");
    }
}
