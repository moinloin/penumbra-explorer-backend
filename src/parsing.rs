use anyhow::Result;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use cometindex::ContextualizedEvent;
use regex::Regex;
use serde_json::{json, Value};
use std::fmt::Write;

/// Helper function to convert bytes to a hexadecimal string
#[must_use]
pub fn encode_to_hex<T: AsRef<[u8]>>(data: T) -> String {
    let bytes = data.as_ref();
    let mut hex_string = String::with_capacity(bytes.len() * 2);

    for &byte in bytes {
        let _ = write!(&mut hex_string, "{byte:02X}");
    }

    hex_string
}

/// Helper function to convert bytes to a base64 string
#[must_use]
pub fn encode_to_base64<T: AsRef<[u8]>>(data: T) -> String {
    let bytes = data.as_ref();
    BASE64.encode(bytes)
}

/// Parse attribute string from an event with improved handling for EventAttribute format
#[must_use]
pub fn parse_attribute_string(attr_str: &str) -> Option<(String, String)> {
    // Check if this is a V037 EventAttribute format
    if attr_str.contains("EventAttribute") {
        // Extract key and value from EventAttribute using regex
        let re =
            Regex::new(r#"EventAttribute\s*\{\s*key:\s*"([^"]+)",\s*value:\s*"([^"]+)""#).ok()?;
        if let Some(caps) = re.captures(attr_str) {
            let key = caps.get(1)?.as_str().to_string();
            let value = caps.get(2)?.as_str().replace("\\\"", "\"");
            return Some((key, value));
        }
    }

    // Standard key:value format check
    if attr_str.contains("key:") && attr_str.contains("value:") {
        let key_start = attr_str.find("key:").unwrap_or(0) + 4;
        let key_end = attr_str[key_start..]
            .find(',')
            .map_or(attr_str.len(), |pos| key_start + pos);
        let key = attr_str[key_start..key_end]
            .trim()
            .trim_matches('"')
            .to_string();

        let value_start = attr_str.find("value:").unwrap_or(0) + 6;
        let value_end = attr_str[value_start..]
            .find(',')
            .map_or(attr_str.len(), |pos| value_start + pos);
        let value = attr_str[value_start..value_end]
            .trim()
            .trim_matches('"')
            .replace("\\\"", "\""); // Unescape the value

        return Some((key, value));
    }

    // JSON-style format check
    if attr_str.contains('{') && attr_str.contains('}') {
        let json_start = attr_str.find('{').unwrap_or(0);
        let field_name = attr_str[0..json_start].trim().to_string();

        if !field_name.is_empty() {
            let json_content = &attr_str[json_start..];
            return Some((field_name, json_content.to_string()));
        }
    }

    None
}

/// Converts a Penumbra event to JSON format
///
/// # Errors
/// Returns an error if JSON serialization fails, or if attribute conversion fails
pub fn event_to_json(
    event: ContextualizedEvent<'_>,
    tx_hash: Option<[u8; 32]>,
) -> Result<Value, anyhow::Error> {
    let mut attributes = Vec::new();

    for attr in &event.event.attributes {
        let attr_str = format!("{attr:?}");

        if let Some((key, value)) = parse_attribute_string(&attr_str) {
            let mut attr_json = serde_json::Map::new();
            attr_json.insert("key".to_string(), json!(key));
            attr_json.insert("value".to_string(), json!(value));

            attributes.push(serde_json::Value::Object(attr_json));
        } else {
            let mut attr_json = serde_json::Map::new();
            attr_json.insert("key".to_string(), json!(attr_str.clone()));
            attr_json.insert("value".to_string(), json!("Unknown"));

            attributes.push(serde_json::Value::Object(attr_json));
        }
    }

    let mut json_event_map = serde_json::Map::new();
    json_event_map.insert("type".to_string(), json!(event.event.kind));
    json_event_map.insert("attributes".to_string(), json!(attributes));

    let json_event = serde_json::Value::Object(json_event_map);

    Ok(json_event)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_to_hex() {
        assert_eq!(encode_to_hex([]), "");
        assert_eq!(encode_to_hex([0]), "00");
        assert_eq!(encode_to_hex([255]), "FF");
        assert_eq!(encode_to_hex([0, 1, 2, 3]), "00010203");
        assert_eq!(encode_to_hex([255, 254, 253, 252]), "FFFEFDFC");

        let vec_bytes = vec![10, 20, 30, 40, 50];
        assert_eq!(encode_to_hex(vec_bytes), "0A141E2832");

        let array = [171, 205, 239];
        assert_eq!(encode_to_hex(&array[..]), "ABCDEF");
    }

    #[test]
    fn test_encode_to_base64() {
        assert_eq!(encode_to_base64([]), "");
        assert_eq!(encode_to_base64([0]), "AA==");
        assert_eq!(encode_to_base64([255]), "/w==");

        assert_eq!(encode_to_base64([0, 1, 2, 3]), "AAECAw==");
        assert_eq!(encode_to_base64([255, 254, 253, 252]), "//79/A==");

        let vec_bytes = vec![72, 101, 108, 108, 111];
        assert_eq!(encode_to_base64(vec_bytes), "SGVsbG8=");

        let array = [84, 101, 115, 116, 105, 110, 103];
        assert_eq!(encode_to_base64(&array[..]), "VGVzdGluZw==");
    }

    #[test]
    fn test_parse_attribute_string() {
        // Test standard attribute format
        let attr_with_key_value = "Attribute { key: \"action\", value: \"swap\" }";
        let result = parse_attribute_string(attr_with_key_value);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "action");
        assert_eq!(value, "swap");

        let v037_attr = "V037(EventAttribute { key: \"height\", value: \"44033\", index: false })";
        let result = parse_attribute_string(v037_attr);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "height");
        assert_eq!(value, "44033");

        let attr_with_json = "event_type {\"timestamp\": 12345, \"block\": 100}";
        let result = parse_attribute_string(attr_with_json);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "event_type");
        assert_eq!(value, "{\"timestamp\": 12345, \"block\": 100}");

        let invalid_attr = "Something without key or value";
        let result = parse_attribute_string(invalid_attr);
        assert!(result.is_none());

        let empty_attr = "";
        let result = parse_attribute_string(empty_attr);
        assert!(result.is_none());
    }
}
