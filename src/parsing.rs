use anyhow::Result;
use cometindex::ContextualizedEvent;
use serde_json::{json, Value};
use std::fmt::Write;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

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

/// Parse attribute string from an event
#[must_use]
pub fn parse_attribute_string(attr_str: &str) -> Option<(String, String)> {
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
            .to_string();

        return Some((key, value));
    }

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

/// Convert event to JSON format
///
/// # Errors
/// Returns an error if the JSON serialization fails or if there's an issue with formatting the event data
pub fn event_to_json(
    event: ContextualizedEvent<'_>,
    tx_hash: Option<[u8; 32]>,
) -> Result<Value, anyhow::Error> {
    let mut attributes = Vec::new();

    for attr in &event.event.attributes {
        let attr_str = format!("{attr:?}");

        attributes.push(json!({
            "key": attr_str.clone(),
            "composite_key": format!("{}.{}", event.event.kind, attr_str),
            "value": "Unknown"
        }));
    }

    let json_event = json!({
        "block_id": event.block_height,
        "tx_id": tx_hash.map(encode_to_hex),
        "type": event.event.kind,
        "attributes": attributes
    });

    Ok(json_event)
}
