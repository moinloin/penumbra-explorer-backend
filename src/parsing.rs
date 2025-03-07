use anyhow::{anyhow, Context as _};
use penumbra_sdk_app::genesis::{AppState, Content};
use serde_json::Value;

const GENESIS_NO_CONTENT_ERROR: &str = r#"
Error: using an upgrade genesis file instead of an initial genesis file.
This genesis file only contains a checkpoint hash of the state,
rather than information about how the initial state of the chain was initialized,
at the very first genesis.
Make sure that you're using the very first genesis file, before any upgrades.
"#;

/// Attempt to parse content from a value.
///
/// This is useful to get the initial chain state for app views.
///
/// This has a nice error message, so you should use this.
pub fn parse_content(data: Value) -> anyhow::Result<Content> {
    let app_state: AppState = serde_json::from_value(data)
        .context("error decoding app_state json: make sure that this is a penumbra genesis file")?;
    let content = app_state
        .content()
        .ok_or(anyhow!(GENESIS_NO_CONTENT_ERROR))?;
    Ok(content.clone())
}

/// Helper function to convert bytes to a hexadecimal string
pub fn encode_to_hex<T: AsRef<[u8]>>(data: T) -> String {
    let bytes = data.as_ref();
    let mut hex_string = String::with_capacity(bytes.len() * 2);

    for &byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut hex_string, "{:02X}", byte);
    }

    hex_string
}

/// Parse attribute string from an event
pub fn parse_attribute_string(attr_str: &str) -> Option<(String, String)> {
    if attr_str.contains("key:") && attr_str.contains("value:") {
        let key_start = attr_str.find("key:").unwrap_or(0) + 4;
        let key_end = attr_str[key_start..].find(',').map(|pos| key_start + pos).unwrap_or(attr_str.len());
        let key = attr_str[key_start..key_end].trim().trim_matches('"').to_string();

        let value_start = attr_str.find("value:").unwrap_or(0) + 6;
        let value_end = attr_str[value_start..].find(',').map(|pos| value_start + pos).unwrap_or(attr_str.len());
        let value = attr_str[value_start..value_end].trim().trim_matches('"').to_string();

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
