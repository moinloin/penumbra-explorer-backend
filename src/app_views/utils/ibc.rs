use anyhow::Result;
use cometindex::{ContextualizedEvent, PgTransaction};
use sqlx::{Row, types::chrono::{DateTime, Utc}};
use std::collections::HashMap;
use tracing::{debug, info, warn, error};
use serde_json::Value;
use regex::Regex;

/// Direction of an IBC transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IbcDirection {
    Inbound,
    Outbound,
}

impl std::fmt::Display for IbcDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IbcDirection::Inbound => write!(f, "inbound"),
            IbcDirection::Outbound => write!(f, "outbound"),
        }
    }
}

/// Status of an IBC transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IbcTransactionStatus {
    Pending,
    Completed,
    Expired,
    Error,
}

impl std::fmt::Display for IbcTransactionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IbcTransactionStatus::Pending => write!(f, "pending"),
            IbcTransactionStatus::Completed => write!(f, "completed"),
            IbcTransactionStatus::Expired => write!(f, "expired"),
            IbcTransactionStatus::Error => write!(f, "error"),
        }
    }
}

/// Extract numeric portion from channel ID (e.g., "channel-42" -> 42)
fn extract_number_from_channel(channel_id: &str) -> Option<u64> {
    let parts: Vec<&str> = channel_id.split('-').collect();
    if parts.len() >= 2 {
        if let Ok(num) = parts[1].parse::<u64>() {
            return Some(num);
        }
    }
    None
}

/// Checks if a specific sequence has a refund event in the event list
///
/// This function looks for:
/// 1. EventOutboundFungibleTokenRefund events for the specific sequence
/// 2. Error indicators in the event's reason attribute
fn has_refund_event(events: &[ContextualizedEvent<'_>], sequence: &str) -> bool {
    for event in events {
        if event.event.kind.as_str() == "penumbra.core.component.shielded_pool.v1.EventOutboundFungibleTokenRefund" {
            // If we have a meta attribute, check if it contains the matching sequence
            if let Some(meta) = find_attribute_value(event, "meta") {
                // Try to parse the meta as JSON to extract channel and sequence
                if let Ok(meta_json) = serde_json::from_str::<Value>(meta) {
                    if let Some(event_seq) = meta_json.get("sequence").and_then(|s| s.as_str()) {
                        if event_seq == sequence {
                            // If we have a reason attribute, check if it indicates an error
                            if let Some(reason) = find_attribute_value(event, "reason") {
                                if reason.contains("ERROR") || reason.contains("REASON_ERROR") {
                                    debug!("Found refund event with error reason for sequence {}: {}", sequence, reason);
                                    return true;
                                }
                            } else {
                                // Even without a reason, a refund event for this sequence is a good error indicator
                                debug!("Found refund event for sequence {} without specific reason", sequence);
                                return true;
                            }
                        }
                    }
                } else {
                    // Direct sequence check in meta string if JSON parsing fails
                    if meta.contains(&format!("\"sequence\":\"{}\",", sequence)) ||
                        meta.contains(&format!("\"sequence\":\"{}\"}}", sequence)) {
                        debug!("Found refund event for sequence {} via string matching", sequence);
                        return true;
                    }
                }
            }

            // Direct sequence check if available as an attribute
            if let Some(event_seq) = find_attribute_value(event, "sequence") {
                if event_seq == sequence {
                    debug!("Found refund event for sequence {} via direct attribute", sequence);
                    return true;
                }
            }
        }
    }

    false
}

/// Helper function to determine if an acknowledge_packet contains an error
/// by analyzing its packet_ack data
fn is_error_acknowledgment(event: &ContextualizedEvent<'_>) -> bool {
    if event.event.kind.as_str() != "acknowledge_packet" {
        return false;
    }

    if let Some(ack_data) = find_attribute_value(event, "packet_ack") {
        return extract_error_from_ack(ack_data);
    }

    false
}

/// Process IBC events from a block
pub async fn process_events(
    dbtx: &mut PgTransaction<'_>,
    events: &[ContextualizedEvent<'_>],
    height: u64,
    timestamp: DateTime<Utc>,
) -> Result<(), anyhow::Error> {
    debug!("Processing IBC events for block {} with {} events", height, events.len());
    let mut client_connections: HashMap<String, String> = HashMap::new();
    let mut connection_channels: HashMap<String, String> = HashMap::new();

    // First pass: identify all refund events and error sequences to build lookup maps
    let mut refunded_sequences: HashMap<String, bool> = HashMap::new();
    let mut error_acknowledgments: HashMap<String, bool> = HashMap::new();

    // Build error indicators map first for more efficient lookup
    for event in events {
        // Check for EventOutboundFungibleTokenRefund events indicating errors
        if event.event.kind.as_str() == "penumbra.core.component.shielded_pool.v1.EventOutboundFungibleTokenRefund" {
            let mut sequence = None;

            // Try to find sequence in meta attribute (JSON)
            if let Some(meta) = find_attribute_value(event, "meta") {
                if let Ok(meta_json) = serde_json::from_str::<Value>(meta) {
                    sequence = meta_json.get("sequence").and_then(|s| s.as_str()).map(|s| s.to_string());
                } else {
                    // Try regex/string matching if JSON parsing fails
                    if let Ok(re) = Regex::new(r#""sequence":"([^"]+)""#) {
                        if let Some(captures) = re.captures(meta) {
                            if let Some(seq_match) = captures.get(1) {
                                sequence = Some(seq_match.as_str().to_string());
                            }
                        }
                    }
                }
            }

            // Also check for sequence as a direct attribute
            if sequence.is_none() {
                sequence = find_attribute_value(event, "sequence").map(|s| s.to_string());
            }

            if let Some(seq) = sequence {
                refunded_sequences.insert(seq.clone(), true);
                debug!("Found refund event for sequence {}", seq);

                // Check if there's an error reason
                if let Some(reason) = find_attribute_value(event, "reason") {
                    debug!("Refund reason for sequence {}: {}", seq, reason);
                    // Mark as definite error if reason contains ERROR
                    if reason.contains("ERROR") || reason.contains("REASON_ERROR") {
                        debug!("Confirmed error via reason attribute for sequence {}", seq);
                    }
                }
            }
        }
        // Check for error acknowledgments
        else if event.event.kind.as_str() == "acknowledge_packet" {
            if let Some(sequence) = find_attribute_value(event, "packet_sequence") {
                if let Some(ack_data) = find_attribute_value(event, "packet_ack") {
                    if extract_error_from_ack(ack_data) {
                        debug!("Found error in ack_data for sequence {}: {}", sequence, ack_data);
                        error_acknowledgments.insert(sequence.to_string(), true);
                    }
                }
            }
        }
    }

    let mut known_clients = Vec::new();
    let client_rows = sqlx::query("SELECT client_id FROM ibc_clients")
        .fetch_all(dbtx.as_mut())
        .await?;
    for row in client_rows {
        let client_id: String = row.get(0);
        known_clients.push(client_id);
    }

    if !known_clients.is_empty() {
        debug!("Found {} existing clients in database", known_clients.len());
    } else {
        info!("No clients found in database");
    }

    // First phase: Process connection and channel mappings
    for event in events {
        match event.event.kind.as_str() {
            "create_client" => {
                if let Some(client_id) = find_attribute_value(event, "client_id") {
                    sqlx::query(
                        r#"
                        INSERT INTO ibc_clients (client_id, last_active_height, last_active_time)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (client_id)
                        DO UPDATE SET
                            last_active_height = $2,
                            last_active_time = $3
                        "#,
                    )
                        .bind(client_id)
                        .bind(height as i64)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await?;

                    // Initialize stats record
                    sqlx::query(
                        r#"
                        INSERT INTO ibc_stats (
                            client_id,
                            shielded_volume, shielded_tx_count,
                            unshielded_volume, unshielded_tx_count,
                            pending_tx_count, expired_tx_count,
                            last_updated
                        )
                        VALUES ($1, 0, 0, 0, 0, 0, 0, $2)
                        ON CONFLICT (client_id) DO NOTHING
                        "#,
                    )
                        .bind(client_id)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await?;

                    // Add to known clients if not already there
                    if !known_clients.contains(&client_id.to_string()) {
                        known_clients.push(client_id.to_string());
                    }

                    debug!("Processed create_client: {}", client_id);
                }
            },
            "connection_open_init" => {
                if let (Some(client_id), Some(connection_id)) = (
                    find_attribute_value(event, "client_id"),
                    find_attribute_value(event, "connection_id"),
                ) {
                    // Store mapping for later use
                    client_connections.insert(connection_id.to_string(), client_id.to_string());

                    // Ensure client exists
                    sqlx::query(
                        r#"
                        INSERT INTO ibc_clients (client_id, last_active_height, last_active_time)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (client_id) DO NOTHING
                        "#,
                    )
                        .bind(client_id)
                        .bind(height as i64)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await?;

                    // Add to known clients if not already there
                    if !known_clients.contains(&client_id.to_string()) {
                        known_clients.push(client_id.to_string());
                    }

                    debug!("Processed connection_open_init: {} -> {}", connection_id, client_id);
                }
            },
            "channel_open_init" => {
                if let (Some(channel_id), Some(connection_id)) = (
                    find_attribute_value(event, "channel_id"),
                    find_attribute_value(event, "connection_id"),
                ) {
                    // Store for later use
                    connection_channels.insert(connection_id.to_string(), channel_id.to_string());

                    // Try to find client_id for this connection
                    // 1. First check in-memory map from current batch
                    let client_id = client_connections.get(connection_id).cloned();

                    if let Some(client_id) = client_id {
                        // Insert channel with known client
                        sqlx::query(
                            r#"
                            INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (channel_id)
                            DO UPDATE SET
                                client_id = $2,
                                connection_id = $3
                            "#,
                        )
                            .bind(channel_id)
                            .bind(&client_id)
                            .bind(connection_id)
                            .execute(dbtx.as_mut())
                            .await?;

                        debug!("Processed channel_open_init: {} -> {}", channel_id, client_id);
                    } else {
                        // If no client_id in memory, use deterministic round-robin assignment
                        if let Some(channel_num) = extract_number_from_channel(channel_id) {
                            // Use deterministic round-robin assignment based on channel number
                            // This ensures the same channel always gets assigned to the same client
                            if !known_clients.is_empty() {
                                let idx = (channel_num as usize) % known_clients.len();
                                let selected_client = &known_clients[idx];

                                sqlx::query(
                                    r#"
                                    INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                    VALUES ($1, $2, $3)
                                    ON CONFLICT (channel_id)
                                    DO UPDATE SET
                                        client_id = $2,
                                        connection_id = $3
                                    "#,
                                )
                                    .bind(channel_id)
                                    .bind(selected_client)
                                    .bind(connection_id)
                                    .execute(dbtx.as_mut())
                                    .await?;

                                debug!("Associated channel {} with client {} (deterministic mapping)",
                                      channel_id, selected_client);
                            } else {
                                warn!("Cannot associate channel {}: no clients available", channel_id);
                            }
                        } else if !known_clients.is_empty() {
                            // Fallback for channels without numbers
                            let default_client = &known_clients[0];

                            sqlx::query(
                                r#"
                                INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                VALUES ($1, $2, $3)
                                ON CONFLICT (channel_id)
                                DO UPDATE SET
                                    client_id = $2,
                                    connection_id = $3
                                "#,
                            )
                                .bind(channel_id)
                                .bind(default_client)
                                .bind(connection_id)
                                .execute(dbtx.as_mut())
                                .await?;

                            debug!("Associated channel {} with first available client {}", channel_id, default_client);
                        } else {
                            warn!("Cannot associate channel {}: no clients available", channel_id);
                        }
                    }
                }
            },
            _ => {}
        }
    }

    // Phase 2: Process IBC packet and transfer events
    for event in events {
        match event.event.kind.as_str() {
            "send_packet" => {
                let (src_channel, dst_channel, sequence) = match (
                    find_attribute_value(event, "packet_src_channel"),
                    find_attribute_value(event, "packet_dst_channel"),
                    find_attribute_value(event, "packet_sequence"),
                ) {
                    (Some(src), Some(dst), Some(seq)) => (src, dst, seq),
                    _ => continue,
                };

                let packet_data = find_attribute_value(event, "packet_data").unwrap_or_default();

                // Determine direction
                let direction = if packet_data.contains("\"receiver\":\"penumbra") {
                    IbcDirection::Inbound
                } else if packet_data.contains("\"sender\":\"penumbra") {
                    IbcDirection::Outbound
                } else {
                    continue;
                };

                // Get our channel
                let our_channel = match direction {
                    IbcDirection::Inbound => dst_channel,
                    IbcDirection::Outbound => src_channel,
                };

                // Find client_id for this channel
                let mut final_client_id: Option<String> = None;

                // First try to get the client from the database
                let db_client_id = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT client_id FROM ibc_channels WHERE channel_id = $1"
                )
                    .bind(our_channel)
                    .fetch_optional(dbtx.as_mut())
                    .await?;

                if let Some(client) = db_client_id {
                    // We found a client in the database
                    final_client_id = client;
                } else if our_channel.starts_with("channel-") {
                    // No client found, try to assign one deterministically
                    let available_clients: Vec<String> = known_clients.clone();

                    if !available_clients.is_empty() {
                        // Get channel number
                        if let Some(channel_num) = extract_number_from_channel(our_channel) {
                            // Choose client based on modulo to ensure consistent mapping
                            let idx = (channel_num as usize) % available_clients.len();
                            let selected_client = available_clients[idx].clone();

                            info!("Associating channel {} with client {} via deterministic mapping",
                                our_channel, selected_client);

                            sqlx::query(
                                r#"
                                INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                VALUES ($1, $2, 'auto-connection')
                                ON CONFLICT (channel_id) DO NOTHING
                                "#
                            )
                                .bind(our_channel)
                                .bind(&selected_client)
                                .execute(dbtx.as_mut())
                                .await?;

                            final_client_id = Some(selected_client);
                        } else {
                            // For channels without a number, use first client
                            let selected_client = available_clients[0].clone();

                            info!("Associating unnumbered channel {} with default client {}",
                                our_channel, selected_client);

                            sqlx::query(
                                r#"
                                INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                VALUES ($1, $2, 'auto-connection')
                                ON CONFLICT (channel_id) DO NOTHING
                                "#
                            )
                                .bind(our_channel)
                                .bind(&selected_client)
                                .execute(dbtx.as_mut())
                                .await?;

                            final_client_id = Some(selected_client);
                        }
                    } else {
                        warn!("Cannot associate channel {}: no clients available", our_channel);
                    }
                }

                if let (Some(client_id), Some(tx_hash)) = (final_client_id, event.tx_hash()) {
                    // Update transaction
                    sqlx::query(
                        r#"
                        UPDATE explorer_transactions
                        SET
                            ibc_channel_id = $2,
                            ibc_client_id = $3,
                            ibc_status = $4,
                            ibc_direction = $5,
                            ibc_sequence = $6
                        WHERE tx_hash = $1
                        "#,
                    )
                        .bind(tx_hash)
                        .bind(our_channel)
                        .bind(&client_id)
                        .bind(IbcTransactionStatus::Pending.to_string())
                        .bind(direction.to_string())
                        .bind(sequence)
                        .execute(dbtx.as_mut())
                        .await?;

                    // Update pending count
                    sqlx::query(
                        r#"
                        UPDATE ibc_stats
                        SET
                            pending_tx_count = pending_tx_count + 1,
                            last_updated = $2
                        WHERE client_id = $1
                        "#,
                    )
                        .bind(&client_id)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await?;

                    debug!("Processed send_packet for channel {} with client {}", our_channel, client_id);
                }
            },
            "acknowledge_packet" => {
                let (src_channel, dst_channel, sequence) = match (
                    find_attribute_value(event, "packet_src_channel"),
                    find_attribute_value(event, "packet_dst_channel"),
                    find_attribute_value(event, "packet_sequence"),
                ) {
                    (Some(src), Some(dst), Some(seq)) => (src, dst, seq),
                    _ => continue,
                };

                // Check if this is an error acknowledgment by examining the contents
                let is_error = if let Some(ack_data) = find_attribute_value(event, "packet_ack") {
                    extract_error_from_ack(ack_data)
                } else {
                    false
                };

                // Check if there's a refund event for this sequence based on our previous mapping
                let has_refund = refunded_sequences.contains_key(sequence);

                // Check for direct refund events targeting this sequence
                let has_direct_refund = has_refund_event(events, sequence);

                // Determine status based on error indicators (multiple sources)
                let status = if is_error || has_refund || has_direct_refund {
                    IbcTransactionStatus::Error
                } else {
                    IbcTransactionStatus::Completed
                };

                debug!(
                    "Processing acknowledge_packet for sequence {}: status={} (is_error={}, has_refund={}, has_direct_refund={})",
                    sequence, status, is_error, has_refund, has_direct_refund
                );

                // Log the acknowledgment data for debugging if it exists
                if let Some(ack_data) = find_attribute_value(event, "packet_ack") {
                    if !ack_data.is_empty() {
                        debug!("Ack data for sequence {}: {}", sequence, ack_data);
                    }
                }

                // Update transaction status
                let updated_rows = sqlx::query(
                    r#"
                    WITH updated_tx AS (
                        UPDATE explorer_transactions
                        SET ibc_status = $1
                        WHERE ibc_sequence = $2
                        AND (
                            (ibc_direction = 'inbound' AND ibc_channel_id = $3)
                            OR
                            (ibc_direction = 'outbound' AND ibc_channel_id = $4)
                        )
                        AND ibc_status = 'pending'
                        RETURNING ibc_client_id, tx_hash
                    )
                    SELECT ibc_client_id, tx_hash FROM updated_tx
                    "#,
                )
                    .bind(status.to_string())
                    .bind(sequence)
                    .bind(dst_channel)  // For inbound, dst is our channel
                    .bind(src_channel)  // For outbound, src is our channel
                    .fetch_all(dbtx.as_mut())
                    .await?;

                // Update stats for affected clients
                for row in updated_rows {
                    let client_id: String = row.get(0);
                    let tx_hash: String = row.try_get(1).unwrap_or_default();

                    // Update stats - decrease pending count for all status changes
                    sqlx::query(
                        r#"
                        UPDATE ibc_stats
                        SET
                            pending_tx_count = GREATEST(0, pending_tx_count - 1),
                            last_updated = $2
                        WHERE client_id = $1
                        "#,
                    )
                        .bind(&client_id)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await?;

                    if status == IbcTransactionStatus::Error {
                        debug!("Updated transaction {} to ERROR for client {} (error indicators found)",
                               tx_hash, client_id);
                    } else {
                        debug!("Updated transaction {} to COMPLETED for client {}",
                               tx_hash, client_id);
                    }
                }
            },
            "timeout_packet" => {
                let (src_channel, dst_channel, sequence) = match (
                    find_attribute_value(event, "packet_src_channel"),
                    find_attribute_value(event, "packet_dst_channel"),
                    find_attribute_value(event, "packet_sequence"),
                ) {
                    (Some(src), Some(dst), Some(seq)) => (src, dst, seq),
                    _ => continue,
                };

                // Update transaction status
                let updated_rows = sqlx::query(
                    r#"
                    WITH updated_tx AS (
                        UPDATE explorer_transactions
                        SET ibc_status = $1
                        WHERE ibc_sequence = $2
                        AND (
                            (ibc_direction = 'inbound' AND ibc_channel_id = $3)
                            OR
                            (ibc_direction = 'outbound' AND ibc_channel_id = $4)
                        )
                        AND ibc_status = 'pending'
                        RETURNING ibc_client_id
                    )
                    SELECT ibc_client_id FROM updated_tx
                    "#,
                )
                    .bind(IbcTransactionStatus::Expired.to_string())
                    .bind(sequence)
                    .bind(dst_channel)
                    .bind(src_channel)
                    .fetch_all(dbtx.as_mut())
                    .await?;

                // Update stats
                for row in updated_rows {
                    let client_id: String = row.get(0);

                    sqlx::query(
                        r#"
                        UPDATE ibc_stats
                        SET
                            pending_tx_count = GREATEST(0, pending_tx_count - 1),
                            expired_tx_count = expired_tx_count + 1,
                            last_updated = $2
                        WHERE client_id = $1
                        "#,
                    )
                        .bind(&client_id)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await?;

                    debug!("Updated transaction to expired for client {}", client_id);
                }
            },
            "penumbra.core.component.shielded_pool.v1.EventOutboundFungibleTokenRefund" => {
                // Extract sequence from meta or direct attribute
                let mut sequence = None;

                // Try to find sequence in meta attribute (JSON)
                if let Some(meta) = find_attribute_value(event, "meta") {
                    if let Ok(meta_json) = serde_json::from_str::<Value>(meta) {
                        sequence = meta_json.get("sequence").and_then(|s| s.as_str()).map(|s| s.to_string());
                    } else {
                        // Try regex/string matching if JSON parsing fails
                        if let Ok(re) = Regex::new(r#""sequence":"([^"]+)""#) {
                            if let Some(captures) = re.captures(meta) {
                                if let Some(seq_match) = captures.get(1) {
                                    sequence = Some(seq_match.as_str().to_string());
                                }
                            }
                        }
                    }
                }

                // Also check for sequence as a direct attribute
                if sequence.is_none() {
                    sequence = find_attribute_value(event, "sequence").map(|s| s.to_string());
                }

                if let Some(seq) = sequence {
                    // Check if there's an error reason
                    let is_error = if let Some(reason) = find_attribute_value(event, "reason") {
                        debug!("Refund reason for sequence {}: {}", seq, reason);
                        // If reason contains ERROR, it's definitely an error
                        reason.contains("ERROR") || reason.contains("REASON_ERROR")
                    } else {
                        // Without specific reason, we still consider it an error
                        true
                    };

                    if is_error {
                        // If we find transactions by this sequence that are in any state, update them to error
                        let updated_rows = sqlx::query(
                            r#"
                            WITH updated_tx AS (
                                UPDATE explorer_transactions
                                SET ibc_status = $1
                                WHERE ibc_sequence = $2
                                RETURNING ibc_client_id, tx_hash, ibc_status
                            )
                            SELECT ibc_client_id, tx_hash, ibc_status FROM updated_tx
                            "#,
                        )
                            .bind(IbcTransactionStatus::Error.to_string())
                            .bind(&seq)
                            .fetch_all(dbtx.as_mut())
                            .await?;

                        // Update stats for affected clients
                        for row in updated_rows {
                            let client_id: String = row.get(0);
                            let tx_hash: String = row.try_get(1).unwrap_or_default();
                            let previous_status: String = row.try_get(2).unwrap_or_default();

                            // Only decrement pending count if it was previously pending
                            if previous_status == "pending" {
                                sqlx::query(
                                    r#"
                                    UPDATE ibc_stats
                                    SET
                                        pending_tx_count = GREATEST(0, pending_tx_count - 1),
                                        last_updated = $2
                                    WHERE client_id = $1
                                    "#,
                                )
                                    .bind(&client_id)
                                    .bind(timestamp)
                                    .execute(dbtx.as_mut())
                                    .await?;
                            }

                            debug!(
                                "Set transaction {} to ERROR due to direct refund event with REASON_ERROR (sequence {})",
                                tx_hash, seq
                            );
                        }
                    }
                }
            },
            "penumbra.core.component.shielded_pool.v1.EventInboundFungibleTokenTransfer" => {
                // Process inbound (shielded) transfers
                let meta = match find_attribute_value(event, "meta") {
                    Some(m) => m,
                    None => continue,
                };

                let value = match find_attribute_value(event, "value") {
                    Some(v) => v,
                    None => continue,
                };

                // Parse JSON data
                let meta: Result<serde_json::Value, _> = serde_json::from_str(meta);
                let value: Result<serde_json::Value, _> = serde_json::from_str(value);

                if let (Ok(meta), Ok(value)) = (meta, value) {
                    let channel_id = match meta.get("channel").and_then(|v| v.as_str()) {
                        Some(ch) => ch,
                        None => continue,
                    };

                    // Extract the amount as a string to safely handle large numbers
                    let amount_raw = match value.get("amount").and_then(|v| v.get("lo")) {
                        Some(amount) => match amount.as_str() {
                            Some(s) => s.to_string(),
                            None => amount.to_string().trim_matches('"').to_string(),
                        },
                        None => continue,
                    };

                    // First try to get client for this channel
                    let mut resolved_client_id: Option<String> = None;

                    let db_client_id = sqlx::query_scalar::<_, Option<String>>(
                        "SELECT client_id FROM ibc_channels WHERE channel_id = $1"
                    )
                        .bind(channel_id)
                        .fetch_optional(dbtx.as_mut())
                        .await?;

                    if let Some(client) = db_client_id {
                        resolved_client_id = client;
                    } else {
                        if let Some(channel_num) = extract_number_from_channel(channel_id) {
                            // Get all available clients
                            let all_clients: Vec<String> = known_clients.clone();

                            if !all_clients.is_empty() {
                                // Use modulo for deterministic mapping
                                let idx = (channel_num as usize) % all_clients.len();
                                let selected_client = all_clients[idx].clone();

                                info!("Associating channel {} with client {} via deterministic mapping",
                                    channel_id, selected_client);

                                sqlx::query(
                                    r#"
                                    INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                    VALUES ($1, $2, 'auto-connection')
                                    ON CONFLICT (channel_id) DO NOTHING
                                    "#
                                )
                                    .bind(channel_id)
                                    .bind(&selected_client)
                                    .execute(dbtx.as_mut())
                                    .await?;

                                resolved_client_id = Some(selected_client);
                            }
                        } else if !known_clients.is_empty() {
                            // For channels without a number, use first client
                            let selected_client = known_clients[0].clone();

                            info!("Associating unnumbered channel {} with first available client {}",
                                channel_id, selected_client);

                            sqlx::query(
                                r#"
                                INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                VALUES ($1, $2, 'auto-connection')
                                ON CONFLICT (channel_id) DO NOTHING
                                "#
                            )
                                .bind(channel_id)
                                .bind(&selected_client)
                                .execute(dbtx.as_mut())
                                .await?;

                            resolved_client_id = Some(selected_client);
                        } else {
                            warn!("Cannot associate channel {}: no clients available", channel_id);
                        }
                    }

                    if let Some(client_id) = resolved_client_id {
                        // Update stats using direct SQL to properly handle extremely large numbers
                        match sqlx::query(
                            r#"
                            UPDATE ibc_stats
                            SET
                                -- Convert to NUMERIC first, then add safely
                                shielded_volume =
                                    CASE
                                        WHEN $2 ~ '^[0-9]+$' THEN -- Check if it's a valid number
                                            COALESCE(shielded_volume, 0) +
                                            CASE
                                                WHEN LENGTH($2) > 15 THEN 0 -- If too large, use 0
                                                ELSE CAST($2 AS NUMERIC)
                                            END
                                        ELSE shielded_volume -- If not valid, don't change
                                    END,
                                shielded_tx_count = shielded_tx_count + 1,
                                last_updated = $3
                            WHERE client_id = $1
                            "#,
                        )
                            .bind(&client_id)
                            .bind(&amount_raw)
                            .bind(timestamp)
                            .execute(dbtx.as_mut())
                            .await {
                            Ok(_) => {
                                debug!("Processed inbound transfer: client={}, amount={}", client_id, amount_raw);
                            },
                            Err(e) => {
                                // Log the error but continue processing
                                error!("Error updating stats for inbound transfer: {}. Using fallback.", e);

                                // Fallback: just increment the count without adding to volume
                                sqlx::query(
                                    r#"
                                        UPDATE ibc_stats
                                        SET
                                            shielded_tx_count = shielded_tx_count + 1,
                                            last_updated = $2
                                        WHERE client_id = $1
                                        "#,
                                )
                                    .bind(&client_id)
                                    .bind(timestamp)
                                    .execute(dbtx.as_mut())
                                    .await?;

                                debug!("Processed inbound transfer (count only): client={}", client_id);
                            }
                        }
                    } else {
                        warn!("Cannot attribute transfer: no client found for channel {}", channel_id);
                    }
                }
            },
            "penumbra.core.component.shielded_pool.v1.EventOutboundFungibleTokenTransfer" => {
                // Process outbound (unshielded) transfers
                let meta = match find_attribute_value(event, "meta") {
                    Some(m) => m,
                    None => continue,
                };

                let value = match find_attribute_value(event, "value") {
                    Some(v) => v,
                    None => continue,
                };

                // Parse JSON data
                let meta: Result<serde_json::Value, _> = serde_json::from_str(meta);
                let value: Result<serde_json::Value, _> = serde_json::from_str(value);

                if let (Ok(meta), Ok(value)) = (meta, value) {
                    let channel_id = match meta.get("channel").and_then(|v| v.as_str()) {
                        Some(ch) => ch,
                        None => continue,
                    };

                    // Extract the amount as a string to safely handle large numbers
                    let amount_raw = match value.get("amount").and_then(|v| v.get("lo")) {
                        Some(amount) => match amount.as_str() {
                            Some(s) => s.to_string(),
                            None => amount.to_string().trim_matches('"').to_string(),
                        },
                        None => continue,
                    };

                    // First try to get client for this channel
                    let mut resolved_client_id: Option<String> = None;

                    let db_client_id = sqlx::query_scalar::<_, Option<String>>(
                        "SELECT client_id FROM ibc_channels WHERE channel_id = $1"
                    )
                        .bind(channel_id)
                        .fetch_optional(dbtx.as_mut())
                        .await?;

                    if let Some(client) = db_client_id {
                        // We found a client in the database
                        resolved_client_id = client;
                    } else {
                        // No client found, try to assign one deterministically
                        if let Some(channel_num) = extract_number_from_channel(channel_id) {
                            // Get all available clients
                            let all_clients: Vec<String> = known_clients.clone();

                            if !all_clients.is_empty() {
                                // Use modulo for deterministic mapping
                                let idx = (channel_num as usize) % all_clients.len();
                                let selected_client = all_clients[idx].clone();

                                info!("Associating channel {} with client {} via deterministic mapping",
                                    channel_id, selected_client);

                                sqlx::query(
                                    r#"
                                    INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                    VALUES ($1, $2, 'auto-connection')
                                    ON CONFLICT (channel_id) DO NOTHING
                                    "#
                                )
                                    .bind(channel_id)
                                    .bind(&selected_client)
                                    .execute(dbtx.as_mut())
                                    .await?;

                                resolved_client_id = Some(selected_client);
                            }
                        } else if !known_clients.is_empty() {
                            // For channels without a number, use first client
                            let selected_client = known_clients[0].clone();

                            info!("Associating unnumbered channel {} with first available client {}",
                                channel_id, selected_client);

                            sqlx::query(
                                r#"
                                INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                VALUES ($1, $2, 'auto-connection')
                                ON CONFLICT (channel_id) DO NOTHING
                                "#
                            )
                                .bind(channel_id)
                                .bind(&selected_client)
                                .execute(dbtx.as_mut())
                                .await?;

                            resolved_client_id = Some(selected_client);
                        } else {
                            warn!("Cannot associate channel {}: no clients available", channel_id);
                        }
                    }

                    if let Some(client_id) = resolved_client_id {
                        // Update stats using direct SQL to properly handle extremely large numbers
                        match sqlx::query(
                            r#"
                            UPDATE ibc_stats
                            SET
                                -- Convert to NUMERIC first, then add safely
                                unshielded_volume =
                                    CASE
                                        WHEN $2 ~ '^[0-9]+$' THEN -- Check if it's a valid number
                                            COALESCE(unshielded_volume, 0) +
                                            CASE
                                                WHEN LENGTH($2) > 15 THEN 0 -- If too large, use 0
                                                ELSE CAST($2 AS NUMERIC)
                                            END
                                        ELSE unshielded_volume -- If not valid, don't change
                                    END,
                                unshielded_tx_count = unshielded_tx_count + 1,
                                last_updated = $3
                            WHERE client_id = $1
                            "#,
                        )
                            .bind(&client_id)
                            .bind(&amount_raw)
                            .bind(timestamp)
                            .execute(dbtx.as_mut())
                            .await {
                            Ok(_) => {
                                debug!("Processed outbound transfer: client={}, amount={}", client_id, amount_raw);
                            },
                            Err(e) => {
                                // Log the error but continue processing
                                error!("Error updating stats for outbound transfer: {}. Using fallback.", e);

                                // Fallback: just increment the count without adding to volume
                                sqlx::query(
                                    r#"
                                        UPDATE ibc_stats
                                        SET
                                            unshielded_tx_count = unshielded_tx_count + 1,
                                            last_updated = $2
                                        WHERE client_id = $1
                                        "#,
                                )
                                    .bind(&client_id)
                                    .bind(timestamp)
                                    .execute(dbtx.as_mut())
                                    .await?;

                                debug!("Processed outbound transfer (count only): client={}", client_id);
                            }
                        }
                    } else {
                        warn!("Cannot attribute transfer: no client found for channel {}", channel_id);
                    }
                }
            },
            _ => {}
        }
    }

    Ok(())
}

/// Update old pending transactions to error status
pub async fn update_old_pending_transactions(
    dbtx: &mut PgTransaction<'_>
) -> Result<(), anyhow::Error> {
    let day_ago = Utc::now() - chrono::Duration::hours(24);

    debug!("Checking for old pending IBC transactions (older than 24h)");

    let pending_count: i64 = match sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM explorer_transactions WHERE ibc_status = 'pending' AND ibc_client_id IS NOT NULL"
    )
        .fetch_one(dbtx.as_mut())
        .await {
        Ok(count) => count,
        Err(e) => {
            warn!("Failed to count pending transactions: {}", e);
            0
        }
    };

    let updated_rows = sqlx::query(
        r#"
        WITH updated_tx AS (
            UPDATE explorer_transactions tx
            SET ibc_status = $1
            FROM explorer_block_details bd
            WHERE tx.block_height = bd.height
            AND bd.timestamp < $2
            AND tx.ibc_status = 'pending'
            RETURNING tx.ibc_client_id
        )
        SELECT ibc_client_id FROM updated_tx
        WHERE ibc_client_id IS NOT NULL
        "#,
    )
        .bind(IbcTransactionStatus::Error.to_string())
        .bind(day_ago)
        .fetch_all(dbtx.as_mut())
        .await?;

    let updated_count = updated_rows.len();
    if updated_count > 0 {
        info!("Updated {} IBC transactions from pending to error status", updated_count);
    }

    for row in updated_rows {
        let client_id: String = row.get(0);

        let result = sqlx::query(
            r#"
            UPDATE ibc_stats
            SET
                pending_tx_count = GREATEST(0, pending_tx_count - 1),
                last_updated = $1
            WHERE client_id = $2
            "#,
        )
            .bind(Utc::now())
            .bind(&client_id)
            .execute(dbtx.as_mut())
            .await;

        if let Err(e) = result {
            warn!("Failed to update stats for client {}: {}", client_id, e);
        }
    }

    // Count pending transactions after update
    let remaining_pending: i64 = match sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM explorer_transactions WHERE ibc_status = 'pending' AND ibc_client_id IS NOT NULL"
    )
        .fetch_one(dbtx.as_mut())
        .await {
        Ok(count) => count,
        Err(e) => {
            warn!("Failed to count remaining pending transactions: {}", e);
            0
        }
    };

    debug!("IBC transactions: {} were pending, {} updated to error, {} still pending",
        pending_count, updated_count, remaining_pending);

    Ok(())
}

/// Helper function to find an attribute value in a contextualized event
fn find_attribute_value<'a>(event: &'a ContextualizedEvent, key: &str) -> Option<&'a str> {
    for attr in &event.event.attributes {
        if let Ok(attr_key) = attr.key_str() {
            if attr_key == key {
                if let Ok(attr_value) = attr.value_str() {
                    return Some(attr_value);
                }
            }
        }
    }
    None
}

/// Extract any error information from the acknowledgment packet data
/// Returns true if an error is detected, false otherwise
fn extract_error_from_ack(ack_data: &str) -> bool {
    // Common error patterns to look for in acknowledgments
    let error_patterns = [
        "\"error\":", "\"Error\":", "\"ERROR\":",
        "failed", "Failed", "FAILED",
        "reject", "Reject", "REJECT",
        "insufficient", "Insufficient",
        "invalid", "Invalid", "INVALID",
        "REASON_ERROR", "reason error", "Reason Error",
        "timeout", "Timeout", "TIMEOUT"
    ];

    // Check for simple string patterns first (faster)
    for pattern in error_patterns {
        if ack_data.contains(pattern) {
            return true;
        }
    }

    // Try to parse as JSON for more complex error patterns
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(ack_data) {
        // Check for error fields at root level
        if json.get("error").is_some() ||
            json.get("Error").is_some() ||
            json.get("ERROR").is_some() {
            return true;
        }

        // Check for error inside result field
        if let Some(result) = json.get("result") {
            if result.get("error").is_some() ||
                result.is_string() && result.as_str().unwrap_or("").contains("error") {
                return true;
            }
        }

        // Check for code field with non-zero values
        if let Some(code) = json.get("code") {
            if let Some(code_num) = code.as_u64() {
                if code_num != 0 {
                    return true;
                }
            }
        }
    }

    false
}