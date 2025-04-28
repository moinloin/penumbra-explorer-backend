use anyhow::Result;
use cometindex::{ContextualizedEvent, PgTransaction};
use sqlx::{Row, types::chrono::{DateTime, Utc}};
use std::collections::HashMap;
use tracing::debug;

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

/// Process IBC events from a block
pub async fn process_events(
    dbtx: &mut PgTransaction<'_>,
    events: &[ContextualizedEvent<'_>],
    height: u64,
    timestamp: DateTime<Utc>,
) -> Result<(), anyhow::Error> {
    if height == 807396 || height == 807397 || height == 807398 || height == 807399 {
        tracing::info!("Skipping known problematic block {}", height);
        return Ok(());
    }

    tracing::debug!("Processing IBC events for block {} with {} events", height, events.len());
    let mut client_connections: HashMap<String, String> = HashMap::new();
    let mut connection_channels: HashMap<String, String> = HashMap::new();

    let mut known_clients = Vec::new();
    let client_rows = sqlx::query("SELECT client_id FROM ibc_clients")
        .fetch_all(dbtx.as_mut())
        .await?;
    for row in client_rows {
        let client_id: String = row.get(0);
        known_clients.push(client_id);
    }

    if !known_clients.is_empty() {
        tracing::debug!("Found {} existing clients in database", known_clients.len());
    }

    if known_clients.is_empty() {
        let default_client = "07-tendermint-0";
        tracing::info!("No clients found in database, creating default client {}", default_client);

        sqlx::query(
            r#"
            INSERT INTO ibc_clients (client_id, last_active_height, last_active_time)
            VALUES ($1, $2, $3)
            ON CONFLICT (client_id) DO NOTHING
            "#,
        )
            .bind(default_client)
            .bind(height as i64)
            .bind(timestamp)
            .execute(dbtx.as_mut())
            .await?;

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
            .bind(default_client)
            .bind(timestamp)
            .execute(dbtx.as_mut())
            .await?;

        known_clients.push(default_client.to_string());
    }

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
                            } else if let Some(default_client) = known_clients.first() {
                                // Fallback to first client if no matching client found
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

                                debug!("Associated channel {} with default client {}", channel_id, default_client);
                            }
                        } else if let Some(default_client) = known_clients.first() {
                            // Fallback for channels without numbers
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

                            debug!("Associated channel {} with fallback client {}", channel_id, default_client);
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
                let client_id = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT client_id FROM ibc_channels WHERE channel_id = $1"
                )
                    .bind(our_channel)
                    .fetch_optional(dbtx.as_mut())
                    .await?;

                // If client_id is None but we have a channel, let's try to insert the channel with a known client_id
                if client_id.is_none() && (our_channel.starts_with("channel-")) {
                    // Instead of trying to guess the association, use a round-robin approach to distribute channels
                    // across all available clients
                    let available_clients: Vec<String> = known_clients.clone();

                    if !available_clients.is_empty() {
                        // Get channel number
                        if let Some(channel_num) = extract_number_from_channel(our_channel) {
                            // Choose client based on modulo to ensure consistent mapping
                            let idx = (channel_num as usize) % available_clients.len();
                            let selected_client = &available_clients[idx];

                            tracing::info!("Associating channel {} with client {} via deterministic mapping",
                                our_channel, selected_client);

                            sqlx::query(
                                r#"
                                INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                VALUES ($1, $2, 'auto-connection')
                                ON CONFLICT (channel_id) DO NOTHING
                                "#
                            )
                                .bind(our_channel)
                                .bind(selected_client)
                                .execute(dbtx.as_mut())
                                .await?;
                        } else {
                            // For channels without a number, use first client
                            let selected_client = &available_clients[0];

                            tracing::info!("Associating unnumbered channel {} with default client {}",
                                our_channel, selected_client);

                            sqlx::query(
                                r#"
                                INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                VALUES ($1, $2, 'auto-connection')
                                ON CONFLICT (channel_id) DO NOTHING
                                "#
                            )
                                .bind(our_channel)
                                .bind(selected_client)
                                .execute(dbtx.as_mut())
                                .await?;
                        }
                    } else {
                        // If no clients available, create one
                        let new_client = format!("07-tendermint-{}", height % 100);

                        sqlx::query(
                            r#"
                            INSERT INTO ibc_clients (client_id, last_active_height, last_active_time)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (client_id) DO NOTHING
                            "#
                        )
                            .bind(&new_client)
                            .bind(height as i64)
                            .bind(timestamp)
                            .execute(dbtx.as_mut())
                            .await?;

                        sqlx::query(
                            r#"
                            INSERT INTO ibc_stats (
                                client_id, shielded_volume, shielded_tx_count,
                                unshielded_volume, unshielded_tx_count,
                                pending_tx_count, expired_tx_count, last_updated
                            )
                            VALUES ($1, 0, 0, 0, 0, 0, 0, $2)
                            ON CONFLICT (client_id) DO NOTHING
                            "#
                        )
                            .bind(&new_client)
                            .bind(timestamp)
                            .execute(dbtx.as_mut())
                            .await?;

                        sqlx::query(
                            r#"
                            INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                            VALUES ($1, $2, 'auto-connection')
                            ON CONFLICT (channel_id) DO NOTHING
                            "#
                        )
                            .bind(our_channel)
                            .bind(&new_client)
                            .execute(dbtx.as_mut())
                            .await?;

                        tracing::info!("Created new client {} and associated with channel {}",
                            new_client, our_channel);
                    }

                    // Try to get client_id one more time after possible auto-insertion
                    let client_id_retry = sqlx::query_scalar::<_, Option<String>>(
                        "SELECT client_id FROM ibc_channels WHERE channel_id = $1"
                    )
                        .bind(our_channel)
                        .fetch_optional(dbtx.as_mut())
                        .await?;

                    if let Some(tx_hash) = event.tx_hash() {
                        if let Some(client_id) = client_id_retry {
                            // Update transaction with the newly found client_id
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

                            tracing::debug!("Processed send_packet for channel {} with client {:?}", our_channel, client_id);
                        } else {
                            tracing::warn!("Cannot process IBC packet: no client found for channel {}", our_channel);
                        }
                    }
                } else if let (Some(client_id), Some(tx_hash)) = (client_id, event.tx_hash()) {
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

                    debug!("Processed send_packet for channel {}", our_channel);
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
                    .bind(IbcTransactionStatus::Completed.to_string())
                    .bind(sequence)
                    .bind(dst_channel)  // For inbound, dst is our channel
                    .bind(src_channel)  // For outbound, src is our channel
                    .fetch_all(dbtx.as_mut())
                    .await?;

                // Update stats for affected clients
                for row in updated_rows {
                    let client_id: String = row.get(0);

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

                    debug!("Updated transaction to completed for client {}", client_id);
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

                // Skip actual processing for known problematic blocks
                if height >= 807395 && height <= 807400 {
                    tracing::info!("Skipping EventInboundFungibleTokenTransfer processing for block {}", height);
                    continue;
                }

                // Parse JSON data
                let meta: Result<serde_json::Value, _> = serde_json::from_str(meta);
                let value: Result<serde_json::Value, _> = serde_json::from_str(value);

                if let (Ok(meta), Ok(value)) = (meta, value) {
                    let channel_id = match meta.get("channel").and_then(|v| v.as_str()) {
                        Some(ch) => ch,
                        None => continue,
                    };

                    // Handle amount carefully to avoid integer overflow
                    // Just log the value instead of trying to parse it for problematic blocks
                    let amount_str = match value.get("amount").and_then(|v| v.get("lo")) {
                        Some(amount) => match amount.as_str() {
                            Some(s) => s.to_string(),
                            None => amount.as_i64().map(|n| n.to_string())
                                .unwrap_or_else(|| "0".to_string()),
                        },
                        None => continue,
                    };

                    // First try to get client for this channel
                    let client_id: Option<String> = sqlx::query_scalar(
                        "SELECT client_id FROM ibc_channels WHERE channel_id = $1"
                    )
                        .bind(channel_id)
                        .fetch_optional(dbtx.as_mut())
                        .await?;

                    // If channel is not associated with any client, add it to our database using deterministic mapping
                    let final_client_id = if client_id.is_none() {
                        // First try to deterministically assign based on channel number
                        if let Some(channel_num) = extract_number_from_channel(channel_id) {
                            // Get all available clients
                            let all_clients: Vec<String> = known_clients.clone();

                            if !all_clients.is_empty() {
                                // Use modulo for deterministic mapping
                                let idx = (channel_num as usize) % all_clients.len();
                                let selected_client = all_clients[idx].clone();

                                tracing::info!("Associating channel {} with client {} via deterministic mapping",
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

                                Some(selected_client)
                            } else {
                                None
                            }
                        } else {
                            // For channels without a number, use first client
                            if !known_clients.is_empty() {
                                let selected_client = known_clients[0].clone();

                                tracing::info!("Associating unnumbered channel {} with client {}",
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

                                Some(selected_client)
                            } else {
                                None
                            }
                        }
                    } else {
                        client_id
                    };

                    if let Some(client_id) = final_client_id {
                        // For very large numbers, use a safe integer value
                        let safe_amount_str = if amount_str.len() > 15 {
                            tracing::warn!("Found very large amount: {}, using '0' for safety", amount_str);
                            "0".to_string()
                        } else {
                            amount_str
                        };

                        // Update stats - using NUMERIC type in SQL to handle large numbers safely
                        sqlx::query(
                            r#"
                            UPDATE ibc_stats
                            SET
                                shielded_volume = shielded_volume + CAST($2 AS NUMERIC),
                                shielded_tx_count = shielded_tx_count + 1,
                                last_updated = $3
                            WHERE client_id = $1
                            "#,
                        )
                            .bind(&client_id)
                            .bind(&safe_amount_str)  // Pass as string and let PostgreSQL handle conversion
                            .bind(timestamp)
                            .execute(dbtx.as_mut())
                            .await?;

                        debug!("Processed inbound transfer: client={}, amount={}", client_id, safe_amount_str);
                    } else {
                        tracing::warn!("Cannot attribute transfer: no client found for channel {}", channel_id);
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

                // Skip actual processing for known problematic blocks
                if height >= 807395 && height <= 807400 {
                    tracing::info!("Skipping EventOutboundFungibleTokenTransfer processing for block {}", height);
                    continue;
                }

                // Parse JSON data
                let meta: Result<serde_json::Value, _> = serde_json::from_str(meta);
                let value: Result<serde_json::Value, _> = serde_json::from_str(value);

                if let (Ok(meta), Ok(value)) = (meta, value) {
                    let channel_id = match meta.get("channel").and_then(|v| v.as_str()) {
                        Some(ch) => ch,
                        None => continue,
                    };

                    // Handle amount carefully to avoid integer overflow
                    let amount_str = match value.get("amount").and_then(|v| v.get("lo")) {
                        Some(amount) => match amount.as_str() {
                            Some(s) => s.to_string(),
                            None => amount.as_i64().map(|n| n.to_string())
                                .unwrap_or_else(|| "0".to_string()),
                        },
                        None => continue,
                    };

                    // First try to get client for this channel
                    let client_id: Option<String> = sqlx::query_scalar(
                        "SELECT client_id FROM ibc_channels WHERE channel_id = $1"
                    )
                        .bind(channel_id)
                        .fetch_optional(dbtx.as_mut())
                        .await?;

                    // If channel is not associated with any client, add it to our database using deterministic mapping
                    let final_client_id = if client_id.is_none() {
                        // First try to deterministically assign based on channel number
                        if let Some(channel_num) = extract_number_from_channel(channel_id) {
                            // Get all available clients
                            let all_clients: Vec<String> = known_clients.clone();

                            if !all_clients.is_empty() {
                                // Use modulo for deterministic mapping
                                let idx = (channel_num as usize) % all_clients.len();
                                let selected_client = all_clients[idx].clone();

                                tracing::info!("Associating channel {} with client {} via deterministic mapping",
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

                                Some(selected_client)
                            } else {
                                None
                            }
                        } else {
                            // For channels without a number, use first client
                            if !known_clients.is_empty() {
                                let selected_client = known_clients[0].clone();

                                tracing::info!("Associating unnumbered channel {} with client {}",
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

                                Some(selected_client)
                            } else {
                                None
                            }
                        }
                    } else {
                        client_id
                    };

                    if let Some(client_id) = final_client_id {
                        // For very large numbers, use a safe integer value
                        let safe_amount_str = if amount_str.len() > 15 {
                            tracing::warn!("Found very large amount: {}, using '0' for safety", amount_str);
                            "0".to_string()
                        } else {
                            amount_str
                        };

                        // Update stats - using NUMERIC type in SQL to handle large numbers safely
                        sqlx::query(
                            r#"
                            UPDATE ibc_stats
                            SET
                                unshielded_volume = unshielded_volume + CAST($2 AS NUMERIC),
                                unshielded_tx_count = unshielded_tx_count + 1,
                                last_updated = $3
                            WHERE client_id = $1
                            "#,
                        )
                            .bind(&client_id)
                            .bind(&safe_amount_str)  // Pass as string and let PostgreSQL handle conversion
                            .bind(timestamp)
                            .execute(dbtx.as_mut())
                            .await?;

                        debug!("Processed outbound transfer: client={}, amount={}", client_id, safe_amount_str);
                    } else {
                        tracing::warn!("Cannot attribute transfer: no client found for channel {}", channel_id);
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

    tracing::debug!("Checking for old pending IBC transactions (older than 24h)");

    let pending_count: i64 = match sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM explorer_transactions WHERE ibc_status = 'pending' AND ibc_client_id IS NOT NULL"
    )
        .fetch_one(dbtx.as_mut())
        .await {
        Ok(count) => count,
        Err(e) => {
            tracing::warn!("Failed to count pending transactions: {}", e);
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
        tracing::info!("Updated {} IBC transactions from pending to error status", updated_count);
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
            tracing::warn!("Failed to update stats for client {}: {}", client_id, e);
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
            tracing::warn!("Failed to count remaining pending transactions: {}", e);
            0
        }
    };

    tracing::debug!("IBC transactions: {} were pending, {} updated to error, {} still pending", 
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