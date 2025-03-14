use async_graphql::SimpleObject;

#[derive(SimpleObject, Clone)]
pub struct AssetId {
    pub alt_base_denom: String,
    pub alt_bech32m: String,
    pub inner: String,
}

impl Default for AssetId {
    fn default() -> Self {
        Self {
            alt_base_denom: String::new(),
            alt_bech32m: String::new(),
            inner: String::new(),
        }
    }
}
