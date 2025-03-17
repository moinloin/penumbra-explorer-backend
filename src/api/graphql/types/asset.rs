use async_graphql::SimpleObject;

// Rename fields to match what the frontend expects
#[derive(SimpleObject, Clone)]
pub struct AssetId {
    pub alt_base_denom: String,
    #[graphql(name = "altBaseDenom")]
    pub _alt_base_denom: String,

    pub alt_bech32m: String,
    #[graphql(name = "altBech32M")]
    pub _alt_bech32m: String,

    pub inner: String,
}

impl Default for AssetId {
    fn default() -> Self {
        Self {
            alt_base_denom: String::new(),
            _alt_base_denom: String::new(),
            alt_bech32m: String::new(),
            _alt_bech32m: String::new(),
            inner: String::new(),
        }
    }
}

impl AssetId {
    pub fn new(alt_base_denom: String, alt_bech32m: String, inner: String) -> Self {
        Self {
            alt_base_denom: alt_base_denom.clone(),
            _alt_base_denom: alt_base_denom,
            alt_bech32m: alt_bech32m.clone(),
            _alt_bech32m: alt_bech32m,
            inner,
        }
    }
}