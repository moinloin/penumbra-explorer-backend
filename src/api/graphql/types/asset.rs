use async_graphql::SimpleObject;

#[derive(SimpleObject, Clone, Default)]
pub struct AssetId {
    pub alt_base_denom: String,
    #[graphql(name = "altBaseDenom")]
    pub _alt_base_denom: String,

    pub alt_bech32m: String,
    #[graphql(name = "altBech32M")]
    pub _alt_bech32m: String,

    pub inner: String,
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