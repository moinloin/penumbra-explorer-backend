use async_graphql::SimpleObject;

#[derive(SimpleObject, Clone, Default)]
#[allow(clippy::module_name_repetitions)]
pub struct AssetId {
    pub alt_base_denom: String,
    #[graphql(name = "altBaseDenom")]
    #[allow(clippy::pub_underscore_fields)]
    pub _alt_base_denom: String,

    pub alt_bech32m: String,
    #[graphql(name = "altBech32M")]
    #[allow(clippy::pub_underscore_fields)]
    pub _alt_bech32m: String,

    pub inner: String,
}

impl AssetId {
    #[must_use]
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
