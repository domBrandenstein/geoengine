use crate::datasets::listing::ProvenanceOutput;
use crate::error::{Error, Result};
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{Layer, LayerCollection, LayerCollectionListOptions};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, DataProviderId, LayerId};
use geoengine_datatypes::primitives::{
    CacheTtlSeconds, RasterQueryRectangle, VectorQueryRectangle,
};
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use postgres_types::{FromSql, ToSql};
use reqwest::Client;
use serde::{Deserialize, Serialize};

pub const IOER_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0x8d52_2635_6251_41d5_84ed_adde_9a59_4a16);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSql, FromSql)]
#[serde(rename_all = "camelCase")]
pub struct IoerDataProviderDefinition {
    pub name: String,
    pub api_key: String,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

#[async_trait]
impl DataProviderDefinition for IoerDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DataProvider>> {
        Ok(Box::new(IoerDataProvider::new(
            self.api_key,
            self.cache_ttl,
        )?))
    }

    fn type_name(&self) -> &'static str {
        "Ioer"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        IOER_PROVIDER_ID
    }
}

#[derive(Debug)]
pub struct IoerDataProvider {
    client: Client,
    api_key: String,
    cache_ttl: CacheTtlSeconds,
}

impl IoerDataProvider {
    fn new(api_key: String, cache_ttl: CacheTtlSeconds) -> Result<Self> {
        Ok(Self {
            client: Client::new(),
            api_key,
            cache_ttl,
        })
    }
}

#[async_trait]
impl LayerCollectionProvider for IoerDataProvider {
    async fn load_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        todo!()
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId("ioer".to_string()))
    }

    async fn load_layer(&self, id: &LayerId) -> Result<Layer> {
        todo!()
    }
}

#[async_trait]
impl DataProvider for IoerDataProvider {
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput> {
        todo!()
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for IoerDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
    > {
        todo!()
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for IoerDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
    > {
        todo!()
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for IoerDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
    > {
        todo!()
    }
}
