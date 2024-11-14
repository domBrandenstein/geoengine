use async_trait::async_trait;
use geoengine_datatypes::collections::VectorDataType;
use geoengine_datatypes::dataset::{DataId, DataProviderId, LayerId};
use geoengine_datatypes::primitives::{
    CacheTtlSeconds, RasterQueryRectangle, VectorQueryRectangle,
};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterResultDescriptor, StaticMetaData, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset, OgrSourceErrorSpec};
use postgres_types::{FromSql, ToSql};
use reqwest::Client;
use serde::{Deserialize, Deserializer};
use std::fmt::Debug;
use std::str::FromStr;
use typetag::serde;

use crate::contexts::GeoEngineDb;
use crate::datasets::listing::ProvenanceOutput;
use crate::error::Error;
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{Layer, LayerCollection, LayerCollectionListOptions};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider, ProviderCapabilities};

#[derive(Deserialize, FromSql, ToSql, Debug)]
#[serde(rename_all = "camelCase")]
struct WfsDataProviderDefinition {
    id: DataProviderId,
    endpoint: String,
    name: String,
    description: String,
    priority: Option<i16>,
    #[serde(default)]
    cache_ttl: CacheTtlSeconds,
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for WfsDataProviderDefinition {
    async fn initialize(self: Box<Self>, _db: D) -> crate::error::Result<Box<dyn DataProvider>> {
        let caps = Client::new()
            .get(format!(
                "{}?service=WFS&request=GetCapabilities",
                self.endpoint
            ))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        println!("{}", caps.clone());

        let capabilities: WfsCapabilities = serde_xml_rs::from_str(&caps).unwrap();

        Ok(Box::new(WfsDataProvider {
            id: self.id,
            endpoint: self.endpoint,
            name: self.name,
            description: self.description,
            priority: self.priority,
            cache_ttl: self.cache_ttl,
            capabilities,
        }))
    }

    fn type_name(&self) -> &'static str {
        "WFS"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        self.id
    }
}

#[derive(Debug)]
struct WfsDataProvider {
    id: DataProviderId,
    endpoint: String,
    name: String,
    description: String,
    priority: Option<i16>,
    cache_ttl: CacheTtlSeconds,
    capabilities: WfsCapabilities,
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for WfsDataProvider
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
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for WfsDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
    > {
        let layer_id = id
            .external()
            .ok_or(Error::InvalidDataId)
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?
            .layer_id
            .0;

        let layer = self
            .capabilities
            .features
            .features
            .iter()
            .find(|&feature| feature.name.eq(&layer_id))
            .ok_or(Error::InvalidDataId)
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?;

        Ok(Box::new(StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: layer.name.clone(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: Default::default(),
            },
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::Data,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: Default::default(),
                time: None,
                bbox: None,
            },
            phantom: Default::default(),
        }))
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for WfsDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl DataProvider for WfsDataProvider {
    async fn provenance(&self, id: &DataId) -> crate::error::Result<ProvenanceOutput> {
        todo!()
    }
}

#[async_trait]
impl LayerCollectionProvider for WfsDataProvider {
    fn capabilities(&self) -> ProviderCapabilities {
        todo!()
    }

    fn name(&self) -> &str {
        todo!()
    }

    fn description(&self) -> &str {
        todo!()
    }

    async fn load_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> crate::error::Result<LayerCollection> {
        todo!()
    }

    async fn get_root_layer_collection_id(&self) -> crate::error::Result<LayerCollectionId> {
        todo!()
    }

    async fn load_layer(&self, id: &LayerId) -> crate::error::Result<Layer> {
        todo!()
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename = "wfs:WFS_Capabilities")]
struct WfsCapabilities {
    #[serde(rename = "FeatureTypeList")]
    features: FeatureTypeList,
}

#[derive(Deserialize, Debug)]
struct FeatureTypeList {
    #[serde(rename = "FeatureType")]
    features: Vec<FeatureType>,
}

#[derive(Deserialize, Debug)]
struct FeatureType {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Title")]
    title: String,
    #[serde(rename = "Abstract")]
    description: String,
    #[serde(rename = "DefaultCRS")]
    default_crs: String,
    #[serde(rename = "WGS84BoundingBox")]
    bounding_box: WGS84BoundingBox,
}

#[derive(Deserialize, Debug)]
struct WGS84BoundingBox {
    #[serde(rename = "LowerCorner")]
    #[serde(deserialize_with = "from_space_separated_string")]
    upper_left: (f64, f64),
    #[serde(rename = "UpperCorner")]
    #[serde(deserialize_with = "from_space_separated_string")]
    lower_right: (f64, f64),
}

fn from_space_separated_string<'de, D>(deserializer: D) -> Result<(f64, f64), D::Error>
where
    D: Deserializer<'de>,
{
    let input: String = Deserialize::deserialize(deserializer)?;
    let split: Vec<&str> = input.split(' ').collect();
    Ok((
        f64::from_str(split[0]).unwrap(),
        f64::from_str(split[1]).unwrap(),
    ))
}
