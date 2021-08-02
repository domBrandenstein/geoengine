mod feature_collection_merger;
mod raster_subquery_adapter;
mod raster_time;
mod raster_time_substream;
mod stream_statistics_adapter;

pub use feature_collection_merger::FeatureCollectionChunkMerger;
pub use raster_subquery_adapter::{
    fold_by_coordinate_lookup_future, FoldTileAccu, FoldTileAccuMut, RasterSubQueryAdapter,
    SubQueryTileAggregator, TileReprojectionSubQuery,
};
pub use raster_time::RasterTimeAdapter;

use self::{raster_time_substream::RasterTimeMultiFold};
pub use stream_statistics_adapter::StreamStatisticsAdapter;
use crate::util::Result;
use futures::{stream::Fuse, Future, Stream, StreamExt};
use geoengine_datatypes::{
    collections::FeatureCollection,
    primitives::Geometry,
    raster::{Pixel, RasterTile2D},
    util::arrow::ArrowTyped,
};

/// This trait extends `RasterTile2D` `Stream`s with Geo-Engine-specific functionality.
///
pub trait RasterStreamExt<P>: Stream<Item = Result<RasterTile2D<P>>>
where
    P: Pixel,
{
    /// This function performs multiple fold operations on a raster stream and outputs a new stream of results.
    /// For all raster tiles of the same interval, one fold is performed and one output is generated.
    /// Before each fold, the accumulator is generated by calling `accum_init_fn`.
    /// Within each fold, new raster tiles are processed by calling `fold_fn`.
    ///
    /// This method assumes all raster tiles arrive geo first, time second.
    ///
    fn time_multi_fold<Accum, AccumInitFn, FoldFn, Fut>(
        self,
        accum_init_fn: AccumInitFn,
        fold_fn: FoldFn,
    ) -> RasterTimeMultiFold<Self, Accum, AccumInitFn, FoldFn, Fut>
    where
        Self: Sized,
        AccumInitFn: FnMut() -> Accum,
        FoldFn: FnMut(Accum, Self::Item) -> Fut,
        Fut: Future<Output = Accum>,
    {
        RasterTimeMultiFold::new(self, accum_init_fn, fold_fn)
    }

    /// Wraps a `Stream` with a `StreamStatisticsAdapter`.
    fn statistics_with_id(self, id: String) -> StreamStatisticsAdapter<Self> where Self: Stream + Sized {
        StreamStatisticsAdapter::statistics_with_id(self, id)
    }
}

impl<T: ?Sized, P: Pixel> RasterStreamExt<P> for T where T: Stream<Item = Result<RasterTile2D<P>>> {}

/// This trait extends `FeatureCollection` `Stream`s with Geo-Engine-specific functionality.
///
pub trait FeatureCollectionStreamExt<CollectionType>:
    Stream<Item = Result<FeatureCollection<CollectionType>>>
where
    CollectionType: Geometry + ArrowTyped + 'static,
{
    /// Transforms a `Stream` of `FeatureCollection`s and merges them in a way that they
    /// are `chunk_size_bytes` large.
    fn merge_chunks(
        self,
        chunk_size_bytes: usize,
    ) -> FeatureCollectionChunkMerger<Fuse<Self>, CollectionType>
    where
        Self: Sized,
    {
        FeatureCollectionChunkMerger::new(self.fuse(), chunk_size_bytes)
    }

    /// Wraps a `Stream` with a `StreamStatisticsAdapter`.
    fn statistics_with_id(self, id: String) -> StreamStatisticsAdapter<Self> where Self: Stream + Sized {
        StreamStatisticsAdapter::statistics_with_id(self, id)
    }
}

impl<T: ?Sized, CollectionType: Geometry + ArrowTyped + 'static>
    FeatureCollectionStreamExt<CollectionType> for T
where
    T: Stream<Item = Result<FeatureCollection<CollectionType>>>,
{
}
