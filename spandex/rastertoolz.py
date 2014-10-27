import numpy as np
import rasterio
from osgeo import gdal, ogr
from rasterstats.utils import (bbox_to_pixel_offsets, shapely_to_ogr_type, get_features,
                               raster_extent_as_bounds)
from shapely.geometry import shape, box, MultiPolygon


"""Contains untested raster functions."""


def from_geotiff(path_to_tif):
    with rasterio.drivers(CPL_DEBUG=True):
        with rasterio.open(path_to_tif) as src:
            b, g, r = src.read()

        total = np.zeros(r.shape, dtype=rasterio.uint16)
        for band in r, g, b:
            total += band
        total /= 3

    return total, src


def to_geotiff(array, src, path_to_tif):
        kwargs = src.meta
        kwargs.update(
            dtype=rasterio.uint8,
            count=1,
            compress='lzw')

        with rasterio.open(path_to_tif, 'w', **kwargs) as dst:
            dst.write_band(1, array.astype(rasterio.uint8))


# Modified version of rasterstats function of same name.  Added functionality to
# return the np array image of each geometry and apply arbitrary function instead
# of precanned set.  See notebook in the spandex examples dir for example usage.
def zonal_stats(vectors, raster, layer_num=0, band_num=1, func=None,
                nodata_value=None, categorical=False, stats=None,
                copy_properties=False, all_touched=False, transform=None):

    if not stats:
        if not categorical:
            stats = ['count', 'min', 'max', 'mean', 'std']
            if func:
                stats.append('func')

    # must have transform arg
    if not transform:
        raise Exception("Must provide the 'transform' kwarg")
    rgt = transform
    rsize = (raster.shape[1], raster.shape[0])

    rbounds = raster_extent_as_bounds(rgt, rsize)
    features_iter, strategy, spatial_ref = get_features(vectors, layer_num)
    global_src_offset = (0, 0, raster.shape[0], raster.shape[1])
    global_src_array = raster

    mem_drv = ogr.GetDriverByName('Memory')
    driver = gdal.GetDriverByName('MEM')

    results = []
    entity_images = []

    for i, feat in enumerate(features_iter):
        if feat['type'] == "Feature":
            geom = shape(feat['geometry'])
        else:  # it's just a geometry
            geom = shape(feat)

        # Point and MultiPoint don't play well with GDALRasterize
        # convert them into box polygons the size of a raster cell
        buff = rgt[1] / 2.0
        if geom.type == "MultiPoint":
            geom = MultiPolygon([box(*(pt.buffer(buff).bounds))
                                for pt in geom.geoms])
        elif geom.type == 'Point':
            geom = box(*(geom.buffer(buff).bounds))

        ogr_geom_type = shapely_to_ogr_type(geom.type)

        # "Clip" the geometry bounds to the overall raster bounding box
        # This should avoid any rasterIO errors for partially overlapping polys
        geom_bounds = list(geom.bounds)
        if geom_bounds[0] < rbounds[0]:
            geom_bounds[0] = rbounds[0]
        if geom_bounds[1] < rbounds[1]:
            geom_bounds[1] = rbounds[1]
        if geom_bounds[2] > rbounds[2]:
            geom_bounds[2] = rbounds[2]
        if geom_bounds[3] > rbounds[3]:
            geom_bounds[3] = rbounds[3]

        # calculate new geotransform of the feature subset
        src_offset = bbox_to_pixel_offsets(rgt, geom_bounds)

        new_gt = (
            (rgt[0] + (src_offset[0] * rgt[1])),
            rgt[1],
            0.0,
            (rgt[3] + (src_offset[1] * rgt[5])),
            0.0,
            rgt[5]
        )

        if src_offset[2] <= 0 or src_offset[3] <= 0:
            # we're off the raster completely, no overlap at all
            # so there's no need to even bother trying to calculate
            feature_stats = dict([(s, None) for s in stats])
            img = {'__fid__': i, 'img': None}
        else:
            # derive array from global source extent array
            # useful *only* when disk IO or raster format inefficiencies
            # are your limiting factor
            # advantage: reads raster data in one pass before loop
            # disadvantage: large vector extents combined with big rasters
            # need lotsa memory
            xa = src_offset[0] - global_src_offset[0]
            ya = src_offset[1] - global_src_offset[1]
            xb = xa + src_offset[2]
            yb = ya + src_offset[3]
            src_array = global_src_array[ya:yb, xa:xb]

            # Create a temporary vector layer in memory
            mem_ds = mem_drv.CreateDataSource('out')
            mem_layer = mem_ds.CreateLayer('out', spatial_ref, ogr_geom_type)
            ogr_feature = ogr.Feature(feature_def=mem_layer.GetLayerDefn())
            ogr_geom = ogr.CreateGeometryFromWkt(geom.wkt)
            ogr_feature.SetGeometryDirectly(ogr_geom)
            mem_layer.CreateFeature(ogr_feature)

            # Rasterize it
            rvds = driver.Create(
                'rvds', src_offset[2], src_offset[3], 1, gdal.GDT_Byte)
            rvds.SetGeoTransform(new_gt)

            if all_touched:
                gdal.RasterizeLayer(
                    rvds, [1], mem_layer, None, None,
                    burn_values=[1], options=['ALL_TOUCHED=True'])
            else:
                gdal.RasterizeLayer(
                    rvds, [1], mem_layer, None, None,
                    burn_values=[1], options=['ALL_TOUCHED=False'])
            rv_array = rvds.ReadAsArray()

            # Mask the source data array with our current feature
            # we take the logical_not to flip 0<->1 to get the correct mask effect
            # we also mask out nodata values explictly
            masked = np.ma.MaskedArray(
                src_array,
                mask=np.logical_or(
                    src_array == nodata_value,
                    np.logical_not(rv_array)
                )
            )

            feature_stats = {}

            if 'min' in stats:
                feature_stats['min'] = float(masked.min())
            if 'max' in stats:
                feature_stats['max'] = float(masked.max())
            if 'mean' in stats:
                feature_stats['mean'] = float(masked.mean())
            if 'count' in stats:
                feature_stats['count'] = int(masked.count())
            if 'std' in stats:
                feature_stats['std'] = float(masked.std())
            # optional
            if 'func' in stats:
                feature_stats[func.__name__] = func(masked)
            if 'sum' in stats:
                feature_stats['sum'] = float(masked.sum())
            if 'std' in stats:
                feature_stats['std'] = float(masked.std())
            if 'median' in stats:
                feature_stats['median'] = float(np.median(masked.compressed()))
            if 'range' in stats:
                try:
                    rmin = feature_stats['min']
                except KeyError:
                    rmin = float(masked.min())
                try:
                    rmax = feature_stats['max']
                except KeyError:
                    rmax = float(masked.max())
                feature_stats['range'] = rmax - rmin
            img = {'__fid__': i, 'img': masked}

        # Use the enumerated id as __fid__
        feature_stats['__fid__'] = i

        if 'properties' in feat and copy_properties:
            for key, val in list(feat['properties'].items()):
                feature_stats[key] = val

        results.append(feature_stats)
        entity_images.append(img)
    return results, entity_images
