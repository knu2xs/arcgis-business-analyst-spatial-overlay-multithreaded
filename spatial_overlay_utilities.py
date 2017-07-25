"""
DOB: 24 Jul 2017
Purpose: Performing overlay analysis (geoenrichment) locally takes a long time. This script attempts to speed
    this up every so slightly by taking advantage of multithreading.
Author: Joel McCune - http://github.com/knu2xs
"""
import os
import arcpy
import multiprocessing
from functools import partial
import math
from uuid import uuid4

# import the BA toolbox...it is not automatically loaded
ba_toolbox_path = r'C:\Program Files (x86)\ArcGIS\Desktop10.5\Business Analyst\ArcToolbox\Toolboxes\Business Analyst Tools.tbx'
arcpy.ImportToolbox(ba_toolbox_path)

# ensure BA extension is checked out
arcpy.CheckExtension('Business')


def _get_uid():
    """
    Cough out a UID without dashes
    :return: UID string without spaces
    """
    return str(uuid4()).replace('-', '')


def spatial_overlay(bds_layer, target_feature_class, overlay_attributes, where_clause=None):
    """
    Perform spatial overlay on subset of data identified using a SQL where clause.
    :param bds_layer: Input BDS layer used as the source for overlay.
    :param target_feature_class: Target polygons to which the overlay attributes will be apportioned to.
    :param overlay_attributes: Attributes to be apportioned to the target feature class.
    :param where_clause: SQL where clause to identify the subset of data from the target feature class being
        apportioned.
    :return: String path to newly created output feature class.
    """
    # Each overlay layer needs a unique name, so use oid
    target_layer = arcpy.MakeFeatureLayer_management(target_feature_class, "overlay{}".format(_get_uid()))[0]

    # if a where clause is specified, use this to limit the processing
    if where_clause:
        arcpy.SelectLayerByAttribute_management(target_layer, "NEW_SELECTION", where_clause)

    # perform spatial overlay
    output_feature_class = os.path.join(arcpy.env.scratchGDB, 'temp{}'.format(_get_uid()))
    output = arcpy.SpatialOverlay_ba(
        InputFeatureLayer=bds_layer,
        OverlayLayer=target_layer,
        SelectedSummarizations=overlay_attributes,
        OutputFeatureClass=output_feature_class,
        SpatialOverlayAppendData=False,  # cannot append, as it will throw an error after the first run
        UseSelectedFeatures=True  # obviously want to limit analysis to speed up processing...this is the point
    )[0]

    # take out the trash
    arcpy.Delete_management(target_layer)

    # return the path to this output
    return output


def spaital_overlay_multithreaded(bds_layer, target_feature_class, overlay_attributes, output_feature_class):
    """
    Perform spatial overlay (apportionment) to the new target feature class using multiple cores via the multithreading
        module.
    :param bds_layer: Input BDS layer used as the source for overlay.
    :param target_feature_class: Target polygons to which the overlay attributes will be apportioned to.
    :param overlay_attributes: Attributes to be apportioned to the target feature class.
    :param output_feature_class:
    :return:
    """
    # utilize one less thread than the maximum available
    thread_count = multiprocessing.cpu_count() - 1

    # calculate how many features should be in each chunk
    chunk_size = int(math.ceil(float(arcpy.GetCount_management(target_feature_class)[0]) / thread_count))

    # get the name of the OID field
    oid_field = arcpy.Describe(target_feature_class).OIDFieldName

    # get a list of all OID values
    oid_list = [_[0] for _ in arcpy.da.SearchCursor(target_feature_class, oid_field)]

    # split the OID list into chunked sublists
    chunk_list = [oid_list[x:x + chunk_size] for x in xrange(0, len(oid_list), chunk_size)]

    # iterate the list of chunks and assemble a complete sql query to select the features in the chunk
    chunk_sql_list = [
        ' OR '.join(['{0} = {1}'.format(oid_field, oid) for oid in oid_chunk])
        for oid_chunk in chunk_list
    ]

    # ensure data paths are being used, since multiprocessing has trouble with layer objects
    bds_path = arcpy.Describe(bds_layer).catalogPath
    target_path = arcpy.Describe(target_feature_class).catalogPath

    # use partial to set the constant arguments
    partial_spatial_overlay = partial(spatial_overlay, bds_path, target_path, overlay_attributes)

    # create pool object instance
    pool = multiprocessing.Pool(processes=thread_count)

    # get the collective result by mapping the selections via the partial object for performing the spatial join
    multithreading_feature_class_list = pool.map(partial_spatial_overlay, chunk_sql_list)

    # close and join the pool process
    pool.close()
    pool.join()

    # now, union the results back into a single feature class
    return arcpy.Merge_management(multithreading_feature_class_list, output_feature_class)[0]
