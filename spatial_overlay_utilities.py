"""
    Title:
        multicode

    Description:
        The module that does multicore Spatial Overlay.

    Limitations:
        This code expects the folder c:\temp\tc to exist, this is where the output ends up. As geoprocessing objects
        cannot be "pickled" the full path to the dataset is passed to the worker function. This means that any selection
        on the input DriveTimesInput layer is ignored.

    Author:
        Duncan Hornby (ddh@geodata.soton.ac.uk)

    Created:
        2/4/15
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


def spatial_overlay(bds_layer, polygons_receiving_data, bds_layer_properties, where_clause=None):
    """
        Title:
            spatial_overlay

        Description:
            This is the function that gets called as does the work. The parameter oid comes from the idList when the
            function is mapped by pool.map(func,idList) in the multi function.

            Note that this function does not try to write to arcpy.AddMessage() as nothing is ever displayed.

            If the Overlay succeeds then it returns TRUE else FALSE.
    """
    # Each overlay layer needs a unique name, so use oid
    target_layer = arcpy.MakeFeatureLayer_management(polygons_receiving_data, "DriveTimesInput_{}".format(_get_uid()))[0]

    # if a where clause is specified, use this to limit the processing
    if where_clause:
        arcpy.SelectLayerByAttribute_management(target_layer, "NEW_SELECTION", where_clause)

    # perform spatial overlay
    output_feature_class = os.path.join(arcpy.env.scratchGDB, 'temp{}'.format(_get_uid()))
    output = arcpy.SpatialOverlay_ba(
        InputFeatureLayer=bds_layer,
        OverlayLayer=target_layer,
        SelectedSummarizations=bds_layer_properties,
        OutputFeatureClass=output_feature_class,
        SpatialOverlayAppendData=False,  # cannot append, as it will throw an error after the first run
        UseSelectedFeatures=True  # obviously want to limit analysis to speed up processing...this is the point
    )[0]

    # take out the trash
    arcpy.Delete_management(target_layer)

    # return the path to this output
    return output


def spaital_overlay_multithreaded(bds_layer, polygons_receiving_data, bds_layer_properties):
    """

    :param bds_layer:
    :param polygons_receiving_data:
    :return:
    """
    # utilize one less thread than the maximum available
    thread_count = multiprocessing.cpu_count() - 1

    # calculate how many features should be in each chunk
    chunk_size = int(math.ceil(float(arcpy.GetCount_management(polygons_receiving_data)[0]) / thread_count))

    # get the name of the OID field
    oid_field = arcpy.Describe(polygons_receiving_data).OIDFieldName

    # get a list of all OID values
    oid_list = [_[0] for _ in arcpy.da.SearchCursor(polygons_receiving_data, oid_field)]

    # split the OID list into chunked sublists
    chunk_list = [oid_list[x:x + chunk_size] for x in xrange(0, len(oid_list), chunk_size)]

    # iterate the list of chunks and assemble a complete sql query to select the features in the chunk
    chunk_sql_list = [
        ' OR '.join(['{0} = {1}'.format(oid_field, oid) for oid in oid_chunk])
        for oid_chunk in chunk_list
    ]

    # ensure data paths are being used, since multiprocessing has trouble with layer objects
    bds_path = arcpy.Describe(bds_layer).catalogPath
    target_path = arcpy.Describe(polygons_receiving_data).catalogPath

    # use partial to set the constant arguments
    partial_spatial_overlay = partial(spatial_overlay, bds_path, target_path, bds_layer_properties)

    # create pool object instance
    pool = multiprocessing.Pool(processes=thread_count)

    # get the collective result by mapping the selections via the partial object for performing the spatial join
    result = pool.map(partial_spatial_overlay, chunk_sql_list)

    # close and join the pool process
    pool.close()
    pool.join()

    # now, union the results back into a single feature class
    