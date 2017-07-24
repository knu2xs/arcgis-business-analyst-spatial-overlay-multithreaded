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
import os, sys
import arcpy
import multiprocessing
from functools import partial

# import the BA toolbox...it is not automatically loaded
ba_toolbox_path = r'C:\Program Files (x86)\ArcGIS\Desktop10.5\Business Analyst\ArcToolbox\Toolboxes\Business Analyst Tools.tbx'
arcpy.ImportToolbox(ba_toolbox_path)

# ensure BA extension is checked out
arcpy.CheckExtension('Business')


def doWork(BDSLayer, DriveTimesInput, oid):
    """
        Title:
            doWork

        Description:
            This is the function that gets called as does the work. The parameter oid comes from the idList when the
            function is mapped by pool.map(func,idList) in the multi function.

            Note that this function does not try to write to arcpy.AddMessage() as nothing is ever displayed.

            If the Overlay succeeds then it returns TRUE else FALSE.
    """
    # define the attributes to use for spatial overlay...enrichment variables
    DataToAppend = "TOTPOP_CY;HHPOP_CY"

    # Each overlay layer needs a unique name, so use oid
    drive_time_layer = arcpy.MakeFeatureLayer_management(DriveTimesInput, "DriveTimesInput_{}".format(oid))[0]

    # Select the polygon in the layer, this means the clip tool will use only that polygon
    df = arcpy.AddFieldDelimiters(
        datasource=DriveTimesInput,
        field=arcpy.Describe(DriveTimesInput).OIDFieldName
    )
    query = "{} = {}".format(df, oid)
    arcpy.SelectLayerByAttribute_management(drive_time_layer, "NEW_SELECTION", query)

    # Do the SpatialOverlay
    outFC = os.path.join(arcpy.env.scratchGDB, 'clip_{}'.format(oid))
    output = arcpy.SpatialOverlay_ba(
        InputFeatureLayer=BDSLayer,
        OverlayLayer=drive_time_layer,
        SelectedSummarizations=DataToAppend,
        OutputFeatureClass=outFC,
        SpatialOverlayAppendData=False,  # cannot append, as it will throw an error after the first run
        UseSelectedFeatures=True  # obviously want to limit analysis to speed up processing
    )[0]

    # take out the trash
    arcpy.Delete_management(drive_time_layer)

    # return the path to this output
    return output


def spatialoverlay(BDSLayer, DriveTimesInput):
    try:
        arcpy.env.overwriteOutput = True

        # Create a list of object IDs for DriveTimesInput polygons
        arcpy.AddMessage("Creating Polygon OID list...")
        descObj = arcpy.Describe(DriveTimesInput)
        field = descObj.OIDFieldName
        idList = []
        with arcpy.da.SearchCursor(DriveTimesInput, [field]) as cursor:
            for row in cursor:
                id = row[0]
                idList.append(id)
        arcpy.AddMessage("There are " + str(len(idList)) + " object IDs (polygons) to process.")

        # Call doWork function, this function is called as many OIDS in idList

        # This line creates a "pointer" to the real function but its a nifty way for declaring parameters.
        # Note the layer objects are passing their full path as layer objects cannot be pickled
        func = partial(doWork, BDSLayer.dataSource, DriveTimesInput.dataSource)

        arcpy.AddMessage("Sending to pool")
        # declare number of cores to use, use 1 less than the max
        pool_count = multiprocessing.cpu_count() - 1

        # Create the pool object
        pool = multiprocessing.Pool(processes=pool_count)

        # Fire off list to worker function.
        # res is a list that is created with what ever the worker function is returning
        res = pool.map(func, idList)
        pool.close()
        pool.join()

        # If an error has occurred report it
        if False in res:
            arcpy.AddError("A worker failed!")
        arcpy.AddMessage("Finished multiprocessing!")
    except arcpy.ExecuteError:
        # Geoprocessor threw an error
        arcpy.AddError(arcpy.GetMessages(2))
    except Exception as e:
        # Capture all other errors
        arcpy.AddError(str(e))