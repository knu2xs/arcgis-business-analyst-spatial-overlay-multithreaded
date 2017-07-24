"""
    Title:
        SpatialOverlayToolMP

    Description:
        This simple script is the script that is wired up into toolbox
"""

import arcpy  
import SpatialOverlayWorkCode

# Get parameters (These are Layer objects)  
BDSLayer = arcpy.GetParameterAsText(0)  
DriveTimesInput = arcpy.GetParameterAsText(1)
DataToAppend = arcpy.GetParameterAsText(2)

def main():  
    arcpy.AddMessage("Calling code...")  
    SpatialOverlayWorkCode.spatialoverlay(BDSLayer,DriveTimesInput)

if __name__ == '__main__':  
    main()