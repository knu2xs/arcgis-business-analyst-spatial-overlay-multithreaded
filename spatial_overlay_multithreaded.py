"""
DOB: 24 Jul 2017
Purpose: Very simple, wrapper for multithreaded spatial overlay.
Author: Joel McCune - http://github.com/knu2xs
"""
import spatial_overlay_utilities
from arcpy import GetParameterAsText

# Get parameters (These are Layer objects)  
bds_layer = GetParameterAsText(0)
target_polygons = GetParameterAsText(1)
overlay_attributes = GetParameterAsText(2)
output_feature_class = GetParameterAsText(3)

if __name__ == '__main__':
    spatial_overlay_utilities.spaital_overlay_multithreaded(
        bds_layer=bds_layer, 
        target_feature_class=target_polygons,
        overlay_attributes=overlay_attributes,
        output_feature_class=output_feature_class
    )
