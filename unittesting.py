import unittest
import arcpy
from get_business_analyst_data_paths import get_usa_data_path
from os import path
import spatial_overlay_utilities

# reusable variables
bds_fc = path.join(get_usa_data_path(), r'Data\Demographic Data\BlockGroups_bg_esri.bds')
bds_layer = arcpy.MakeFeatureLayer_management(bds_fc)[0]
bds_layer_properties = "TOTPOP_CY;HHPOP_CY"
this_directory = path.dirname(__file__)
drive_times_input = path.join(this_directory, r'resources\Testing.gdb\ServiceAreas')


class TestCase(unittest.TestCase):

    def test_spatial_overlay(self):

        oid = [_[0] for _ in arcpy.da.SearchCursor(drive_times_input, 'OID@')][0]
        output_path = path.join(arcpy.env.scratchGDB, 'test_{}'.format(oid))
        oid_field_name = arcpy.Describe(bds_layer).OIDFieldName

        if arcpy.Exists(output_path):
            arcpy.Delete_management(output_path)

        result = spatial_overlay_utilities.spatial_overlay(
            bds_layer=bds_layer,
            target_feature_class=drive_times_input,
            overlay_attributes=bds_layer_properties,
            where_clause='{} = {}'.format(oid_field_name, oid)
        )

        self.assertTrue(arcpy.Exists(result))

    def test_spatial_overlay_multithreaded(self):

        output_path = path.join(arcpy.env.scratchGDB, 'test{}'.format(spatial_overlay_utilities._get_uid()))

        result = spatial_overlay_utilities.spaital_overlay_multithreaded(
            bds_layer=bds_layer,
            target_feature_class=drive_times_input,
            overlay_attributes=bds_layer_properties,
            output_feature_class=output_path
        )

        self.assertTrue(arcpy.Exists(result))


if __name__ == '__main__':
    unittest.main()
