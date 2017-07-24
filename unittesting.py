import unittest
import arcpy
from get_business_analyst_data_paths import get_usa_data_path
from os import path
from SpatialOverlayWorkCode import *

# reusable variables
bds_fc = path.join(get_usa_data_path(), r'Data\Demographic Data\BlockGroups_bg_esri.bds')
bds_layer = arcpy.MakeFeatureLayer_management(bds_fc)[0]
drive_times_input = r'D:\spatialData\dataStoryConsulting\resources\Testing.gdb\ServiceAreas'

class TestCase(unittest.TestCase):

    oid = [_[0] for _ in arcpy.da.SearchCursor(drive_times_input, 'OID@')][0]

    def test_doWork(self):
        output_path = path.join(arcpy.env.scratchGDB, 'clip_{}'.format(self.oid))
        if arcpy.Exists(output_path):
            arcpy.Delete_management(output_path)
        result = doWork(
            BDSLayer=bds_layer,
            DriveTimesInput=drive_times_input,
            oid=self.oid
        )
        exists = arcpy.Exists(result)
        self.assertTrue(exists)

    def test_spatialOverlay(self):



if __name__ == '__main__':
    unittest.main()
