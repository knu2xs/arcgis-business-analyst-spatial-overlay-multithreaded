# ArcGIS Business Analyst Spatial Overlay - Multithreaded
 
For those of us using the ArcGIS Desktop Business Analyst extension, one of the most powerful, and frequently used tools, is the Spatial Overlay tool. This tool is somewhat computationally expensive, and has the potential to greatly benefit from parallel processing using the Python Multithreading module - exactly the reason for this repository's existence.

## Use

It is not very complicated. There is one toolbox accessible from within ArcMap. This toolbox calls the script after populating four parameters mirroring the normal parameters for the Spatial Overlay tool. 