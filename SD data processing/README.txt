Scripts in this folder:
0_process_sd_raw_execute: Using functions from "0_process_sd_raw_functions", reads in and runs preliminary cleaning for raw SD card data

0_process_sd_raw_functions: Functions to read in raw data

1_join_data: Joins older SD data with recent SD data

2_timefix: Fixes erroneous timestamps where the data jumps backwards in time

3_rollingSD_functions: Functions used to clean data before applying rolling standard deviation scripts; also contains functions used in rolling standard deviation cleaning scripts

3_rollingSD_pt1: Cleans noise in thermistor data using narrow rolling standard deviation windows

3b_fill_ardlog: Fills SD data with data from the ARDLOG table in LoggerNet

4_process_sd_15min: Aggregate filled SD data to 15 minute intervals

5_rollingSD_pt2: Apply more rigorous rolling standard deviation cleaning functions to all temperature data

6_join_loggernet: Join SD data with the Export and Waterlevel200 tables from LoggerNet. Also create a copy of the SD/Export/Waterlevel200 data joined with flux data

7_derived_vars: Create derived variables used for analysis


This folder also contains:
GENX_design: Contains variable names with design scale, new column names, and link to join with experimental design.
Also contains units and definitions.

GENX_plotnames: Experimental design with chambers mapped to heating treatment zones. 
"design_link" and "logger" columns will link up to each other in these sheets.