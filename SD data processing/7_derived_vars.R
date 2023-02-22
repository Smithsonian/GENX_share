# Create derived variables in data
# Written by Selina Cheng, last modified 22 February 2023
# ----------------------------------------------------------------------------------------------------
# Load libraries and create functions 
if (!require("pacman")) install.packages("pacman")
pacman::p_load(lubridate, data.table, plyr, tidyverse, zoo, suncalc)

mean_rm <- function(x){ mean(x, na.rm = TRUE) } # mean function with na.rm = TRUE
# ----------------------------------------------------------------------------------------------------
# Set source and output directories
source_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/6_loggernet_merge")

output_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/7_derived_vars")

# Read in data
dt <- fread(file.path(source_dir, "GENX_SD_Export_Waterlevel_2021-05-06_to_2022-12-30.csv"))
# ----------------------------------------------------------------------------------------------------
# Create time variables for yday, year, month, and week
dt[, yday := yday(time2)]
dt[, year := year(time2)]
dt[, month := month(time2)]
dt[, week := week(time2)]
dt[,date := date(time2)]

# Lat and long of GENX
lat <- 38.874767
long <- -76.549773
  
# Get sunrise/sunset times
dt_dates <- unique(dt$date)
dt_dates <- getSunlightTimes(dt_dates, lat, long, keep = c("solarNoon", "sunrise", "sunset"), tz = "America/Jamaica")

# Merge sunset/sunrise with dt
setDT(dt_dates)
setkey(dt_dates, "date")
setkey(dt, "date")
dt2 <- dt[dt_dates]

# Create variable that tells you whether it's day or night
dt2[, day := ifelse(time2 < sunrise | time2 > sunset, "night", "day")]

# ---------------------------------------------------------------------------------------------------
# Clean btemps data
# This is based on visual assessment of the btemps -- noticing that there are erroneous data for these timestamps
dt2[, btemp_avg2 := btemp_avg]
dt2[, btemp_avg2 := ifelse(zone_name == "c0_amb" & date(time2) > mdy("11/22/2022"), NA, btemp_avg2)]
dt2[, btemp_avg2 := ifelse(zone_name == "c1_amb" & date(time2) > mdy("5/24/2022") & date(time2) < mdy("6/19/2022"), NA, btemp_avg2)]
dt2[, btemp_avg2 := ifelse(zone_name == "a7_e4.5" & date(time2) > mdy("7/1/2022") & date(time2) < mdy("8/5/2022"), NA, btemp_avg2)]
dt2[, btemp_avg2 := ifelse(zone_name == "c4_e2.25" & date(time2) > mdy("4/26/2022") & date(time2) < mdy("5/18/2022"), NA, btemp_avg2)]
dt2[, btemp_avg2 := ifelse(zone == "zone_7" & btemp_avg2 > 35, NA, btemp_avg2)]
# ---------------------------------------------------------------------------------------------------
# Create ambient averages, averaging chambers 1+2 for temp, therm10c, therm25c, therm10pin, therm90ext
dt2[chamber == 1 | chamber == 2, amb_temp := mean_rm(temp), by = time2]
dt2[, amb_temp := mean_rm(amb_temp), by = time2]

dt2[chamber == 1 | chamber == 2, amb_therm10c := mean_rm(therm10c), by = time2]
dt2[, amb_therm10c := mean_rm(amb_therm10c), by = time2]

dt2[chamber == 1 | chamber == 2, amb_therm10pin := mean_rm(therm10pin), by = time2]
dt2[, amb_therm10pin := mean_rm(amb_therm10pin), by = time2]

dt2[chamber == 1 | chamber == 2, amb_therm25c := mean_rm(therm25c), by = time2]
dt2[, amb_therm25c := mean_rm(amb_therm25c), by = time2]

dt2[chamber == 1 | chamber == 2, amb_therm90ext := mean_rm(therm90ext), by = time2]
dt2[, amb_therm90ext := mean_rm(amb_therm90ext), by = time2]

# Create ambient averages for btemps
# Create an average over zone 0
dt2[zone == "zone_0", amb_btemp_z0 := mean_rm(btemp_avg2), by = time2]
dt2[, amb_btemp_z0 := mean_rm(amb_btemp_z0), by = time2]

# Create an average over zones 0 and 1
dt2[zone == "zone_0" | zone == "zone_1", amb_btemp_z0_z1 := mean_rm(btemp_avg2), by = time2]
dt2[, amb_btemp_z0_z1 := mean_rm(amb_btemp_z0_z1), by = time2]

# Create a variable just for zone mean of btemps
dt2[, btemp_zone_avg := mean_rm(btemp_avg2), by = .(zone, time2)]

# -----------------------------------------------------------------------------------------------
# Create deltas
# plot variable - ambient averages
# Delta from ambient air temperature
dt2[, delta_amb_temp := (temp - amb_temp)]

# Delta from ambient therm10 chamber
dt2[, delta_amb_therm10c := (therm10c - amb_therm10c)]

# Delta from ambient therm10pin
dt2[, delta_amb_therm10pin := (therm10pin - amb_therm10pin)]

# Delta from ambient therm25 chamber
dt2[, delta_amb_therm25c := (therm25c - amb_therm25c)]

# Delta from therm90extension
dt2[, delta_amb_therm90ext := (therm90ext - amb_therm90ext)]

# Delta between plot therm10chamber and amb_pinb_z0
dt2[, delta_amb_btemp_therm10c := (therm10c - amb_btemp_z0)]

# Delta between plot therm25chamber and amb_pinb_z0
dt2[, delta_amb_btemp_therm25c := (therm25c - amb_btemp_z0)]

# Delta between individual plot btemp and the ambient zones
dt2[, delta_amb_btemp_z0 := (btemp_avg2 - amb_btemp_z0)]
dt2[, delta_amb_btemp_z0_z1 := (btemp_avg2 - amb_btemp_z0_z1)]

# Set all NANs to NAs
dt3 <- as.data.table(lapply(dt2, function(x) {gsub("NaN", NA, x, ignore.case = T)}))

# Write file
filename <- paste0("GENX_SD_Export_Waterlevel_", date(dt3$time2[1]), "_to_", date(dt3$time2[nrow(dt3)]), "_derived_clean_btemps.csv")
out_path <- file.path(output_dir, filename)

write.table(dt3, out_path, append = FALSE, quote = FALSE, sep = ",",
            na = "NA", dec = ".", row.names = FALSE,
            col.names = TRUE, qmethod = c("escape", "double"))

