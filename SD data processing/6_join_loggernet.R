# Join 15 minute cleaned SD data with LoggerNet data (Export and Waterlevel200 tables)
# Code by Roy Rich and Selina Cheng
# Last modified by Selina Cheng on 22 February 2023

# Load packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(lubridate, data.table, plyr, tidyverse, zoo)
#  ---------------------------------------------------------------
# Set source and output tables
source_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/5_rollSD_15min")
output_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/6_loggernet_merge")
loggernet_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                        "/Dropbox (Smithsonian)/GCREW_RESEARCHER_DATA")

# Read in plotnames table to add experimental design to data
plotnames <- fread(paste0("C:/Users/", Sys.getenv("USERNAME"),
                          "/Dropbox (Smithsonian)/GCREW_LOGGERNET_WORKFLOW/design documents/GENX_plotnames.csv"))

# Modify plotnames table so it's suitable for joining with data
plotnames <- plotnames[,c(2:11)]
plotnames <- unique(plotnames)

# ------------------------------------------------------------------
# Bind all chambers together 
# Get file names
i <- list.files(source_dir, pattern = "15min_clean", all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)

# Bind all the data together
dt_genx <- fread(i[1])

for(m in 2:length(i)){
  dt2 <- fread(i[m])
  dt_genx <- rbind.fill(dt_genx, dt2)
}

# ----------------------------------------------------------
# Read in thermistor data that was visually cleaned in JMP 16
dt_cleantherms <- fread(file.path(source_dir, "GENX_rollSD_15min_LONG_VisualClean_SC.csv"))

# Cast data to wide form
dt_cleantherms_wide <- dcast(dt_cleantherms, sl_id+chamber+time3 ~ variable_name, subset = NULL, 
                             drop = TRUE, value.var = "value")

# Format timestamps
dt_cleantherms_wide$old_time3 <- dt_cleantherms_wide$time3
dt_cleantherms_wide$time3 <- as.POSIXct(dt_cleantherms_wide$old_time3, format = "%Y/%m/%d %I:%M:%S %p", tz = "Etc/GMT")

# Join clean thermistor data with original data
setDT(dt_cleantherms_wide)
setDT(dt_genx)

mergekey <- c("sl_id", "chamber", "time3")
setkeyv(dt_cleantherms_wide, mergekey)
setkeyv(dt_genx, mergekey)

dt_genx_clean <- merge(dt_genx, dt_cleantherms_wide, by = mergekey)

# Keep only the variables we want
keep <- c("sl_id", "chamber", "time3", "co2_use", "co2ave1_use", "temp", "pressure_use", "new_humidity", "voc_use", 
          "therm10c", "therm10pin", "therm25c", "therm90ext", "cs_code")

dt_genx_clean2 <- subset(dt_genx_clean, select = keep)

# Assign new column names
newnames <- c("sl_id", "chamber", "time2", "co2", "co2ave1", "temp", "pressure", "humidity", "voc", "therm10c",
              "therm10pin", "therm25c", "therm90ext", "cs_code")

setnames(dt_genx_clean2, keep, newnames)

# Sum # of NA columns to see which rows have no data
num_nas <- rowSums(is.na(dt_genx_clean2[,c(4:14)]))
dt_genx_clean2[, num_nas := .(num_nas)]

# Get rid of rows with no data at all
dt_genx_clean3 <- dt_genx_clean2[num_nas != 11]

# Keep only these columns
keep <- c("sl_id", "chamber", "time2", "co2", "co2ave1", "temp", "pressure", "humidity", "voc", "therm10c",
              "therm10pin", "therm25c", "therm90ext")
dt_genx_clean3 <- subset(dt_genx_clean3, select = keep)

# Join genx data with experimental design ("plotnames")
dt_genx_clean4 <- merge(dt_genx_clean3, plotnames, all.x = T, all.y = F, by = "chamber")

# GENX SD data is ready to join with export and waterlevel data now
# ----------------------------------------------------------------------------
# Read in export data
export_dir <- file.path(loggernet_dir, "gcrew_genx", "yearly")
export_dt <- fread(file.path(export_dir, "genx_export_2022.csv"))

# Merge export and dt_genx_clean3
mergekey <- c("time2", "chamber", "zone_treatment", "zone_name", "transect", "zone", "plot_id", "distance", "chamber_treatment",
              "miu_valve", "chamber_name")
setkeyv(dt_genx_clean4, mergekey)
setkeyv(export_dt, mergekey)

# There is duplicate data in export_dt from like Feb 7 to Feb 9 2022 that is running different programs but has the same data
# Remove the program column
export_dt[, program:=NULL]
export_dt <- unique(export_dt)

# Merge tables
dt_merged <- merge(dt_genx_clean4, export_dt, all.x = T, all.y = T, by = mergekey)

# ----------------------------------------------------------------------------
# Read in waterlevel data
water_dir <- file.path(loggernet_dir, "gcrew_waterlevel", "yearly", "combined")
water_dt <- fread(file.path(water_dir, "waterlevel_combined_2021-04-13_to_2022-12-28.csv"))

# There are also duplicates in the water level table
water_dt <- unique(water_dt)

# Set logger and site for all dt_merged
dt_merged$logger <- "genx"
dt_merged$site <- "genx"

# Merge (GENX SD + Export) with the waterlevel table
mergekey <- "time2"
setkeyv(dt_merged, mergekey)
setDT(water_dt)
setkeyv(water_dt, mergekey)

dt_merged2 <- merge(dt_merged, water_dt, all.x = T, all.y = F, by = mergekey,
                    suffixes = c("_export", "_waterlevel"))

# Save dt_merged2
# Order by timestamp
dt_merged2 <- dt_merged2[order(time2)]

# Create filename and write file
filename <- paste0("GENX_SD_Export_Waterlevel_", date(dt_merged2$time2[1]), "_to_", date(dt_merged2$time2[nrow(dt_merged2)]), ".csv")
out_path <- file.path(output_dir, filename)

write.table(dt_merged2, out_path, append = FALSE, quote = FALSE, sep = ",",
            na = "NA", dec = ".", row.names = FALSE,
            col.names = TRUE, qmethod = c("escape", "double"))

# ----------------------------------------------------------------------------
# Run this separately 
# Create a flux dataset with derived variables, where you start with flux data and the GENX SD+Export+Waterlevel derived variable data 
# Read in derived + merged GENX data (full dataset!)
derived_vars <- "C:/Users/ChengS1/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/7_derived_vars/GENX_SD_Export_Waterlevel_2021-05-06_to_2022-12-30_derived_clean_btemps.csv"
dt_derived <- fread(derived_vars)

# Read in flux files
flux_dir <- file.path("C:", "Users", Sys.getenv("USERNAME"), "Dropbox (Smithsonian)",
                      "GenX", "Data", "flux data", "Cleaned data", "fifteen_min_data")

f <- list.files(flux_dir, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)

# Bind all the data together
dt_flux <- fread(f[1])

for(k in 2:length(f)){
  dt2 <- fread(f[k])
  dt_flux <- rbind.fill(dt_flux, dt2)
}

# Change flux names
setnames(dt_flux, c("TIMESTAMP", "timestamp_15min"), c("flux_timestamp", "time2"))

# Checks
dt_flux$id <- paste0(dt_flux$time2, "_", dt_flux$chamber)

# There are also duplicates in the flux data?
dt_flux <- unique(dt_flux)

# Remove id col
dt_flux <- select(dt_flux, -id)

# Join with plot data
dt_flux <- merge(dt_flux, plotnames, all.x = T, all.y = F, by = "chamber")

# Combine derived vars with flux data
mergekey <- c("chamber", "time2", "zone_treatment", "zone_name", "transect", "zone", "plot_id", "distance",
              "chamber_treatment", "miu_valve", "chamber_name")

setkeyv(dt_derived, mergekey)
setDT(dt_flux)
setkeyv(dt_flux, mergekey)

flux_merge <- merge(dt_flux, dt_derived, all.x = T, all.y = F, by = mergekey)

# Save table
filename <- paste0("GENX_Flux_SD_Loggernet_", min(date(flux_merge$time2)), "_to_", max(date(flux_merge$time2)), "_derived.csv")
output_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/7_derived_vars")
out_path <- file.path(output_dir, filename)

write.table(flux_merge, out_path, append = FALSE, quote = FALSE, sep = ",",
            na = "NA", dec = ".", row.names = FALSE,
            col.names = TRUE, qmethod = c("escape", "double"))

