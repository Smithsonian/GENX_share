# Fill gaps in SD data with ARDLOG table from LoggerNet
# Apply preliminary rolling standard deviation limitation functions to thermistor data again after filling with ARDLOG table
# This eliminates high amounts of noise in the thermistor data
# Written by Roy Rich and Selina Cheng, last modified 21 February 2023
# --------------------------------------------------
# Get functions
source("3_rollingSD_functions.R")

# Write a time ladder function where you can specify the increment
# This function adds in missing timestamps in the data
time_ladder <- function(dt_raw3, increment) {
  #create POSIXct object with sequence of 15 minute intervals in the data (900 s)
  time2 <- seq.POSIXt(min(dt_raw3$time2, na.rm = TRUE), max(dt_raw3$time2, na.rm = TRUE), by = increment) 
  dt3 <- as.data.frame(time2)
  dt3$time2 <- as.POSIXct(dt3$time2, tz = "Etc/GMT")
  # only use gap time - get the vales in the ideal sequence that aren't in the df - these can then be flagged as missing data.
  dt4 <- anti_join(dt3, dt_raw3, copy = TRUE, suffix = c(".x", ".y")) 
  # make raw data with time ladder incorporated
  dt_raw3 <- full_join(dt_raw3, dt4, copy = TRUE, suffix = c(".x", ".y")) 
  #check on number of missing obs - this could be built into a QC check/test
  print(paste("There were", nrow(dt4), "missing observations in the raw data."))
  return(dt_raw3)
}
# -------------------------------------------------------------------
# Set paths for source directory and output directory
# Starting with SD data that has already been range limited and had narrow SD limitations applied to thermistors
source_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/3_rollSD_pt1")

output_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/3b_filled_ardlog")

ardlog_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_loggernet/GENX ARD_LOG")

# ------------------------------------------------------------------
# Get file names
i <- list.files(source_dir, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)

for(n in 1:length(i)){
  # Read in SD file
  dt <- fread(i[n])
  
  # Aggregate SD data (currently 10-second data) to 1-min timestamps -------------------------------------------------
  # Set timestamp as POSIX object
  dt$new_timestamp <- as.POSIXct(dt$new_timestamp, format = "%Y-%m-%d %H:%M:%S", tz ="Etc/GMT")
  
  # Now aggregate data to 1 minute interval
  rounded_seconds <- round(second(dt$new_timestamp)/60)*60
  dt$time2 <- update(dt$new_timestamp, sec = rounded_seconds)
  
  # Set constant for sl_id and chamber
  dt$sl_id <- unique(dt$sl_id)[1]
  dt$chamber <- unique(dt$chamber)[1]
  
  # Aggregate sd data to 1 minute data
  group <- c("sl_id", "chamber", "time2")
  
  # This group contains variables that we want to average together
  use <- c("co2", "co2ave_1", "co2_2", "temp", "pressure", "humidity", "voc", "new_therm_10_chamber", "new_therm_25_chamber",
           "new_therm_10_pin", "new_therm_90_ext", "cs_code")
  
  # First subset the data by these variables (discard all others)
  subgroup <- c(group, use)
  dt2 <- subset(dt, select = subgroup)
  dt2 <- unique(dt2)
  
  # Apply mean_rm function by group (which is organized by 1 min intervals)
  dt_sd <- dt2[,lapply(.SD, mean_rm), by = group]
  
  # Turn all NANs into NAs
  dt_sd <- as.data.table(lapply(dt_sd, function(x) {gsub("NaN", NA, x, ignore.case = T)}))
  dt_sd$time2 <- as.POSIXct(dt_sd$time2, format = "%Y-%m-%d %H:%M:%S", tz ="Etc/GMT")

  # Order by timestamp
  dt_sd <- dt_sd[order(dt_sd$time2),]
  rm(dt2)
  rm(dt)
  
  # Get ARDLOG file ------------------------------------------------------------
  # Get chamber number of SD data
  chamber <- unique(dt_sd$chamber)[1]
  
  # Get ARDLOG file
  ard_file <- list.files(ardlog_dir, pattern = paste0("Ch", chamber, "_"), all.files = FALSE,
                         full.names = TRUE, recursive = TRUE,
                         ignore.case = FALSE)
  
  # If there's more than one ARDLOG file for the chamber, bind together
  if(length(ard_file) > 1){
    dt_ard <- rbind(fread(ard_file[1]), fread(ard_file[2]))
  } else{
    dt_ard <- fread(ard_file)
  }
  
  # Keep only these columns
  keep <- c("site", "zone_treatment", "zone_name", "transect", "zone", "plot_id", "chamber", "distance", "chamber_treatment",
            "miu_valve", "chamber_name", "logger", "time2", "ptemp_c", "co2", "co2ave1", "bmetemp", "bmepressure", "bmerh", 
            "bmevoc", "therm10c", "therm25c", "therm10pin", "therm90ext", "therm10amb")
  dt_ard <- subset(dt_ard, select = keep)
  
  # Also give new names to ARDLOG and SD data so there are no duplicates
  setnames(dt_ard,
           c("co2", "co2ave1", "bmetemp", "bmepressure", "bmerh", 
             "bmevoc", "therm10c", "therm25c", "therm10pin", "therm90ext", "therm10amb"),
           c("co2_ard", "co2ave1_ard", "temp_ard", "pressure_ard", "humidity_ard", "voc_ard", "therm10c_ard", "therm25c_ard",
             "therm10pin_ard", "therm90ext_ard", "therm10amb_ard"))
  
  setnames(dt_sd, 
           c("co2", "co2ave_1", "co2_2", "temp", "pressure", "humidity", "voc", "new_therm_10_chamber", "new_therm_25_chamber", 
             "new_therm_10_pin", "new_therm_90_ext"), 
           c("co2_sd", "co2ave1_sd", "co22_sd", "temp_sd", "pressure_sd", "humidity_sd", "voc_sd", "therm10c_sd", "therm25c_sd",
             "therm10pin_sd", "therm90ext_sd"))
  
  # Now merge the two datasets by chamber and time2 -------------------------------------------------
  dt_ard$chamber <- as.character(dt_ard$chamber)
  
  # Set merge keys
  mergekey <- c("chamber", "time2")
  setDT(dt_sd)
  setDT(dt_ard)
  setkeyv(dt_sd, mergekey)
  setkeyv(dt_ard, mergekey)
  
  # Merge ARDLOG and SD data
  dt_merged <- merge(dt_sd, dt_ard, all = TRUE, by = mergekey)
  
  # Create columns for each of the variables that you want to fill with ardlog
  # If SD data is NA, fill with ARDLOG data. Otherwise, use SD data
  dt_merged[, co2_use := ifelse(is.na(co2_sd), co2_ard, co2_sd)]
  dt_merged[, co2ave1_use := ifelse(is.na(co2ave1_sd), co2ave1_ard, co2ave1_sd)]
  dt_merged[, temp_use := ifelse(is.na(temp_sd), temp_ard, temp_sd)]
  dt_merged[, pressure_use := ifelse(is.na(pressure_sd), pressure_ard, pressure_sd)]
  dt_merged[, humidity_use := ifelse(is.na(humidity_sd), humidity_ard, humidity_sd)]
  dt_merged[, voc_use := ifelse(is.na(voc_sd), voc_ard, voc_sd)]
  dt_merged[, therm10c_use := ifelse(is.na(therm10c_sd), therm10c_ard, therm10c_sd)]
  dt_merged[, therm25c_use := ifelse(is.na(therm25c_sd), therm25c_ard, therm25c_sd)]
  dt_merged[, therm10pin_use := ifelse(is.na(therm10pin_sd), therm10pin_ard, therm10pin_sd)]
  dt_merged[, therm90ext_use := ifelse(is.na(therm90ext_sd), therm90ext_ard, therm90ext_sd)]

  # Keep only the _use columns
  keep <- c("chamber", "sl_id", "time2", "co2_use", "co2ave1_use", "temp_use", "pressure_use", "humidity_use", 
            "voc_use","therm10c_use", "therm25c_use", "therm10pin_use", "therm90ext_use", "therm10amb_ard", "cs_code")
  
  dt_filled <- subset(dt_merged, select = keep)
  
  # Apply range limitations to new data -------------------------------------------------
  # Run range limitation functions
  # These are the columns that we want to apply range limitation functions on
  cols <- c("co2_use", "co2ave1_use", "temp_use", "pressure_use", "humidity_use", 
            "voc_use","therm10c_use", "therm25c_use", "therm10pin_use", "therm90ext_use", "therm10amb_ard", "cs_code")
  
  # Set all measured variables to numeric
  dt_filled[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
  
  # Apply different range limitations to different columns
  dt_filled[, co2_use := co2_replace(co2_use)]
  dt_filled[, co2ave1_use := co2_replace(co2ave1_use)]
  dt_filled[, temp_use := temp_replace(temp_use)]
  dt_filled[, pressure_use := press_replace(pressure_use)]
  dt_filled[, humidity_use := hum_replace(humidity_use)]
  dt_filled[, voc_use := voc_replace(voc_use)]
  dt_filled[, therm10c_use := therm_replace(therm10c_use)]
  dt_filled[, therm25c_use := therm_replace(therm25c_use)]
  dt_filled[, therm10pin_use := therm_replace(therm10pin_use)]
  dt_filled[, therm90ext_use := therm_replace(therm90ext_use)]
  dt_filled[, therm10amb_ard := therm_replace(therm10amb_ard)]
  
  # Add in timeladder for a 1-minute interval -----------------------------------------
  dt_full <- time_ladder(dt_filled, 60)
  dt_full <- dt_full[order(time2)]
  
  # Add yday and hour as variables
  dt_full[,yday := yday(time2)]
  dt_full[,hour := hour(time2)]
  
  key <-  c("yday","hour") 
  
  # Data cleaning functions, developed to be all the same for each variable
  # Therm10chamber ----------------------------------------------------------------------
  dt_full[, therm10c_new := therm10c_use] # Create new version of variable
  dt_full[, m_therm_10_chamber := mean_trim(therm10c_use), by = key] # Mean of chamber by day and hour
  dt_full[, m_therm_10_chamber_upper := m_therm_10_chamber + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_10_chamber_lower := m_therm_10_chamber - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$therm10c_new <- ifelse(dt_full$therm10c_new > dt_full$m_therm_10_chamber_upper, NA, dt_full$therm10c_new) 
  dt_full$therm10c_new <- ifelse(dt_full$therm10c_new < dt_full$m_therm_10_chamber_lower, NA, dt_full$therm10c_new)
  
  # Create rolling standard deviation over 10 rows -- narrow window
  dt_full[, therm_10_chamber_hr := frollapply(therm10c_use, 10, sd_rm, fill = NA, align = c("center")), ] 
  
  # if standard deviation is NOT NA, and stdev is greater than 95% quantile, remove data
  # Otherwise, if standard deviation is NA, NA the data
  dt_full$therm10c_new <- ifelse(!is.na(dt_full$therm_10_chamber_hr),
                                         ifelse(dt_full$therm_10_chamber_hr > quantile(dt_full$therm_10_chamber_hr, 0.95, na.rm = TRUE), NA, 
                                                dt_full$therm10c_new), NA) 
  
  # Therm25chamber ----------------------------------------------------------------------
  dt_full[, therm25c_new := therm25c_use] # Create new version of variable
  dt_full[, m_therm_25_chamber := mean_trim(therm25c_use), by = key] # Mean of chamber by day and hour
  dt_full[, m_therm_25_chamber_upper := m_therm_25_chamber + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_25_chamber_lower := m_therm_25_chamber - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$therm25c_new <- ifelse(dt_full$therm25c_new > dt_full$m_therm_25_chamber_upper, NA, dt_full$therm25c_new) 
  dt_full$therm25c_new <- ifelse(dt_full$therm25c_new < dt_full$m_therm_25_chamber_lower, NA, dt_full$therm25c_new)
  
  # Create rolling standard deviation over 10 rows -- narrow window
  dt_full[, therm_25_chamber_hr := frollapply(therm25c_use, 10, sd_rm, fill = NA, align = c("center")), ]
  
  # if standard deviation is NOT NA, and stdev is greater than 95% quantile, remove data
  # Otherwise, if standard deviation is NA, NA the data
  dt_full$therm25c_new <- ifelse(!is.na(dt_full$therm_25_chamber_hr),
                                         ifelse(dt_full$therm_25_chamber_hr > quantile(dt_full$therm_25_chamber_hr, 0.95, na.rm = TRUE), NA, 
                                                dt_full$therm25c_new), NA)
  
  # Therm10pin ------------------------------------------------------------------------------  
  dt_full[, therm10pin_new := therm10pin_use] # Create new version of variable
  dt_full[, m_therm_10_pin := mean_trim(therm10pin_use), by = key] # Mean of chamber by day and hour
  dt_full[, m_therm_10_pin_upper := m_therm_10_pin + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_10_pin_lower := m_therm_10_pin - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$therm10pin_new <- ifelse(dt_full$therm10pin_new > dt_full$m_therm_10_pin_upper, NA, dt_full$therm10pin_new)
  dt_full$therm10pin_new <- ifelse(dt_full$therm10pin_new < dt_full$m_therm_10_pin_lower, NA, dt_full$therm10pin_new)
  
  # Create rolling standard deviation over 10 rows -- narrow window
  dt_full[, therm_10_pin_hr := frollapply(therm10pin_use, 10, sd_rm, fill = NA, align = c("center")), ] 
  
  # if standard deviation is NOT NA, and stdev is greater than 95% quantile, remove data
  # Otherwise, if standard deviation is NA, NA the data
  dt_full$therm10pin_new <- ifelse(!is.na(dt_full$therm_10_pin_hr),
                                     ifelse(dt_full$therm_10_pin_hr > quantile(dt_full$therm_10_pin_hr, 0.95, na.rm = TRUE), NA, 
                                            dt_full$therm10pin_new), NA) 
  
  # Therm90ext ------------------------------------------------------------------------------  
  dt_full[, therm90ext_new := therm90ext_use] # Create new version of variable
  dt_full[, m_therm_90_ext := mean_trim(therm90ext_use), by = key] # Mean of chamber by day and hour
  dt_full[, m_therm_90_ext_upper := m_therm_90_ext + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_90_ext_lower := m_therm_90_ext - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$therm90ext_new <- ifelse(dt_full$therm90ext_new > dt_full$m_therm_90_ext_upper, NA, dt_full$therm90ext_new) 
  dt_full$therm90ext_new <- ifelse(dt_full$therm90ext_new < dt_full$m_therm_90_ext_lower, NA, dt_full$therm90ext_new)
  
  # Create rolling standard deviation over 10 rows -- narrow window
  dt_full[, therm_90_ext_hr := frollapply(therm90ext_use, 10, sd_rm, fill = NA, align = c("center")), ] 
  
  # if standard deviation is NOT NA, and stdev is greater than 95% quantile, remove data
  # Otherwise, if standard deviation is NA, NA the data
  dt_full$therm90ext_new <- ifelse(!is.na(dt_full$therm_90_ext_hr),
                                     ifelse(dt_full$therm_90_ext_hr > quantile(dt_full$therm_90_ext_hr, 0.95, na.rm = TRUE), NA, 
                                            dt_full$therm90ext_new), NA) 
  
  # Therm10amb ------------------------------------------------------------------------------  
  dt_full[, therm10amb_new := therm10amb_ard] # Create new version of variable
  dt_full[, m_therm10amb := mean_trim(therm10amb_ard), by = key] # Mean of chamber by day and hour
  dt_full[, m_therm_10amb_upper := m_therm10amb + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_10amb_lower := m_therm10amb - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$therm10amb_new <- ifelse(dt_full$therm10amb_new > dt_full$m_therm_10amb_upper, NA, dt_full$therm10amb_new) 
  dt_full$therm10amb_new <- ifelse(dt_full$therm10amb_new < dt_full$m_therm_10amb_lower, NA, dt_full$therm10amb_new)
  
  # Create rolling standard deviation over 10 rows -- narrow window
  dt_full[, therm_10amb_hr := frollapply(therm10amb_ard, 10, sd_rm, fill = NA, align = c("center")), ] 
  
  # if standard deviation is NOT NA, and stdev is greater than 95% quantile, remove data
  # Otherwise, if standard deviation is NA, NA the data
  dt_full$therm10amb_new <- ifelse(!is.na(dt_full$therm_10amb_hr),
                                   ifelse(dt_full$therm_10amb_hr > quantile(dt_full$therm_10amb_hr, 0.95, na.rm = TRUE), NA, 
                                          dt_full$therm10amb_new), NA) 
  # Save table ------------------------------------------------------------------------------  
  # Choose vars to keep
  keep <- c("chamber", "sl_id", "time2", "co2_use", "co2ave1_use", "temp_use", "pressure_use", "humidity_use",
            "voc_use", "therm10c_use", "therm25c_use", "therm10pin_use", "therm90ext_use", "therm10amb_ard", "cs_code",
            "therm10c_new", "therm25c_new", "therm10pin_new", "therm90ext_new", "therm10amb_new")
  dt_full <- subset(dt_full, select = keep)
  
  # Turn all NANs into NAs
  dt_full <- as.data.table(lapply(dt_full, function(x) {gsub("NaN", NA, x, ignore.case = T)}))
  
  # Order by timestamp again
  dt_full <- dt_full[order(time2)]
  
  # Save table
  filename <- paste0("GENX_Arduino_SD_Ch", dt_full$chamber[1], "_", date(dt_full$time2[1]), "_to_",
                     date(dt_full$time2[nrow(dt_full)]), "_filled.csv")
  out_path <- file.path(output_dir, filename)
  
  write.table(dt_full, out_path, append = FALSE, quote = FALSE, sep = ",",
              na = "NA", dec = ".", row.names = FALSE,
              col.names = TRUE, qmethod = c("escape", "double"))
  
}













