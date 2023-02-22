# Clean 15 min GENX SD data and apply rolling standard deviations
# Written by Selina Cheng and Roy Rich, last modified 21 February 2023
# --------------------------------------------------
# Get functions
source("3_rollingSD_functions.R")

# Write a time ladder function where you can specify the increment
# This function adds in missing timestamps in the data
time_ladder <- function(dt_raw3, increment) {
  #create POSIXct object with sequence of 15 minute intervals in the data (900 s)
  time3 <- seq.POSIXt(min(dt_raw3$time3, na.rm = TRUE), max(dt_raw3$time3, na.rm = TRUE), by = increment) 
  dt3 <- as.data.frame(time3)
  dt3$time3 <- as.POSIXct(dt3$time3, tz = "Etc/GMT")
  # only use gap time - get the vales in the ideal sequence that aren't in the df - these can then be flagged as missing data.
  dt4 <- anti_join(dt3, dt_raw3, copy = TRUE, suffix = c(".x", ".y")) 
  # make raw data with time ladder incorporated
  dt_raw3 <- full_join(dt_raw3, dt4, copy = TRUE, suffix = c(".x", ".y")) 
  #check on number of missing obs - this could be built into a QC check/test
  print(paste("There were", nrow(dt4), "missing observations in the raw data."))
  return(dt_raw3)
}
# ----------------------------------------------------------------------------------------------
# Set paths for source directory and output directory
source_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/4_fifteen_min")

output_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/5_rollSD_15min")

# ----------------------------------------------------------------------------------------------
# STEP 1 Purpose: Therm90ext has some outliers that are troublesome.
# Bind all chambers together and get the mean for therm90ext across all chambers for each timestamp.
# Get file names
i <- list.files(source_dir, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)

# Bind all the different chamber data together 
  dt_genx <- fread(i[1])
  
  for(m in 2:length(i)){
    dt2 <- fread(i[m])
    dt_genx <- rbind.fill(dt_genx, dt2)
  }
  
  setDT(dt_genx)
  setkey(dt_genx, "time3")

  # Add in timeladder
  dt_genx_full <- time_ladder(dt_genx, 900)
  # Order by time3
  dt_genx_full <- dt_genx_full[order(time3)]
  
# First take mean of all chambers by time3
  dt_genx_full[, all_mean_therm90ext := mean_trim_all(therm90ext_new), by = time3] # Mean by time3 (15 min interval)
  
# Now isolate them and apply a one-hour rolling mean 
  rolling_mean <- unique(dt_genx_full[, all_mean_therm90ext, by = time3])
  rolling_mean[, roll_mean_therm90ext := frollapply(all_mean_therm90ext, 4, mean_trim_all, fill = NA, align = c("center")),]
  
# Now merge new rolling means with genx_full
  setDT(rolling_mean)
  setDT(dt_genx_full)
  
  dt_genx_all <- merge(dt_genx_full, rolling_mean[, c("time3", "roll_mean_therm90ext")], by = "time3")
  
# ----------------------------------------------------------------------------------------------
# STEP 2: Clean SD data using rolling standard deviations
  # Reads in data chamber by chamber

for(n in 1:length(i)){
  # Read in data
  dt <- fread(i[n])
  
  # Add in timeladder
  dt_full <- time_ladder(dt, 900)
  # Order by time
  dt_full <- dt_full[order(time3)]
  
  # Set constant sl_id and chamber
  dt_full$sl_id <- unique(dt_full$sl_id)[1]
  dt_full$chamber <- unique(dt_full$chamber)[1]
  
  # create derived variables for cleaning
  dt_full[,yday := yday(time3)]
  dt_full[,hour := hour(time3)]
  
  # Add in rolling mean therm90ext data from Step 1 (join)
  key1 <- c("sl_id", "chamber", "time3")
  setDT(dt_genx_all)
  
  setkeyv(dt_genx_all, key1)
  setkeyv(dt_full, key1)
  
  dt_full <- merge(dt_full, dt_genx_all[, c("sl_id", "chamber", "time3", "roll_mean_therm90ext")], by = key1)
  
  # Creating key for grouping data
  key2 <-  c("yday","hour") 
  
  # Data cleaning functions, developed to be all the same for each variable
  # Air Temp -- 10 degrees for mean upper and lower bounds----------------------------------------------------------
  dt_full[, new_temp := temp_use] # Create new version of variable
  dt_full[, m_temp := mean_trim(temp_use), by = key2] # Mean of chamber by day and hour
  dt_full[, m_temp_upper := m_temp + 10,] # mean +/- 10 degrees
  dt_full[, m_temp_lower := m_temp - 10,]
  
  # Remove values that are more than 10C away from the mean
  dt_full$new_temp <- ifelse(dt_full$new_temp > dt_full$m_temp_upper, NA, dt_full$new_temp)
  dt_full$new_temp <- ifelse(dt_full$new_temp < dt_full$m_temp_lower, NA, dt_full$new_temp)
  
  # Create rolling standard deviations
  dt_full[, temp_hr := frollapply(temp_use, 4, sd_rm, fill = NA, align = c("center")), ] #moving window sd for 1 hr
  dt_full[, temp_2day := frollapply(temp_use, 192, sd_rm, fill = NA, align = c("center")), ] # moving window sd for 2 days
  
  # if hourly standard deviation is NOT NA, and hourly stdev is greater than 95% quantile, remove data
  dt_full$new_temp <- ifelse(!is.na(dt_full$temp_hr),
                             ifelse(dt_full$temp_hr > quantile(dt_full$temp_hr, 0.95, na.rm = TRUE), NA, dt_full$new_temp), dt_full$new_temp) 
  
  # If 2 day standard deviation is NOT NA, run the quantile test
  dt_full$new_temp <- ifelse(!is.na(dt_full$temp_2day), 
                             ifelse(dt_full$temp_2day > quantile(dt_full$temp_2day, 0.95, na.rm = TRUE), NA, dt_full$new_temp), dt_full$new_temp)
  
  # If standard deviation of hour and 2 day are both NA, then just NA the data
  dt_full$new_temp <- ifelse(is.na(dt_full$temp_hr) & is.na(dt_full$temp_2day), NA, dt_full$new_temp)
  
  # Humidity  --------------------------------------------------------------------------------------------------------
  dt_full[, new_humidity := humidity_use] # Create new version of variable
  dt_full[, m_humidity := mean_trim(humidity_use), by = key2] # Mean of chamber by day and hour
  dt_full[, m_humidity_upper := m_humidity + 20,] # mean +/- 10 degrees
  dt_full[, m_humidity_lower := m_humidity - 20,]
  
  # Remove values that are more than 20 units away from the mean
  dt_full$new_humidity <- ifelse(dt_full$new_humidity > dt_full$m_humidity_upper, NA, dt_full$new_humidity)
  dt_full$new_humidity <- ifelse(dt_full$new_humidity < dt_full$m_humidity_lower, NA, dt_full$new_humidity)
  
  # Create rolling standard deviations
  dt_full[, humidity_hr := frollapply(humidity_use, 4, sd_rm, fill = NA, align = c("center")), ] #moving window sd for 1 hr
  dt_full[, humidity_2day := frollapply(humidity_use, 192, sd_rm, fill = NA, align = c("center")), ] # moving window sd for 2 days
  
  # if hourly standard deviation is NOT NA, and hourly stdev is greater than 95% quantile, remove data
  dt_full$new_humidity <- ifelse(!is.na(dt_full$humidity_hr),
                        ifelse(dt_full$humidity_hr > quantile(dt_full$humidity_hr, 0.95, na.rm = TRUE), NA, dt_full$new_humidity), dt_full$new_humidity) 
  
  # If 2 day standard deviation is NOT NA, run the quantile test
  dt_full$new_humidity <- ifelse(!is.na(dt_full$humidity_2day), 
                        ifelse(dt_full$humidity_2day > quantile(dt_full$humidity_2day, 0.95, na.rm = TRUE), NA, dt_full$new_humidity), dt_full$new_humidity)
  
  # If standard deviation of hour and 2 day are both NA, then just NA the data
  dt_full$new_humidity <- ifelse(is.na(dt_full$humidity_hr) & is.na(dt_full$humidity_2day), NA, dt_full$new_humidity)
  
  # Therm10chamber ----------------------------------------------------------------------
  dt_full[, newer_therm_10_chamber := therm10c_new] # Create new version of variable
  dt_full[, m_therm_10_chamber := mean_trim(therm10c_new), by = key2] # Mean of chamber by day and hour
  dt_full[, m_therm_10_chamber_upper := m_therm_10_chamber + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_10_chamber_lower := m_therm_10_chamber - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$newer_therm_10_chamber <- ifelse(dt_full$newer_therm_10_chamber > dt_full$m_therm_10_chamber_upper, NA, dt_full$newer_therm_10_chamber)
  dt_full$newer_therm_10_chamber <- ifelse(dt_full$newer_therm_10_chamber < dt_full$m_therm_10_chamber_lower, NA, dt_full$newer_therm_10_chamber)
  # No need to run rolling standard deviation cleaning functions -- it doesn't change the data, and we already cleaned these variables several times.
  
  # Therm25chamber ----------------------------------------------------------------------
  dt_full[, newer_therm_25_chamber := therm25c_new] # Create new version of variable
  dt_full[, m_therm_25_chamber := mean_trim(therm25c_new), by = key2] # Mean of chamber by day and hour
  dt_full[, m_therm_25_chamber_upper := m_therm_25_chamber + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_25_chamber_lower := m_therm_25_chamber - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$newer_therm_25_chamber <- ifelse(dt_full$newer_therm_25_chamber > dt_full$m_therm_25_chamber_upper, NA, dt_full$newer_therm_25_chamber) 
  dt_full$newer_therm_25_chamber <- ifelse(dt_full$newer_therm_25_chamber < dt_full$m_therm_25_chamber_lower, NA, dt_full$newer_therm_25_chamber)

  # Therm10pin ------------------------------------------------------------------------------ 
  dt_full[, newer_therm_10_pin := therm10pin_new] # Create new version of variable
  dt_full[, m_therm_10_pin := mean_trim(therm10pin_new), by = key2] # Mean of chamber by day and hour
  dt_full[, m_therm_10_pin_upper := m_therm_10_pin + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_10_pin_lower := m_therm_10_pin - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$newer_therm_10_pin <- ifelse(dt_full$newer_therm_10_pin > dt_full$m_therm_10_pin_upper, NA, dt_full$newer_therm_10_pin) 
  dt_full$newer_therm_10_pin <- ifelse(dt_full$newer_therm_10_pin < dt_full$m_therm_10_pin_lower, NA, dt_full$newer_therm_10_pin)
  
  # Therm90ext ------------------------------------------------------------------------------  
  dt_full[, newer_therm_90_ext := therm90ext_new] # Create new version of variable
  dt_full[, m_therm_90_ext := mean_trim(therm90ext_new), by = key2] # Mean of chamber by day and hour
  dt_full[, m_therm_90_ext_upper := m_therm_90_ext + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_90_ext_lower := m_therm_90_ext - 5,]
  
  # Remove values that are more than 5C away from the chamber mean
  dt_full$newer_therm_90_ext <- ifelse(dt_full$newer_therm_90_ext > dt_full$m_therm_90_ext_upper, NA, dt_full$newer_therm_90_ext) 
  dt_full$newer_therm_90_ext <- ifelse(dt_full$newer_therm_90_ext < dt_full$m_therm_90_ext_lower, NA, dt_full$newer_therm_90_ext)
  
  # Create +/-6 degree limits for the therm90ext variable averaged over all chambers
  dt_full[, all_therm_90_ext_upper := roll_mean_therm90ext + 6]
  dt_full[, all_therm_90_ext_lower := roll_mean_therm90ext - 6,]
  
  # Remove values that are more than 6C away from the overall mean
  dt_full$newer_therm_90_ext <- ifelse(dt_full$newer_therm_90_ext > dt_full$all_therm_90_ext_upper, NA, dt_full$newer_therm_90_ext) 
  dt_full$newer_therm_90_ext <- ifelse(dt_full$newer_therm_90_ext < dt_full$all_therm_90_ext_lower, NA, dt_full$newer_therm_90_ext)
  
  # Create rolling standard deviations
  dt_full[, therm_90_ext_hr := frollapply(therm90ext_new, 4, sd_rm, fill = NA, align = c("center")), ] #moving window sd for 1 hr
  dt_full[, therm_90_ext_2day := frollapply(therm90ext_new, 192, sd_rm, fill = NA, align = c("center")), ] # moving window sd for 2 days
  
  # if hourly standard deviation is NOT NA, and hourly stdev is greater than 95% quantile, remove data
  dt_full$newer_therm_90_ext <- ifelse(!is.na(dt_full$therm_90_ext_hr),
                        ifelse(dt_full$therm_90_ext_hr > quantile(dt_full$therm_90_ext_hr, 0.95, na.rm = TRUE), NA, dt_full$newer_therm_90_ext), 
                        dt_full$newer_therm_90_ext)
  
  # If 2 day standard deviation is NOT NA, run the quantile test
  dt_full$newer_therm_90_ext <- ifelse(!is.na(dt_full$therm_90_ext_2day),
                        ifelse(dt_full$therm_90_ext_2day > quantile(dt_full$therm_90_ext_2day, 0.95, na.rm = TRUE), NA, dt_full$newer_therm_90_ext),
                        dt_full$newer_therm_90_ext)
  
  # If standard deviation of hour and 2 day are both NA, then just NA the data
  dt_full$newer_therm_90_ext <- ifelse(is.na(dt_full$therm_90_ext_hr) & is.na(dt_full$therm_90_ext_2day), NA, dt_full$newer_therm_90_ext)

  # Therm10ambient ------------------------------------------------------------------------------  # Therm10chamber ----------------------------------------------------------------------
  dt_full[, newer_therm_10_amb := therm10amb_new] # Create new version of variable
  dt_full[, m_therm_10_amb := mean_trim(therm10amb_new), by = key2] # Mean of chamber by day and hour
  dt_full[, m_therm_10_amb_upper := m_therm_10_amb + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_10_amb_lower := m_therm_10_amb - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$newer_therm_10_amb <- ifelse(dt_full$newer_therm_10_amb > dt_full$m_therm_10_amb_upper, NA, dt_full$newer_therm_10_amb) 
  dt_full$newer_therm_10_amb <- ifelse(dt_full$newer_therm_10_amb < dt_full$m_therm_10_amb_lower, NA, dt_full$newer_therm_10_amb)
  
  # Create rolling standard deviations
  dt_full[, therm_10_amb_hr := frollapply(therm10amb_new, 4, sd_rm, fill = NA, align = c("center")), ] #moving window sd for 1 hr
  dt_full[, therm_10_amb_2day := frollapply(therm10amb_new, 192, sd_rm, fill = NA, align = c("center")), ] # moving window sd for 2 days
  
  # if hourly standard deviation is NOT NA, and hourly stdev is greater than 95% quantile, remove data
  dt_full$newer_therm_10_amb <- ifelse(!is.na(dt_full$therm_10_amb_hr),
                                       ifelse(dt_full$therm_10_amb_hr > quantile(dt_full$therm_10_amb_hr, 0.95, na.rm = TRUE), NA, dt_full$newer_therm_10_amb), 
                                       dt_full$newer_therm_10_amb) #remove data based on sd quantile
  
  # If 2 day standard deviation is NOT NA, run the quantile test
  dt_full$newer_therm_10_amb <- ifelse(!is.na(dt_full$therm_10_amb_2day),
                                       ifelse(dt_full$therm_10_amb_2day > quantile(dt_full$therm_10_amb_2day, 0.95, na.rm = TRUE), NA, dt_full$newer_therm_10_amb),
                                       dt_full$newer_therm_10_amb)

  # If standard deviation of hour and 2 day are both NA, then just NA the data
  dt_full$newer_therm_10_amb <- ifelse(is.na(dt_full$therm_10_amb_hr) & is.na(dt_full$therm_10_amb_2day), NA, dt_full$newer_therm_10_amb)

  # ------------------------------------------------------------------------------
  # Keep only these columns
  keep <- c("sl_id", "chamber", "time3", "co2_use", "co2ave1_use", "temp_use", "new_temp", "pressure_use", "humidity_use", "new_humidity", "voc_use", 
            "therm10c_new", "therm25c_new", "therm10pin_new", "therm90ext_new", "therm10amb_new", "newer_therm_10_chamber", "newer_therm_25_chamber",
            "newer_therm_10_pin", "newer_therm_90_ext", "newer_therm_10_amb", "roll_mean_therm90ext", "cs_code")
  dt_full <- subset(dt_full, select = keep)
  
  # Turn all NANs into NAs
  dt_full <- as.data.table(lapply(dt_full, function(x) {gsub("NaN", NA, x, ignore.case = T)}))
  
  # Create file name
  file_name <- paste0("GENX_Arduino_SD_Ch", dt_full$chamber[1], "_", date(dt_full$time3[1]), "_to_", date(dt_full$time3[nrow(dt_full)]), "_15min_clean.csv")
   out_path <- file.path(output_dir, file_name)
  
  # Save table
  write.table(dt_full, out_path, append = FALSE, quote = FALSE, sep = ",",
              na = "NA", dec = ".", row.names = FALSE,
              col.names = TRUE, qmethod = c("escape", "double"))

}

