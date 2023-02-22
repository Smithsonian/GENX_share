# Apply preliminary rolling standard deviation limitation functions to thermistor data in GENX SD cards
# This eliminates high amounts of noise in the thermistor data
# Written by Roy Rich and Selina Cheng, last modified 21 February 2023
# --------------------------------------------------
# Get functions
source("3_rollingSD_functions.R")

# Write a time ladder function where you can specify the increment
# This function adds in missing timestamps in the data
time_ladder <- function(dt_raw3, increment) {
  #create POSIXct object with sequence of 15 minute intervals in the data (900 s)
  new_timestamp <- seq.POSIXt(min(dt_raw3$new_timestamp, na.rm = TRUE), max(dt_raw3$new_timestamp, na.rm = TRUE), by = increment) 
  dt3 <- as.data.frame(new_timestamp)
  dt3$new_timestamp <- as.POSIXct(dt3$new_timestamp, tz = "Etc/GMT")
  # only use gap time - get the vales in the ideal sequence that aren't in the df - these can then be flagged as missing data.
  dt4 <- anti_join(dt3, dt_raw3, copy = TRUE, suffix = c(".x", ".y")) 
  # make raw data with time ladder incorporated
  dt_raw3 <- full_join(dt_raw3, dt4, copy = TRUE, suffix = c(".x", ".y")) 
  #check on number of missing obs - this could be built into a QC check/test
  print(paste("There were", nrow(dt4), "missing observations in the raw data."))
  return(dt_raw3)
}
#---------------------------------------------------
# Set paths for source directory and output directory
source_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/2_timefixed")
output_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/3_rollSD_pt1")

# Clean SD data using rolling standard deviations
# Get file names
i <- list.files(source_dir, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)

for(n in 1:length(i)){
  # Read in data
  dt <- fread(i[n])
  
  # Add in timeladder for 10 second-data
  dt_full <- time_ladder(dt, 10)
  # Order by timestamp
  dt_full <- dt_full[order(new_timestamp)]
  
  # Set sl_id and chamber for grouping
  dt_full$sl_id <- unique(dt_full$sl_id)[1]
  dt_full$chamber <- unique(dt_full$chamber)[1]
  
  # Create derived vars for grouping
  dt_full[,yday := yday(new_timestamp)]
  dt_full[,hour := hour(new_timestamp)]
  
  # Keep these columns
  keep <- c("chamber", "sl_id", "year", "new_timestamp", "yday", "hour", "co2", "co2ave_1", "co2_2",
            "temp", "pressure", "humidity", "voc", "therm_10_chamber", "therm_25_chamber", "therm_10_pin",
            "therm_90_ext", "cs_code")
  dt_full <- subset(dt_full, select = keep)
  
  # Creating key for grouping data
  key2 <-  c("yday","hour") 
  
  # Run range limitation functions
  cols <- c("co2", "co2ave_1", "co2_2", "temp", "pressure", "humidity", "voc", "therm_10_chamber",
            "therm_25_chamber", "therm_10_pin", "therm_90_ext", "cs_code")
  
  # Set all measured variables to numeric
  dt_full <- dt_full[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]

  # Apply different range limitations to different columns
  dt_full <- dt_full %>%
    mutate(co2 = co2_replace(co2),
           co2ave_1 = co2_replace(co2ave_1),
           co2_2 = co2_replace(co2_2),
           temp = temp_replace(temp),
           therm_10_chamber = therm_replace(therm_10_chamber),
           therm_25_chamber = therm_replace(therm_25_chamber),
           therm_10_pin = therm_replace(therm_10_pin),
           therm_90_ext = therm_replace(therm_90_ext),
           pressure = press_replace(pressure),
           humidity = hum_replace(humidity),
           voc = voc_replace(voc))
  
  # Data cleaning functions, developed to be all the same for each variable
  # Therm10chamber ----------------------------------------------------------------------
  dt_full[, new_therm_10_chamber := therm_10_chamber] # Create new version of variable
  dt_full[, m_therm_10_chamber := mean_trim(therm_10_chamber), by = key2] # Mean of chamber by day and hour
  dt_full[, m_therm_10_chamber_upper := m_therm_10_chamber + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_10_chamber_lower := m_therm_10_chamber - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$new_therm_10_chamber <- ifelse(dt_full$new_therm_10_chamber > dt_full$m_therm_10_chamber_upper, NA, dt_full$new_therm_10_chamber) 
  dt_full$new_therm_10_chamber <- ifelse(dt_full$new_therm_10_chamber < dt_full$m_therm_10_chamber_lower, NA, dt_full$new_therm_10_chamber)
  
  # Create rolling standard deviation over 10 rows -- narrow window
  dt_full[, therm_10_chamber_hr := frollapply(therm_10_chamber, 10, sd_rm, fill = NA, align = c("center")), ]

  # if standard deviation is NOT NA, and stdev is greater than 95% quantile, remove data
  dt_full$new_therm_10_chamber <- ifelse(!is.na(dt_full$therm_10_chamber_hr),
                        ifelse(dt_full$therm_10_chamber_hr > quantile(dt_full$therm_10_chamber_hr, 0.95, na.rm = TRUE), NA, dt_full$new_therm_10_chamber), dt_full$new_therm_10_chamber) 
  
  # If standard deviation is NA, then just NA the data
  dt_full$new_therm_10_chamber <- ifelse(is.na(dt_full$therm_10_chamber_hr), NA, dt_full$new_therm_10_chamber)
  
  # Therm25chamber ----------------------------------------------------------------------
  dt_full[, new_therm_25_chamber := therm_25_chamber] # Create new version of variable
  dt_full[, m_therm_25_chamber := mean_trim(therm_25_chamber), by = key2] # Mean of chamber by day and hour
  dt_full[, m_therm_25_chamber_upper := m_therm_25_chamber + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_25_chamber_lower := m_therm_25_chamber - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$new_therm_25_chamber <- ifelse(dt_full$new_therm_25_chamber > dt_full$m_therm_25_chamber_upper, NA, dt_full$new_therm_25_chamber) 
  dt_full$new_therm_25_chamber <- ifelse(dt_full$new_therm_25_chamber < dt_full$m_therm_25_chamber_lower, NA, dt_full$new_therm_25_chamber)
  
  # Create rolling standard deviation over 10 rows -- narrow window
  dt_full[, therm_25_chamber_hr := frollapply(therm_25_chamber, 10, sd_rm, fill = NA, align = c("center")), ] 

  # if standard deviation is NOT NA, and stdev is greater than 95% quantile, remove data
  dt_full$new_therm_25_chamber <- ifelse(!is.na(dt_full$therm_25_chamber_hr),
                        ifelse(dt_full$therm_25_chamber_hr > quantile(dt_full$therm_25_chamber_hr, 0.95, na.rm = TRUE), NA, dt_full$new_therm_25_chamber), dt_full$new_therm_25_chamber) 
  
  # If standard deviation is NA, then just NA the data
  dt_full$new_therm_25_chamber <- ifelse(is.na(dt_full$therm_25_chamber_hr), NA, dt_full$new_therm_25_chamber)
  
  # Therm10pin -------------------------------------------------------------------------------
  dt_full[, new_therm_10_pin := therm_10_pin] # Create new version of variable
  dt_full[, m_therm_10_pin := mean_trim(therm_10_pin), by = key2] # Mean of chamber by day and hour
  dt_full[, m_therm_10_pin_upper := m_therm_10_pin + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_10_pin_lower := m_therm_10_pin - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$new_therm_10_pin <- ifelse(dt_full$new_therm_10_pin > dt_full$m_therm_10_pin_upper, NA, dt_full$new_therm_10_pin) 
  dt_full$new_therm_10_pin <- ifelse(dt_full$new_therm_10_pin < dt_full$m_therm_10_pin_lower, NA, dt_full$new_therm_10_pin)
  
  # Create rolling standard deviation over 10 rows -- narrow window
  dt_full[, therm_10_pin_hr := frollapply(therm_10_pin, 10, sd_rm, fill = NA, align = c("center")), ] 

  # if standard deviation is NOT NA, and stdev is greater than 95% quantile, remove data
  dt_full$new_therm_10_pin <- ifelse(!is.na(dt_full$therm_10_pin_hr),
                        ifelse(dt_full$therm_10_pin_hr > quantile(dt_full$therm_10_pin_hr, 0.95, na.rm = TRUE), NA, dt_full$new_therm_10_pin), dt_full$new_therm_10_pin)

  # If standard deviation is NA, then just NA the data
  dt_full$new_therm_10_pin <- ifelse(is.na(dt_full$therm_10_pin_hr), NA, dt_full$new_therm_10_pin)
  
  # Therm90ext ------------------------------------------------------------------------------  
  dt_full[, new_therm_90_ext := therm_90_ext] # Create new version of variable
  dt_full[, m_therm_90_ext := mean_trim(therm_90_ext), by = key2] # Mean of chamber by day and hour
  dt_full[, m_therm_90_ext_upper := m_therm_90_ext + 5,] # mean +/- 5 degrees
  dt_full[, m_therm_90_ext_lower := m_therm_90_ext - 5,]
  
  # Remove values that are more than 5C away from the mean
  dt_full$new_therm_90_ext <- ifelse(dt_full$new_therm_90_ext > dt_full$m_therm_90_ext_upper, NA, dt_full$new_therm_90_ext) 
  dt_full$new_therm_90_ext <- ifelse(dt_full$new_therm_90_ext < dt_full$m_therm_90_ext_lower, NA, dt_full$new_therm_90_ext)
  
  # Create rolling standard deviation over 10 rows -- narrow window
  dt_full[, therm_90_ext_hr := frollapply(therm_90_ext, 10, sd_rm, fill = NA, align = c("center")), ] 

  # if standard deviation is NOT NA, and stdev is greater than 95% quantile, remove data
  dt_full$new_therm_90_ext <- ifelse(!is.na(dt_full$therm_90_ext_hr),
                        ifelse(dt_full$therm_90_ext_hr > quantile(dt_full$therm_90_ext_hr, 0.95, na.rm = TRUE), NA, dt_full$new_therm_90_ext), dt_full$new_therm_90_ext)

  # If standard deviation is NA, then just NA the data
  dt_full$new_therm_90_ext <- ifelse(is.na(dt_full$therm_90_ext_hr), NA, dt_full$new_therm_90_ext)
  # ------------------------------------------------------------------------------
  # Choose vars to keep
  keep <- c("chamber", "sl_id", "year", "new_timestamp", "co2", "co2ave_1", "co2_2",
            "temp", "pressure", "humidity", "voc", "therm_10_chamber", "therm_25_chamber", "therm_10_pin",
            "therm_90_ext", "cs_code", "new_therm_10_chamber", "new_therm_25_chamber", "new_therm_10_pin", "new_therm_90_ext")
  dt_full <- subset(dt_full, select = keep)
  
  # Remove any extra timeladder rows to reduce size of data
  dt_full <- subset(dt_full, subset = (!is.na(dt_full$cs_code)))
  
  # Turn all NANs into NAs
  dt_full <- as.data.table(lapply(dt_full, function(x) {gsub("NaN", NA, x, ignore.case = T)}))
  # Order by timestamp again
  dt_full <- dt_full[order(new_timestamp),]
  
  # Create file name
  file_name <- paste0("GENX_Arduino_SD_Ch", dt_full$chamber[1], "_", date(dt_full$new_timestamp[1]), "_to_", 
                      date(dt_full$new_timestamp[nrow(dt_full)]), "_cleantherms.csv")
  out_path <- file.path(output_dir, file_name)
  
  # Save table
  write.table(dt_full, out_path, append = FALSE, quote = FALSE, sep = ",",
              na = "NA", dec = ".", row.names = FALSE,
              col.names = TRUE, qmethod = c("escape", "double"))
   
}



