# Condense GENX SD files into 15 min data
# Written by Selina Cheng, last modified 21 February 2023

# Get functions and libraries
source("3_rollingSD_functions.R")

# Path containing GENX data
source_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                  "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/3b_filled_ardlog")
output_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/4_fifteen_min")

  # List files from source directory
  i <- list.files(source_dir, pattern = NULL, all.files = FALSE,
                  full.names = TRUE, recursive = TRUE,
                  ignore.case = FALSE)
  
  for(n in 1:length(i)){
    # Read in data table
    dt <- fread(i[n])
    
    # Set timestamp as POSIX object
    dt$time2 <- as.POSIXct(dt$time2, format = "%Y-%m-%d %H:%M:%S", tz ="Etc/GMT")
    
    # Now aggregate data to 15 minute interval
    rounded_seconds <- round(second(dt$time2)/60)*60
    dt$time3 <- update(dt$time2, sec = rounded_seconds)
    
    # Get minutes rounded to 15 minute interval
    rounded_min <- round(minute(dt$time3)/15)*15
    dt$time3 <- update(dt$time3, min = rounded_min)
    
    # Assign constants sl_id and chamber
    dt$sl_id <- unique(dt$sl_id)[1]
    dt$chamber <- unique(dt$chamber)[1]
    
    # Aggregate sd data to 15 minutes
    group <- c("sl_id", "chamber", "time3")
    
    # This group contains variables that we want to average together
    use <- c("co2_use", "co2ave1_use", "temp_use", "pressure_use", "humidity_use", "voc_use", "therm10c_new", "therm25c_new", "therm10pin_new",
             "therm90ext_new", "therm10amb_new", "cs_code")
    
    # First subset the data by these variables (discard all others)
    subgroup <- c(group, use)
    dt2 <- subset(dt, select = subgroup)
    dt2 <- unique(dt2)
    
    # Apply mean_rm function by group (which is organized by 15 min intervals) -- (mean_rm = mean with na.rm = T)
    dt3 <- dt2[,lapply(.SD, mean_rm), by = group]
    
    # Turn all NANs into NAs
    dt3 <- as.data.table(lapply(dt3, function(x) {gsub("NaN", NA, x, ignore.case = T)}))
    
    # Order by timestamp
    dt3 <- dt3[order(time3)]
    
    # Create file name
    file_name <- paste0("GENX_Arduino_SD_Ch", dt3$chamber[1], "_", date(dt3$time3[1]), "_to_", date(dt3$time3[nrow(dt3)]), "_15min.csv")
    out_path <- file.path(output_dir, file_name)
    
    # Save table
    write.table(dt3, out_path, append = FALSE, quote = FALSE, na = "NA", dec = ".",
                row.names = FALSE, col.names = TRUE, sep =",")
}
