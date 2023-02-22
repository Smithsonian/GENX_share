# Code to fix issues in SD cards with time moving backwards erroneously
# Code by Roy Rich and Selina Cheng
# Script originated 03 January 2023
# Last modified by Selina Cheng on 22 February 2023

# Load libraries
if (!require("pacman")) install.packages("pacman")
pacman::p_load(lubridate, data.table, tidyverse, zoo)
#  ---------------------------------------------------------------
# Load paths for input arduino data and output directory
ARD_path<-"C:/Users/ChengS1/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/1_joined_processed_original_timestamp"
out_dir <- "C:/Users/ChengS1/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/2_timefixed"

# Processed chamber 7 separately for 2021-06-01 to 2022-02-17 and 2022-08-18 to 2022-10-12, then rbind at end

#  ---------------------------------------------------------------
# Get files from Arduino path
j <- list.files(ARD_path, pattern = NULL, all.files = FALSE, 
                full.names = TRUE, recursive = TRUE, 
                ignore.case = FALSE)

  for (n in 1:length(j)) {
    # Pull in Arduino data
    da <- fread(j[n], tz="")
    # Remove any timestamps with erroneous formatting
    da <- subset(da, grepl("20..-..-.. ..:..:..", da$timestamp))
    
    # Format timestamps and add new timeunix column
    da$timestamp <- as.POSIXct(da$timestamp, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT") # posix conversion is rate limiter
    da[,timeunix:= as.numeric(timestamp, origin = "1970-01-01",tz = "Etc/GMT" )] #change timestamp to unixtime
    
    setDT(da)
    
    # Trim time values to specific unix range, set as numeric
    da$unixtime <- as.numeric(da$unixtime)
    da[, timecs2:= ifelse(between(time_cs,1620000000, 1670000000), time_cs, NA)]
    da[, timecs3:= ifelse(between(timecs2, 1620000000, 1670000000),timecs2, NA)]
    
    da$timecs2 <- as.numeric(da$timecs2)
    da$timecs3 <- as.numeric(da$timecs3)
    
    # ---------------------------------------------------------------------------------------------------
    # Fill in missing rowIDs if data was not joined with old SD data ("LN" = initials of previous technician)
    if("LN_data_only" %in% unique(da$table) == F){
      # Create full rowID sequence and join with data
      ID <- seq(min(da$ID, na.rm = TRUE), max(da$ID, na.rm = TRUE)) 
      ID <- as.data.table(ID)
      # Get rowIDs not in data
      dt4 <- anti_join(ID, da, copy = T)
      # Join missing IDs with data
      dt_full_id <- full_join(da, dt4, copy = TRUE) 
      print(paste("There were", nrow(dt4), "missing IDs in the raw data."))
      
      # Order by ID 
      dt_full_id <- dt_full_id[order(dt_full_id$ID),]
      
      # If this is length > 0, then there are IDs not in order
      print(which(diff(dt_full_id$ID) < 0 | diff(dt_full_id$ID) > 1))
      
    }
    
    # Fill in missing rowIDs if data WAS joined with old SD data ("LN" = initials of previous technician)
    if("LN_ID" %in% unique(da$table) == T){
      da$LN_ID <- ifelse(is.na(da$LN_ID), da$ID, da$LN_ID)
      da_LN <- filter(da, table == "LN_data_only" | table == "overlap_data")
      # da_LN <- da[1:2155003,]
      # Create full sequence of rowID from LN data
      LN_ID <- seq(min(da_LN$ID, na.rm = TRUE), max(da_LN$ID, na.rm = TRUE))
      LN_ID <- as.data.table(LN_ID)
      dt4 <- anti_join(LN_ID, da_LN, copy = T, by = c("LN_ID" = "ID"))
      da_LN_full <- full_join(da_LN, dt4, copy = TRUE, by = c("ID" = "LN_ID")) 
      print(paste("There were", nrow(dt4), "missing IDs in the raw data."))
      da_LN_full <- da_LN_full[order(da_LN_full$ID),]
      
      # Create ID timeladder for SC data only ("SC" = initials of current tech)
      # Find first row where SC data begins
      overlap_rows <- which(da$table == "overlap_data")
      last_overlap_line <- overlap_rows[length(overlap_rows)]
      da_SC <- da[last_overlap_line:nrow(da),]
      
      # Create full sequence of rowID from SC data
      SC_ID <- seq(min(da_SC$ID, na.rm = T), max(da_SC$ID, na.rm = T))
      SC_ID <- as.data.table(SC_ID)
      dt4 <- anti_join(SC_ID, da_SC, copy = T, by = c("SC_ID" = "ID"))
      da_SC_full <- full_join(da_SC, dt4, copy = T, by = c("ID" = "SC_ID"))
      print(paste("There were", nrow(dt4), "missing IDs in the raw data."))
      da_SC_full <- da_SC_full[order(da_SC_full$ID),]
  
      # Bind new timeladdered data together
      dt_full_id <- rbind(da_LN_full, da_SC_full[2:nrow(da_SC_full),])
    }
 
    # ---------------------------------------------------------------------------------------------------
    # This code is for chamber 105, where LN data sandwiches the overlapping data. Luckily there was no SC only data
    # So I didn't have to reconcile any weird IDs, I just ordered by LN_ID.
    if("SC_data_only" %in% unique(da$table) == F){
      # Create full sequence of rowID
      da$LN_ID <- da$ID
      LN_ID <- seq(min(da$LN_ID, na.rm = TRUE), max(da$LN_ID, na.rm = TRUE)) 
      LN_ID <- as.data.table(LN_ID)
      dt4 <- anti_join(LN_ID, da, copy = T)
      dt_full_id <- full_join(da, dt4, copy = TRUE) 
      print(paste("There were", nrow(dt4), "missing IDs in the raw data."))
      
      dt_full_id <- dt_full_id[order(dt_full_id$LN_ID),]
      
      # If this is length > 0, then there are IDs not in order
      print(which(diff(dt_full_id$ID) < 0 | diff(dt_full_id$ID) > 1))
    }
    # --------------------------------------------------------------------------------------------
    # Store old data
    da_original <- da
    # Overwrite da with dt_full_id
    da <- dt_full_id
    rm(dt_full_id)
    
    # Identifies 1st occurrence of each timestamp
    da[,label:= (!duplicated(timecs3))] 
    # Removes later occurrences of time_cs
    da[,timecs3:= ifelse(label == F, NA, timecs3)]

    # Check that everything's in increasing order
    clean_timecs3 <- na.omit(da$timecs3)
    diffs <- diff(clean_timecs3)
    # Are there any timestamps that are going backwards in time?
    to_remove <- which(diffs < 0)
    time_remove <- clean_timecs3[to_remove]
    row_remove <- which(da$timecs3 == time_remove)
    
    # Start of bad time period (timestamps beginning at 2021-12-17 13:49:45 jumped backwards)
    # The numbers are the unixtimes that correspond to the bad times
    # View row_remove and see which rows need to be removed 
    start <- which(da$timecs3 == 1639745536)
    end <- which(da$timecs3 == 1639749120)
    end <- end-1
    check <- da[start:end,]
    
    # Remove bad time period
    da_new <- da[-c(start:end),]
    
    # check diffs again
    print(which(diff(na.omit(da_new$timecs3)) < 0))
    
    # Create time spline using linear approximation between known timestamps
    da_new[, time_new_unix:= zoo::na.approx(timecs3, na.rm = F)]
    da_new[, time_new_unix := ifelse(between(time_new_unix, min(da_new$timecs3, na.rm = TRUE), 
                                             max(da_new$timecs3, na.rm = TRUE)), time_new_unix, NA)]
    
    # --------------------------------------------------------------------------------------------
    # If there are leading NAs due to the linear approximation:
    if(is.na(da_new$time_new_unix[1])){
      # Get starting unixtime for leading NAs to create another spline
      start <- da_new$unixtime[1]
      
      # Get number of leading NAs
      leading <- which(is.na(da_new$time_new_unix))
      diffs <- diff(leading)
      which(diffs > 1)
      num_rows <- which(diffs > 1)
      
      # Get unixtime at the end of the leading NAs to create another spline
      end <- as.numeric(da_new$unixtime[num_rows])
      
      # If there is no unixtime at the end of the leading NAs, then take the next unixtime
      if(is.na(end)){ end <- as.numeric(da_new$unixtime[num_rows+1])}
      
      # If the starting unixtime is later than the ending unixtime, the starting time is erroneous, so just create a 10s sequence from the end
      if(start >= end){
        new_times <- seq(from = end, length.out = num_rows+1, by = -10)
        new_times <- rev(new_times[2:length(new_times)])
      } else{
        # Otherwise, create a linear approximation from the starting unixtime to the ending unixtime
        new_times <- zoo::na.approx(c(start, rep(NA, num_rows-1), end))
        new_times <- new_times[1:(length(new_times)-1)]
      }
      # Set leading NA time_new_unix to the new times
      da_new$time_new_unix[1:num_rows] <- new_times
    }
    
    # If there are trailing NAs:
    if(is.na(da_new$time_new_unix[nrow(da_new)])){
      # Get starting unixtime for trailing NAs to create another spline
      start <- da_new$time_new_unix[which(is.na(da_new$time_new_unix))[1]-1]
      # Get number of trailing NAs
      num_rows <- length(which(is.na(da_new$time_new_unix)))
      
      # Get unixtime at the end of the trailing NAs to create another spline
      end <- as.numeric(da_new$timestamp[nrow(da_new)])
      if(end <= start){
        # If the ending unixtime is earlier than the starting unixtime, just increase time by ten seconds
        new_times <- seq(from = start, length.out = num_rows+1, by = 10)
        new_times <- new_times[2:length(new_times)]
      } else{
        # Otherwise, create a linear approximation from the starting unixtime to the ending unixtime
        new_times <- zoo::na.approx(c(start, rep(NA, num_rows-1), end))
        new_times <- new_times[2:length(new_times)]
      }
      # Set trailing NA time_new_unix to the new times
      da_new$time_new_unix[which(is.na(da_new$time_new_unix))] <- new_times
    }
    
    # --------------------------------------------------------------------------------------------
    # Create new timestamp
    da_new[, new_timestamp:= as.POSIXct(time_new_unix, origin = "1970-01-01", tz = "Etc/GMT")]
    
    # Are any timestamps NA?
    which(is.na(da_new$new_timestamp))
    # Check diffs
    print(which(diff(da_new$new_timestamp)< 0))

    # Remove unneeded columns
    da_new <- select(da_new, -sensor_id, -treatment, -year, -timeunix, -timecs2, -label, -counter)
    
    # Add keys for working with LGR data
    da_new[,chamber:=sl_id-100,]
    da_new[,year:= year(new_timestamp),]
    
    # Get filename
    file_name <- c(paste0("SC_tcor_", basename(j[n])))
    out_path <- file.path(out_dir, file_name)
    
    # In the future, change this to append = T
    # Write file
    write.table(da_new, out_path, append = FALSE, quote = FALSE, sep = ",",
                na = "NA", dec = ".", row.names = FALSE,
                col.names = TRUE, qmethod = c("escape", "double"))
  }
