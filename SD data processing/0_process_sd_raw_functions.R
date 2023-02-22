# Script contains functions to read in CO2 data from SD cards
# Written by Leona Neftaliem and Selina Cheng, last updated 21 February 2023

# Load packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(lubridate, data.table, tidyverse)

# Function: add_identifiers
# Add identifiers to data
# arguments
# ... dt_raw - raw dataset after renaming
# returns
# ... dt_raw - raw dataset with identifier columns added
add_identifiers <- function(dt_raw) {
  # Create row ID, sensor_id, and heating treatment
  dt_raw <- dt_raw %>% 
    mutate(ID = row_number()) %>% 
    mutate(sensor_id = sensor_id) %>% 
    mutate(treatment = heating_trt)
  return(dt_raw)
}

# Function: create_shifted_dt
# Create df of rows that have erroneous ("shifted") data
# Arguments 
# ... dt_raw- raw dataset that has gone through above functions
# ... rows_shifted - index of rows that have shifted cells
# Returns
# ... dt_shifted - df of only rows with erroneous cells

create_shifted_dt <- function(dt_raw, rows_shifted) {
  # Make dt of shifted data
  dt_shifted <- dt_raw[rows_shifted,]
  # QC check - how many shifted obs?
  print(paste("There are", nrow(dt_shifted), "shifted rows in the raw data."))
  return(dt_shifted)
}

# Function: create_dt_raw2
# Create dt_raw2 (no erroneous observations)
# Arguments
# ... dt_raw - raw dataset that has gone through above functions
# ... rows_ shifted - index of rows that have shifted cells
# Returns
# ... dt_raw2 - raw dataset with erroneous rows removed
create_dt_raw2 <- function(dt_raw, rows_shifted){
  # If there are erroneous rows, create a new dataset without erroneous data
  if (nrow(dt_shifted) > 0) {
    dt_raw2 <- dt_raw[-rows_shifted,]
    return(dt_raw2)
  }
  else {
    dt_raw2 <- dt_raw
    return(dt_raw2)
  }
}

# Function: format_timestamps
# Format timestamps correctly
# Arguments 
# ... dt_raw2 - raw dataset with erroneous rows removed
# Returns 
# ... dt_raw2 - raw dataset through above functions with formatted timestamp and year variable
format_timestamps <- function(dt_raw2) {
  # add year variable to search for impossible timestamps
  dt_raw2 <- dt_raw2 %>% 
    mutate(year = substring(timestamp, 1, 4))
  # convert date/time format
  dt_raw2$timestamp <- ymd_hms(dt_raw2$timestamp)
  dt_raw2$timestamp <- as.POSIXct(dt_raw2$timestamp, tz= "Etc/GMT")
  return(dt_raw2)
}

# Function: keep_distinct
# Keep only distinct observations
# Arguments 
# ... dt_raw2 - raw dataset through above functions
# Returns
# ... dt_raw3 - raw dataset with only distinct observations
keep_distinct <- function(dt_raw3) {
  dt_raw4 <- dt_raw3 %>% 
    # Keep distinct values in following columns
    distinct(unixtime, timestamp, sl_id, co2, co2ave_1, 
             co2_2, temp, pressure, humidity, voc,
             therm_10_chamber, therm_25_chamber,therm_10_pin,
             therm_90_ext,.keep_all = TRUE)
  #QC check - how many duplicate rows were removed?
  print(paste("There are", nrow(dt_raw3) - nrow(dt_raw4), "duplicated observations in the raw data."))
  return(dt_raw4)
}

# Function: time_ladder
# Fills in all missing timestamps
# Arguments 
# ... dt_raw3 - raw dataset through above functions
# Returns
# ... dt_raw3 - raw dataset with missing timestamps added
time_ladder <- function(dt_raw3) {
  # create POSIXct object with sequence of 10 minute intervals in the data (600 s)
  timestamp <- seq.POSIXt(min(dt_raw3$timestamp, na.rm = TRUE), max(dt_raw3$timestamp, na.rm = TRUE), by = 600) 
  # Turn into a dataframe
  dt3 <- as.data.frame(timestamp)
  dt3$timestamp <- as.POSIXct(dt3$timestamp, tz = "Etc/GMT")
  
  # only use gap time - get the vales in the ideal sequence that aren't in the df - these can then be flagged as missing data.
  dt4<- anti_join(dt3,dt_raw3, copy = TRUE, suffix = c(".x", ".y")) 
  # make raw data with time ladder incorporated
  dt_raw3 <- full_join(dt_raw3, dt4, copy = TRUE, suffix = c(".x", ".y")) 
  #check on number of missing obs - this could be built into a QC check/test
  print(paste("There were", nrow(dt4), "missing observations in the raw data."))
  return(dt_raw3)
}
















