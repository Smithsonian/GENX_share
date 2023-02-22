# CO2 SD Card Data Processing Script
# Written by Leona Neftaliem, Selina Cheng, and Roy Rich, last updated 22 February 2023
# This script loads function scripts and runs functions

# Clear environment
rm(list = ls())

# Run script to load pre-processing functions and libraries
source("0_process_sd_raw_functions.R")

# Input file path to load data
sd_path <- paste0("C:/Users/", Sys.getenv("USERNAME"),"/Dropbox (Smithsonian)/GenX/Data/CO2_data/Raw_SD/7")

# Create output directory path
output_dir <- paste0("C:/Users/", Sys.getenv("USERNAME"),
                     "/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/0_processed")

# Load all files at file path
i <- list.files(path = sd_path, recursive = T, full.names = T)

# Load raw data (I went through it element by element, changing the # in the brackets after "i")
# This function often broke when it came across unusual lines of data (e.g., missing values or "garbage")
# So I went back and deleted the problematic lines in ConTEXT
dt_raw <- fread(i[1], sep = ",", fill = TRUE, header = F, na.strings = c("NA", "", "nan", "NaN"),
                col.names = (c(paste0("V", seq(1:17)))))

# Rename data -- these are the column names we want
labelnames<- c("unixtime", "timestamp", "sl_id", "co2", 
                 "co2ave_1", "co2_2",
                 "temp", "pressure", "humidity", "voc",
                 "therm_10_chamber", "therm_25_chamber",
                 "therm_10_pin","therm_90_ext", "cs_code","time_cs",
                 "counter")
# Set names for dt_raw
dt_raw <- dt_raw %>%
  set_names(labelnames)  

# Below are chambers and associated heating treatments
# 1 = amb
# 2 = amb
# 3 = 0.75
# 4 = 1.5
# 5 = 2.25
# 6 = 2.25
# 7 = 3.0
# 8 = 3.75
# 9 = 3.75
# 10 = 4.5
# 11 = 5.25
# 12 = 6.0
# 13 = ext_amb
# 14 = ext_amb
  
# add ID columns: row ID, sensor ID, and heating treatment
sensor_id <- "11" #plot number
heating_trt <- "5.25" #add degree of plot (amb, 6.1, etc.)if heating treatments are turned on
dt_raw <- add_identifiers(dt_raw)

# Isolate rows that have been shifted - checking for data that might be missing values in the last few columns
rows_shifted <- which(is.na(dt_raw$counter))

# Examine the rows with missing data, making sure they are indeed erroneous
# NOTE - this function also prints the number of rows with shifted cells. Deciding whether
# User needs to make a judgement call on whether this is an acceptable amount of data to omit 
# Compare total number of observations in the data set vs. number of observations with shifted (missing) data.
dt_shifted <- create_shifted_dt(dt_raw, rows_shifted)
View(dt_shifted)

# generate dt_raw2 (omit shifted rows)
dt_raw2 <- create_dt_raw2(dt_raw, rows_shifted)

# Create a variable for year in the data set
dt_raw2 <- dt_raw2 %>% 
  mutate(year = substring(timestamp, 1, 4))

# Look at possible years in dt
unique(dt_raw2$year)

# Filter out years that are not 2021 or 2022
wrong_year <- dt_raw2 %>% 
  filter(year != "2021" & year != "2022" & year != "2023")
# View wrong_year data to confirm that it's garbage
View(wrong_year)

# Create dt_raw3 to keep only the years that are 2021 or 2022
dt_raw3 <- dt_raw2 %>%
  filter(year == "2021" | year == "2022" | year == "2023")

# Create dt_raw4 to manipulate timestamps and convert to POSIXct object
dt_raw4 <- dt_raw3

# Note that timestamps are in Eastern Standard Time
dt_raw4$timestamp <- as.POSIXct(dt_raw4$timestamp, format = "%Y/%m/%d %H:%M:%S", tz = "Etc/GMT+5")

# Check timestamps. "check" highlights rows of data with timestamps that didn't format correctly for some reason.
check <- dt_raw3[which(is.na(dt_raw4$timestamp)),]
View(check)

# remove duplicate observations
dt_raw5 <- keep_distinct(dt_raw4)

# Remove NA timestamps
if(nrow(check) > 0){
  dt_raw5 <- dt_raw5[-which(is.na(dt_raw5$timestamp)),]
}

# write out semi-processed data 
# Get first and last dates.
# Get first date by looking at the first date in the data file
first <- date((dt_raw5$timestamp[1]))
# Get first date by looking at the minimum date in the data file
first <- date(min(dt_raw5$timestamp))
# The minimum date should match the first date in the data file
# If the two methods to get first date do not give the same value, there are some erroneous dates...
# ...where the data have moved backwards
# Sometimes, the year will be off by 1 or 2 years for a given date in the middle of the data set
# If this is the case, run the commented out code (change the date to whatever the minimum date is)
# wrong_date <- which(date(dt_raw5$timestamp) == "2021-04-10")
# dt_raw5 <- dt_raw5[-wrong_date,]

# Get last date by looking at the maximum date in the data file
last <- date(max(dt_raw5$timestamp))

# Check how many rows of data you've lost. Should be much less than 1%
(nrow(dt_raw)-nrow(dt_raw5))/nrow(dt_raw)

# Check NA timestamp
which(is.na(dt_raw5$timestamp))
# Check unique year
unique(dt_raw5$year)
# Check unique SLID
unique(dt_raw5$sl_id)

# Create file path to save processed data to
file_name <- paste0("GENX_SDlog_", sensor_id, "_", first, "_to_", last, ".csv")
out_path <- file.path(file_name, out_dir)

# Write processed data
write.table(dt_raw5, out_path,
            append = FALSE, sep = ",", dec = ".", row.names = FALSE, 
            col.names = TRUE)



