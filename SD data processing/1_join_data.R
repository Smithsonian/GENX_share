# Join SC data with LN data - Add new data to existing SD data
# "SC" = current technician, "LN" = previous technician
# Code by Selina Cheng
# Last modified by Selina Cheng on 22 February 2023
#  ---------------------------------------------------------------
# Load libraries
if (!require("pacman")) install.packages("pacman")
pacman::p_load(lubridate, data.table, plyr, tidyverse, zoo)
#  ---------------------------------------------------------------
# Read in LN cleaned data
genx_path <- "C:/Users/ChengS1/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/0_LN_processed"
genx_files <- list.files(path = genx_path, full.names = T, pattern = ".csv")
# Remove loggernet files and file for 11 because we don't have that
genx_files <- genx_files[-c(3, 6, 8, 12)]

# Create path for LN cleaned data
clean_path <- "C:/Users/ChengS1/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/0_LN_processed_clean"

# Read in SC cleaned data
sd_path <- "C:/Users/ChengS1/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/0_SC_processed"
sd_files <- list.files(path = sd_path, full.names = T)
# Remove 7 and 14 because there's no analog for them in LN data
sd_files <- sd_files[-c(4,10,11)]
#  ---------------------------------------------------------------
# Join different chamber 7 files
# ch7_start <- sd_files[10]
# ch7_end <- sd_files[11]

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

# -----------------------------------------------------------------------
# LN data has not been cleaned yet -- clean LN data first
for(k in 1:length(genx_files)){# Read in LN data
  genx_raw <- fread(genx_files[k], tz = "")
  # Remove places where timestamp is NA
  check <- genx_raw[which(is.na(genx_raw$timestamp)),]
  View(check)
  if(nrow(check) > 0){
  genx_raw2 <- genx_raw[-which(is.na(genx_raw$timestamp)),]
  } else{
    genx_raw2 <- genx_raw
  }
  # Format timestamp
  genx_raw2$timestamp <- as.POSIXct(genx_raw2$timestamp, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT")
  # Check to see if any didn't format correctly
  which(is.na(genx_raw2$timestamp))
  
  # Remove wrong years
  unique(genx_raw2$year)
  # Filter out years that are not 2021 or 2022
  wrong_year <- genx_raw2 %>% 
    filter(year != "2021" & year != "2022")
  # View wrong_year data to confirm that it's erroneous data
  View(wrong_year)
  # Keep only the years that are 2021 or 2022
  genx_raw2 <- genx_raw2 %>%
    filter(year == "2021" | year == "2022")
  
  # Isolate rows that have been shifted - checking for data that might be missing values in the last few columns
  rows_shifted <- which(is.na(genx_raw2$time_cs))
  
  # Examine the rows with missing data, making sure they are indeed erroneous
  dt_shifted <- genx_raw2[rows_shifted,]
  View(dt_shifted)
  
  # Remove the rows with missing data
  if(nrow(dt_shifted) > 0){
  genx_raw2 <- genx_raw2[-rows_shifted,]
  }
  # Keep only distinct observations
  genx_raw2 <- unique(genx_raw2)
  
  # Get first date by looking at the first date in the data file
  first <- date((genx_raw2$timestamp[1]))
  # Get first date by looking at the minimum date in the data file
  first <- date(min(genx_raw2$timestamp))
  
  # Remove erroneous observations if timestamps moved backwards
  which(date(genx_raw2$timestamp) == "2021-01-26")
  genx_raw2 <- genx_raw2[-1710383,]
  
  # Compare cleaned data to original data
  (nrow(genx_raw) - nrow(genx_raw2))/nrow(genx_raw)
  
  # Remove rolling SD columns, don't need 'em.
  genx_raw2 <- select(genx_raw2, -therm_10c_roll_SD, -therm_25c_roll_SD, -therm_10pin_roll_SD, -therm_90_ext_roll_SD)
  
  # write file
  file_name <- paste0(unlist(strsplit(basename(genx_files[k]), "."))[1], "_SC_clean", ".csv")
  out_path <- file.path(clean_path, file_name)
  
  write.table(genx_raw2, out_path, append = FALSE, quote = FALSE, sep = ",",
              na = "NA", dec = ".", row.names = FALSE,
              col.names = TRUE, qmethod = c("escape", "double"))
}

# ----------------------------------------------------------------------------------------------------------

# Read in SC's SD data
sd_raw <- fread(sd_files[k], tz = "")

# This code is for chamber 7 specifically
# ch7_start <- fread(ch7_start)
# ch7_end <- fread(ch7_end)
# 
# ch7_start$timestamp <- as.POSIXct(as.character(ch7_start$timestamp), format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT")
# ch7_end$timestamp <- as.POSIXct(as.character(ch7_end$timestamp), format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT")

# Format timestamp
sd_raw$timestamp <- as.POSIXct(as.character(sd_raw$timestamp), format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT")

# Create data frame for values from sd_raw that have timestamps in genx_raw
sd_dup <- sd_raw[which(sd_raw$timestamp %in% genx_raw2$timestamp),]

# Create data frame for values from genx_raw that have timestamps in sd_raw
genx_dup <- genx_raw2[which(genx_raw2$timestamp %in% sd_raw$timestamp),]

# Compare sd_dup and genx_dup. They should be identical.
# Put compare_dup_sd and compare_genx_dup in the same column and row order
compare_dup_sd <- select(sd_dup, order(colnames(sd_dup)))
compare_dup_sd <- compare_dup_sd[order(compare_dup_sd$timestamp),]
# Remove ID and counter columns
compare_dup_sd <- compare_dup_sd[,-c(4,7, 18)]

# Put compare_dup_sd and compare_genx_dup in the same column and row order
compare_dup_genx <- select(genx_dup, order(colnames(genx_dup)))
compare_dup_genx <- compare_dup_genx[order(compare_dup_genx$timestamp),]
# Remove ID column
compare_dup_genx <- compare_dup_genx[,-c(6, 17)]

# Convert column types to be the same
compare_dup_genx$unixtime <- as.numeric(compare_dup_genx$unixtime)
compare_dup_genx$time_cs <- as.numeric(compare_dup_genx$time_cs)
compare_dup_genx$pressure <- as.numeric(compare_dup_genx$pressure)
compare_dup_genx$co2ave_1 <- as.numeric(compare_dup_genx$co2ave_1)

# Compare, should be TRUE
all.equal(compare_dup_genx, compare_dup_sd)

# If all are equal, i.e. if my data duplicates other data, then keep my ("SC") version of the data.
# Order sd_dup and genx_dup by timestamp
sd_dup <- sd_dup[order(sd_dup$timestamp),]
genx_dup <- genx_dup[order(genx_dup$timestamp),]
all.equal(sd_dup$timestamp, genx_dup$timestamp)

# Create a new ID column that contains LN's IDs
sd_dup$LN_ID <- genx_dup$ID

# Create data frame for values from genx_raw that don't have timestamps in sd_raw
genx_new <- genx_raw2[which((genx_raw2$timestamp %in% sd_raw$timestamp) == F),]

# Create data frame for values from sd_raw that don't have matching timestamps in genx_raw
sd_new <- sd_raw[which((sd_raw$timestamp %in% genx_raw2$timestamp) == F),]

# To append all data together, you should add together:
# genx_new + sd_dup + sd_new
# If I need to append my data, add n = (1 + nrow(LN data)) to the ID of my data
# Make sure there are no duplicated IDs...
# Check all combinations? If no duplicated IDs, don't make any change
which(sd_dup$ID %in% genx_new$ID == T)
which(sd_new$ID %in% genx_new$ID == T)
which(sd_dup$ID %in% sd_new$ID == T)

genx_new$table <- "LN_data_only"
sd_dup$table <- "overlap_data"
sd_new$table <- "SC_data_only"

# Rbind everything together...
dt_new <- rbind(genx_new, sd_dup, fill = T)
dt_new <- rbind(dt_new, sd_new, fill = T)

# This is for chamber 7 specifically
# ch7_end$ID <- ch7_end$ID + 2165958
# dt_new <- rbind(ch7_start, ch7_end, fill = T)

# Do some final checks
# Check min, first, and last dates
first <- date(dt_new$timestamp)[1]
first <- min(date(dt_new$timestamp))
last <- max(date(dt_new$timestamp))

# Remove any erroneous dates
which(date(dt_new$timestamp) == "2021-01-26")
dt_new <- dt_new[-2421783,]

# Check unique years to make sure they're correct
unique(dt_new$year)
# There should be no duplicates in the ID column
which(duplicated(dt_new$ID))
# There should be no NAs in the ID column
which(is.na(dt_new$ID))
# There should be NAs in this column
which(is.na(dt_new$time_cs))
# There should also be no NAs in this column
which(is.na(dt_new$timestamp))
# Check number of rows against original data
nrow(dt_new) / (nrow(genx_raw) + nrow(sd_raw) - nrow(genx_dup))

# Create file name
sensor_id <- dt_new$sensor_id[1]
file_name <- paste0("Joined_GENX_SDlog_", sensor_id, "_", first, "_to_", last, ".csv")
out_path <- "C:/Users/ChengS1/Dropbox (Smithsonian)/GenX/Data/CO2_data/Processed_SD/USE/0_joined_processed_original_timestamp"
final_path <- paste0(out_path, "/", file_name)

# Write out data
write.table(dt_new, final_path,
            append = FALSE, sep = ",", dec = ".", row.names = FALSE, 
            col.names = TRUE)

