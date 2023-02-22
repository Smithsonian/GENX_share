# Range limitation functions used to prep data before applying rolling standard deviations
# Code by Roy Rich and Selina Cheng
# Last modified by Selina Cheng on 21 February 2023

# Load libraries
if (!require("pacman")) install.packages("pacman")
pacman::p_load(lubridate, data.table, plyr, tidyverse, zoo)

sd_rm <- function(x){ sd(x, na.rm = TRUE) } # standard deviation with na.rm = TRUE
mean_trim <- function(x){ mean(x, na.rm = TRUE, trim = 0.35) } ## Create mean, trimmed to the middle 30% of values
mean_trim_all <- function(x){mean(x, na.rm = TRUE, trim = 0.25)} ## Create mean, trimmed to the middle 50% of values
mean_rm <- function(x){ mean(x, na.rm = TRUE) } # mean function with na.rm = TRUE

# For columns co2, co2_ave1, co2_2
# Replace CO2 with NA if values are <= 300 or >= 1250
co2_replace <- function(x){replace(x, x <= (300) | x >= (1250), NA)}

# For columns: temp
# Replace temp with NA if values are < -15 or > 50
temp_replace <- function(x){replace(x, x < (-15) | x > (50), NA)}

# For thermistor columns: therm_10_chamber, therm_25_chamber, therm_10_pin, therm_90_ext
# Replace thermistor temp with NA if values are <= 0 or > 60
therm_replace <- function(x){replace(x, x <= (0) | x > (60), NA)}

# For pressure
# Replace pressure with NA if values are < 990 or > 1040
press_replace <- function(x){replace(x, x < (990) | x > (1040), NA)}

# For humidity
# Replace humidity with NA if values are < 1 or > 100
hum_replace <- function(x){replace(x, x < (1) | x > (100), NA)}

# For voc
# Replace voc with NA if values are < 50 or > 50000
voc_replace <- function(x){replace(x, x < (50) | x > (50000), NA)}