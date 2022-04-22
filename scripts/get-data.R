####################################################################
########################### script setup ########################### 
####################################################################

### specify working directory
wd <- '~/nest-data'

setwd(wd)

### install packages (load)
if (!require("pacman")) install.packages("pacman")
pacman::p_load(dplyr,
               stringr,
               rjson,
               lubridate,
               tidyr,
               RMySQL)

### encode user name for Redshift (these need to be stored in .bash_profile and/or .Renviron file)
open_weather_api_key <- Sys.getenv("open_weather_api_key")

### get user name (these need to be stored in .bash_profile and/or .Renviron file)
mysql_user_name <- Sys.getenv("mysql_user_name")

### get password (these need to be stored in .bash_profile and/or .Renviron file)
mysql_password <- Sys.getenv("mysql_password")

### get host (these need to be stored in .bash_profile and/or .Renviron file)
mysql_host <- Sys.getenv("mysql_host")

### connect to MySQL database
database_connection <- RMySQL::dbConnect(
  RMySQL::MySQL(),
  dbname = 'barutt_prod',
  username = mysql_user_name,
  password = mysql_password,
  host = mysql_host,
  port = 3306,
)

### remove objects
rm(mysql_user_name)
rm(mysql_password)
rm(mysql_host)

####################################################################
############################ data import ########################### 
####################################################################

### get files of specified type in folder
csv_files <- list.files(path = '~/nest-data/data/nest/thermostats/raw-data', pattern = 'csv', recursive = TRUE)
json_files <- list.files(path = '~/nest-data/data/nest/thermostats/raw-data', pattern = 'json', recursive = TRUE)

### import and stack .csvs
csv_list_files <- list()
for (i in 1:length(csv_files)){
  
  tmp_csv_file <- read.csv(file = paste('~/nest-data/data/nest/thermostats/raw-data/',csv_files[i], sep = ''), header = TRUE)
  
  csv_list_files[[length(csv_list_files) + 1]] <- tmp_csv_file
  
  print(i)
  
}

sensor_data <- do.call(rbind, csv_list_files) %>%
               rename(date = Date,
                      time = Time,
                      average_temperature = avg.temp.,
                      average_humidity = avg.humidity.) %>%
               select(date,
                      time, 
                      average_temperature,
                      average_humidity) %>%
               mutate(date_time = as.POSIXct(format(as.POSIXct(paste(date, time, sep = ' '), tz = 'UTC'), tz = 'America/Chicago')))

rm(tmp_csv_file)
rm(csv_files)
rm(csv_list_files)

### import and stack jsons
json_list_files <- list()
for (i in 1:length(json_files)){
  
  tmp_json_file <- fromJSON(file = paste('~/nest-data/data/nest/thermostats/raw-data/',json_files[i], sep = ''))
  
  json_list_files[[length(json_list_files) + 1]] <- tmp_json_file
  
  print(i)
  
}

rm(json_files)
rm(tmp_json_file)

####################################################################
############################# event data ########################### 
####################################################################

#### identify instances of the HVAC system activating
number_of_first_levels <- length(json_list_files)

list_output <- list()

for (i in 1:number_of_first_levels){
  
  tmp_event_data_first_level <- json_list_files[[i]]
  
  if (length(tmp_event_data_first_level) == 0) {next}
  
  number_of_second_levels <- length(tmp_event_data_first_level)
  
  for (j in 1:number_of_second_levels){
    
    tmp_event_data_second_level <- tmp_event_data_first_level[[j]]$cycles
    
    if (length(tmp_event_data_second_level) == 0) {next}
    
    number_of_third_levels <- length(tmp_event_data_second_level)
    
    for (k in 1:number_of_third_levels){
      
      tmp_event_data_third_level <- tmp_event_data_second_level[[k]]
      
      if (length(tmp_event_data_third_level) == 0) {next}
      
      tmp_output <- data.frame(cbind(start_time = tmp_event_data_third_level$caption$parameters$startTime,
                                     end_time = tmp_event_data_third_level$caption$parameters$endTime,
                                     duration = tmp_event_data_third_level$duration,
                                     description = tmp_event_data_third_level$caption$plainText,
                                     heat = tmp_event_data_third_level$heat1,
                                     cool = tmp_event_data_third_level$cool1,
                                     fan = tmp_event_data_third_level$fan))
      
      list_output[[length(list_output) + 1]] <- tmp_output
    
      message((paste('first level:',i,'','second level:',j,'','third level:',k)))
      
    }
  }
}

cleansed_event_data <- do.call(rbind, list_output)

####################################################################
############################# cycle data ########################### 
####################################################################

### identify instances of change in thermostat setting, either scheduled or manual
number_of_first_levels <- length(json_list_files)

list_output <- list()

for (i in 1:number_of_first_levels){
  
  tmp_event_data_first_level <- json_list_files[[i]]
  
  if (length(tmp_event_data_first_level) == 0) {next}
  
  number_of_second_levels <- length(tmp_event_data_first_level)
  
  for (j in 1:number_of_second_levels){
    
    tmp_event_data_second_level <- tmp_event_data_first_level[[j]]$events
    
    if (length(tmp_event_data_second_level) == 0) {next}
    
    number_of_third_levels <- length(tmp_event_data_second_level)
    
    for (k in 1:number_of_third_levels){
      
      tmp_event_data_third_level <- tmp_event_data_second_level[[k]]
      
      if (length(tmp_event_data_third_level) == 0) {next}
      
      tmp_output <- data.frame(cbind(set_point_type = tmp_event_data_third_level$setPoint$setPointType,
                                     schedule_type = tmp_event_data_third_level$setPoint$scheduleType,
                                     pre_conditioning = tmp_event_data_third_level$setPoint$preconditioning,
                                     targets = tmp_event_data_third_level$setPoint$touchedBy,
                                     touched_when = tmp_event_data_third_level$setPoint$touchedWhen,
                                     touched_timezone_offet = tmp_event_data_third_level$setPoint$touchedTimezoneOffset,
                                     touched_where = tmp_event_data_third_level$setPoint$touchedWhere,
                                     scheduled_start = tmp_event_data_third_level$setPoint$scheduledStart,
                                     scheduled_day = tmp_event_data_third_level$setPoint$scheduledDay,
                                     previous_type = tmp_event_data_third_level$setPoint$previousType,
                                     emergency_heat = tmp_event_data_third_level$setPoint$emergencyHeatActive,
                                     heating_target = tmp_event_data_third_level$setPoint$targets$heatingTarget,
                                     cooling_target = tmp_event_data_third_level$setPoint$targets$coolingTarget))
                                     
      
      list_output[[length(list_output) + 1]] <- tmp_output
    
      message((paste('first level:',i,'','second level:',j,'','third level:',k)))
      
    }
  }
}

cleansed_cycle_data <- do.call(rbind, list_output)

rm(tmp_event_data_first_level)
rm(tmp_event_data_second_level)
rm(tmp_event_data_third_level)
rm(number_of_first_levels)
rm(number_of_second_levels)
rm(number_of_third_levels)
rm(tmp_output)
rm(list_output)
rm(i)
rm(j)
rm(k)

cleansed_event_data <- cleansed_event_data %>%
                       mutate(start_time = as.POSIXct(format(as.POSIXct(gsub(pattern = 'T', x = gsub(pattern = 'Z[UTC]', x = start_time, replacement = ' ', fixed = TRUE), replacement = ' ', fixed = TRUE), tz = 'UTC'), tz = 'America/Chicago')),
                              end_time = as.POSIXct(format(as.POSIXct(gsub(pattern = 'T', x = gsub(pattern = 'Z[UTC]', x = end_time, replacement = ' ', fixed = TRUE), replacement = ' ', fixed = TRUE), tz = 'UTC'), tz = 'America/Chicago')),
                              duration = gsub(pattern = 's', x = duration, replacement = ''))

list_output <- list()

for (i in 1:nrow(cleansed_event_data)){
  
  tmp_row <- slice(.data = cleansed_event_data, i)
  
  tmp_sequence <- seq(from = tmp_row$start_time, to = tmp_row$end_time, by = 'min')
  
  tmp_output <- data.frame(cbind(id = i,
                                 event_minute = as.character(tmp_sequence), 
                                 event_start_time = as.character(tmp_row$start_time),
                                 event_end_time = as.character(tmp_row$end_time),
                                 event_description = as.character(tmp_row$description),
                                 event_type_heat = as.character(tmp_row$heat),
                                 event_type_cool = as.character(tmp_row$cool),
                                 event_type_fan = as.character(tmp_row$fan)))
  
  list_output[[length(list_output) + 1]] <- tmp_output
    
  message(paste('row:',i, sep = ' '))
                          
}

raw_event_data <- do.call(rbind, list_output)

rm(tmp_row)
rm(tmp_output)
rm(list_output)
rm(json_list_files)

####################################################################
########################### quick analysis ######################### 
####################################################################

### aggregate heat and cool mins to 15 minute UOA
summarised_event_data <- raw_event_data %>%
                         mutate(rounded_event_minute = floor_date(as.POSIXct(event_minute), '15 minutes')) %>%
                         group_by(rounded_event_minute) %>%
                         summarise(heat = sum(ifelse(event_type_heat == TRUE,1,0)),
                                   cool = sum(ifelse(event_type_cool == TRUE,1,0)),
                                   fan = sum(ifelse(event_type_fan == TRUE,1,0))) %>%
                         ungroup()

### join to censor data
master_data <- left_join(x = sensor_data,
                         y = summarised_event_data,
                         by = c("date_time" = "rounded_event_minute")) %>%
               mutate(heat = coalesce(heat,0),
                      cool = coalesce(cool,0),
                      fan = coalesce(fan,0)) %>%
               fill(average_temperature,
                    average_humidity)

### get daily data; full days only
daily_data <- master_data %>%
              group_by(date = substr(date_time,1,10)) %>%
              summarise(obs = n(),
                        heat = sum(heat),
                        cool = sum(cool),
                        average_temperature = mean(average_temperature, na.rm = TRUE),
                        average_humidity = mean(average_humidity, na.rm = TRUE)) %>%
               ungroup() %>%
               filter(obs == 96) %>%
               mutate(yearless_date = substr(date,6,10)) %>%
               arrange(yearless_date,
                       date) %>%
               group_by(yearless_date) %>%
               mutate(year_num = row_number()) %>%
               ungroup() %>%
               mutate(year_ind = ifelse(year_num == 1,'py','cy'))

### prior year data
year_py <- daily_data %>%
           filter(year_ind == 'py') %>%
           rename(py_date = date,
                  py_obs = obs,
                  py_heat = heat,
                  py_cool = cool,
                  py_average_temperature = average_temperature,
                  py_average_humidity = average_humidity,
                  py_year_num = year_num,
                  py_year_ind = year_ind) %>%
           mutate(py_average_temperature = py_average_temperature*(9/5) + 32)

### current year data
year_cy <- daily_data %>%
           filter(year_ind == 'cy') %>%
           rename(cy_date = date,
                  cy_obs = obs,
                  cy_heat = heat,
                  cy_cool = cool,
                  cy_average_temperature = average_temperature,
                  cy_average_humidity = average_humidity,
                  cy_year_num = year_num,
                  cy_year_ind = year_ind) %>%
           mutate(cy_average_temperature = cy_average_temperature*(9/5) + 32)

### create you comparison
yoy_compare <- inner_join(x = year_py,
                          y = year_cy,
                          by = c("yearless_date" = "yearless_date")) %>%
               arrange(cy_date)

### add cumulative metrics
yoy_compare <- yoy_compare %>%
               mutate(py_heat_cumsum = cumsum(py_heat),
                      cy_heat_cumsum = cumsum(cy_heat))
             
### create named PDFs
pdf("~/nest-data/reference-docs/summary-plots.pdf") 

### compare heat usage by day
labels <- subset(yoy_compare[seq(1,nrow(yoy_compare), 4),], select = "yearless_date")
plot(x = yoy_compare$cy_heat,
     type = 'l',
     col = 'red',
     lwd = 2,
     lty = 1,
     main = 'YOY Heat Usage',
     xlab = 'Date',
     ylab = 'Furnace Minutes',
     ylim = c(0,3600),
     xaxt = 'n')
lines(x = yoy_compare$py_heat,
      type = 'l',
      col = 'blue',
      lwd = 2,
      lty = 1)
legend('topright',
       legend = c('CY','PY'),
       col = c('red','blue'),
       lwd = c(2,2),
       lty = c(1,1))
axis(1, 
       at = seq(1,nrow(yoy_compare),4),  
       labels = labels$yearless_date,
       cex.axis = 0.80)

### compare temperature by day
labels <- subset(yoy_compare[seq(1,nrow(yoy_compare), 4),], select = "yearless_date")
plot(x = yoy_compare$cy_average_temperature,
     type = 'l',
     col = 'red',
     lwd = 2,
     lty = 1,
     main = 'YOY Temperature',
     xlab = 'Date',
     ylab = 'Temperature (F)',
     ylim = c(50,80),
     xaxt = 'n')
lines(x = yoy_compare$py_average_temperature,
      type = 'l',
      col = 'blue',
      lwd = 2,
      lty = 1)
legend('topright',
       legend = c('CY','PY'),
       col = c('red','blue'),
       lwd = c(2,2),
       lty = c(1,1))
axis(1, 
       at = seq(1,nrow(yoy_compare),4),  
       labels = labels$yearless_date,
       cex.axis = 0.80)

### compare humidity by day
labels <- subset(yoy_compare[seq(1,nrow(yoy_compare), 4),], select = "yearless_date")
plot(x = yoy_compare$cy_average_humidity,
     type = 'l',
     col = 'red',
     lwd = 2,
     lty = 1,
     main = 'YOY Humidity',
     xlab = 'Date',
     ylab = '% Humidity',
     ylim = c(20,60),
     xaxt = 'n')
lines(x = yoy_compare$py_average_humidity,
      type = 'l',
      col = 'blue',
      lwd = 2,
      lty = 1)
legend('topright',
       legend = c('CY','PY'),
       col = c('red','blue'),
       lwd = c(2,2),
       lty = c(1,1))
axis(1, 
       at = seq(1,nrow(yoy_compare),4),  
       labels = labels$yearless_date,
       cex.axis = 0.80)

### cumulative heating minutes
labels <- subset(yoy_compare[seq(1,nrow(yoy_compare), 4),], select = "yearless_date")
plot(x = yoy_compare$cy_heat_cumsum,
     type = 'l',
     col = 'red',
     lwd = 2,
     lty = 1,
     main = 'YOY Cumulative Heating Minutes',
     xlab = 'Date',
     ylab = 'Total Minutes',
     ylim = c(0,max(pmax(yoy_compare$py_heat_cumsum,yoy_compare$cy_heat_cumsum))*1.10),
     xaxt = 'n')
lines(x = yoy_compare$py_heat_cumsum,
      type = 'l',
      col = 'blue',
      lwd = 2,
      lty = 1)
legend('topleft',
       legend = c('CY','PY'),
       col = c('red','blue'),
       lwd = c(2,2),
       lty = c(1,1))
axis(1, 
       at = seq(1,nrow(yoy_compare),4),  
       labels = labels$yearless_date,
       cex.axis = 0.80)

### terminate PDF output
dev.off()

### write data do MySQL database
dbWriteTable(conn = database_connection,
             name = 'nest_daily_summary',
             value = yoy_compare,
             append = FALSE,
             row.names = FALSE,
             overwrite = TRUE)
