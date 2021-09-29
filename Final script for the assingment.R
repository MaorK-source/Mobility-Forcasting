################### FINAL ASSIGNMENT: ECONOMY IN THE WORLD OF BIG DATA ######################
################### MAOR KRITENBERG & MATAN HEMI ############################################
################### STEP 1: SUB SETTING AND ORGANIZING THE DATA #############################

###### LOADING THE RELEVANT LIBRERIES FOR OUR ASSIGNMENT ####################################
rm(list = ls())
pacman::p_load(pacman, data.table, lubridate, ggplot2, readr, visdat, 
               reshape2, dplyr, stargazer, quantmod, GGally, bit64,esquisse, caret,naniar)
options(scipen = 999)

#####sub-setting apple mobility data. preparing the data for merging with other data sources#####
##### loading apple mobility report ####

dfAppleMobility <- fread("C:\\big data in economics\\final assignment\\applemobilitytrends-2021-08-11.csv",
                         header = TRUE)

##### turning apple mobility data to long table form####

lsIDColumnNames <- c("geo_type", "region", "transportation_type", "alternative_name", "sub-region", "country" )
dfAppleMobilityLongForm <- tidyr::pivot_longer( data = dfAppleMobility, cols = setdiff( names(dfAppleMobility), lsIDColumnNames), names_to = "Date", values_to = "Value" )
dim(dfAppleMobilityLongForm)

##### turning the data to data table. after we subset the data to the relevant countries and
##### transportation type. after that we removed empty columns ####

apple_mobility <- as.data.table(dfAppleMobilityLongForm, header = TRUE)
apple_mobility <- apple_mobility[region %in% c('Brazil', 'Indonesia',
                                               'Israel','Italy',
                                               'Mexico', 'Russia',
                                               'United States','India')]

apple_mobility <- apple_mobility[transportation_type %in% c('driving','walking')]
apple_mobility[, c("alternative_name","sub-region","country","geo_type"):=NULL]

#### Turning the date column to the lubridate format###
class(apple_mobility$Date)
apple_mobility$Date <- as_date(apple_mobility$Date)

####removing missing dates and changing the variables names so we could merge the data sets together####
apple_mobility<- na.omit(apple_mobility,invert = FALSE)
vis_dat(apple_mobility, warn_large_data = FALSE)

setnames(apple_mobility,"region","location")
setnames(apple_mobility,"Date","date")


###### SUB SETTING OUR WORLD IN DATA DATA SET - WE USE THIS DATA TO GET GLOBAL COVID-19 DATA ######
###### This data contributes covid-19 data such as: mortality rate, reproduction rate etc#########

covid_19_data<- fread("C:\\big data in economics\\final assignment\\owid-covid-data.csv",
                      header = TRUE)

#####sub setting for the relavent countries for the assignment ######

covid_19_data <- covid_19_data[location%in%c('Brazil', 'Indonesia',
                                             'Israel','Italy',
                                             'Mexico', 'Russia',
                                             'United States','India')]


covid_19_data$date <- as_date(covid_19_data$date)
covid_19_data <-covid_19_data[date %between% c("2020-01-13","2021-08-10")]

colnames(covid_19_data)

#########This will be the variables we will merge with the apple mobility data set#######

covid_data_subset <- covid_19_data[,c('location','date','total_cases_per_million',
                                      'new_cases_smoothed_per_million','new_deaths_smoothed_per_million',
                                      'reproduction_rate','icu_patients_per_million','weekly_hosp_admissions_per_million',
                                      'positive_rate','total_vaccinations_per_hundred','people_vaccinated_per_hundred',
                                      'people_fully_vaccinated_per_hundred','new_vaccinations_smoothed_per_million',
                                      'population_density','gdp_per_capita','extreme_poverty',
                                      'hospital_beds_per_thousand')]


#####loading Oxford data set which contributes to our predictions by######
#####adding global data about covid-19 countries prevantion policy####


oxford_data<- fread("C:\\big data in economics\\final assignment\\OxCGRT_Download_130821_063706_Compressed.csv",
                    header = TRUE)
colnames(oxford_data)
##### subseting the data:relevant countries only and removing irelavent colums from our data#####

oxford_data_subset <- oxford_data[CountryName%in%c('Brazil', 'Indonesia',
                                                   'Israel','Italy',
                                                   'Mexico', 'Russia',
                                                   'United States','India')]


oxford_data_10_var <- oxford_data_subset[,c('CountryName','Date','E3_Fiscal measures',
                                            'E4_International support','StringencyIndex',
                                            'StringencyLegacyIndex','GovernmentResponseIndex',
                                            'ContainmentHealthIndex','EconomicSupportIndex',
                                            'RegionName')]


#####working on the variables: removing NA, changing to the right formatting (date as "DATE"), etc.####

oxford_data_10_var$Date <-ymd(oxford_data_10_var$Date)


#####changing the dates range - so it will fits to the apple mobility data####

oxford_data_10_var<-oxford_data_10_var[Date %between% c("2020-01-13","2021-08-10")]

#####we will merge the data by:country&date. so we changed the name so they will be the same for all
##### the data sets####
colnames(oxford_data_subset)

setnames(oxford_data_10_var,"Date","date")
setnames(oxford_data_10_var,"CountryName","location")


##### when the region name =="", it means it is a state level data. we want to include only
##### the data for the whole country and not the region data####

country_level_data <- oxford_data_10_var[RegionName==""]

##### we reduces the data set from 180024 rows and 51 var to 4504 rows and 10 vars######
##### only the relevant countries, dates and var are part of "country_level_data"####


#### removing only the relavant data sets for the merge####
rm(covid_19_data,dfAppleMobility,dfAppleMobilityLongForm,oxfor_data_10_var,oxford_data,
   oxford_data_10_var,oxford_data_subset,oxford_subset_1)

################## MERGING THE 3 DATA SETS #########################################

ox_plus_apple_data1 <-merge(x = apple_mobility, y = country_level_data,
                            by = c("location", "date"), all = TRUE)
ox_plus_apple_data1[,'RegionName':=NULL]


mereged_data1 <-merge(x = ox_plus_apple_data1, y = covid_data_subset,
                      by = c("location", "date"), all = TRUE)

####exporting the data set to the working directory - for more convenient work####

fwrite(mereged_data1, file = "mobillity+covid data.csv")

rm(list = ls())
mobility_covid_data<- fread("C:\\big data in economics\\final assignment\\mobillity+covid data.csv",
                            header = TRUE)

####add weekdays to the data set####

Sys.setlocale("LC_TIME", "English")
mobility_covid_data$DayName <- weekdays(mobility_covid_data$date)
vis_dat(mobility_covid_data[order(date)], warn_large_data = FALSE)

############## turning the variables mentioned below to numeric var ################

mobility_covid_data$`E4_International support` <- as.numeric(mobility_covid_data$`E4_International support`)
mobility_covid_data$new_vaccinations_smoothed_per_million <- as.numeric(mobility_covid_data$new_vaccinations_smoothed_per_million)
vis_dat(mobility_covid_data[order(date)], warn_large_data = FALSE)

###### week end variable#####

mobility_covid_data$weekend_days <- ifelse(mobility_covid_data$location == 'Israel',
                                             ifelse(mobility_covid_data$DayName%in%c('Friday','Saturday'),1,0),
                                             ifelse(mobility_covid_data$DayName%in%c('Saturday','Sunday'),1,0))

######adding holidays dates to the dataset - we wanted to check if the holiday days effect mobility paterns #######

holidays_dates <- fread("C:\\big data in economics\\final assignment\\total holidays 8 countries.csv",
                                      header = TRUE)
holidays_dates <- holidays_dates[,.(location,date)]
holidays_dates$date <- dmy(holidays_dates$date)
holidays_dates$date <- as_date(holidays_dates$date)
holidays_dates <- holidays_dates[date %between% c("2020-01-13","2021-08-10")]
holidays_dates$is_holiday <- 1


mobility_covid_data <-merge(x = mobility_covid_data, y = holidays_dates,
                      by = c("location", "date"), all = TRUE)

fwrite(mobility_covid_data, file = "mobillity+covid data with holidays.csv")
rm(list = ls())

###### working on the final data set - removing NA, turning to the correct variable type########

mobility_covid_data <- fread("C:\\big data in economics\\final assignment\\mobillity+covid data with holidays.csv",
                             header = TRUE)
##### we remove 4 varibles where there is a large number of missing values####
##### and they will not contribute to our model###############################

colnames(mobility_covid_data)
mobility_covid_data[,c('RegionName','weekly_hosp_admissions_per_million',
                       'icu_patients_per_million','StringencyLegacyIndex'):=NULL]

vis_dat(mobility_covid_data[order(date)],warn_large_data = FALSE)

####turning this variables to factors####For the holiday variable we turned NA to be 0 because
#### it is a regular day and not missing value#######
mobility_covid_data$new_vaccinations_smoothed_per_million <- as.numeric(mobility_covid_data$new_vaccinations_smoothed_per_million)
mobility_covid_data$weekend_days <- as.factor(mobility_covid_data$weekend_days)
mobility_covid_data$is_holiday[is.na(mobility_covid_data$is_holiday)] = 0
mobility_covid_data$is_holiday <- as.factor(mobility_covid_data$is_holiday)


############The veccinations started only in January 2021 so there is alot of NA######
############ We turn this values to be 0 #############################################

mobility_covid_data$new_vaccinations_smoothed_per_million[mobility_covid_data$date=="2020-01-13"] <- 0
mobility_covid_data$total_vaccinations_per_hundred[mobility_covid_data$date=="2020-01-13"] <- 0
mobility_covid_data$people_fully_vaccinated_per_hundred[mobility_covid_data$date=="2020-01-13"] <- 0
mobility_covid_data$people_vaccinated_per_hundred[mobility_covid_data$date=="2020-01-13"] <- 0

mobility_covid_data$new_vaccinations_smoothed_per_million <- na.locf(mobility_covid_data$new_vaccinations_smoothed_per_million)
mobility_covid_data$total_vaccinations_per_hundred <- na.locf(mobility_covid_data$total_vaccinations_per_hundred)
mobility_covid_data$people_fully_vaccinated_per_hundred <- na.locf(mobility_covid_data$people_fully_vaccinated_per_hundred)
mobility_covid_data$people_vaccinated_per_hundred <- na.locf(mobility_covid_data$people_vaccinated_per_hundred)

##########removing NA from reproduction rate & positive rate variables#######
######### we decided to use the method of replacing NA with the value that came before it #####
######### we belive it will be a good proxy for the real value###########

mobility_covid_data$reproduction_rate[mobility_covid_data$date=="2020-01-13"] <-0
mobility_covid_data$positive_rate[mobility_covid_data$date=="2020-01-13"] <- 0

mobility_covid_data$reproduction_rate <- na.locf(mobility_covid_data$reproduction_rate)
mobility_covid_data$positive_rate <- na.locf(mobility_covid_data$positive_rate)


###### Because of the merge of 3 different data sets we have values that are NA due to the fact ######
##### that the measurement started from different dates. The missing values in this situation was replaced
##### by 0 or the value that came before them#######
mobility_covid_data$new_cases_smoothed_per_million[mobility_covid_data$date=="2020-01-13"] <-0
mobility_covid_data$new_deaths_smoothed_per_million[mobility_covid_data$date=="2020-01-13"]<-0
mobility_covid_data$total_cases_per_million[mobility_covid_data$date=="2020-01-13"]<- 0

mobility_covid_data$new_cases_smoothed_per_million <- na.locf(mobility_covid_data$new_cases_smoothed_per_million)
mobility_covid_data$new_deaths_smoothed_per_million <- na.locf(mobility_covid_data$new_deaths_smoothed_per_million)
mobility_covid_data$total_cases_per_million <-na.locf(mobility_covid_data$total_cases_per_million)

mobility_covid_data$StringencyIndex <- na.locf(mobility_covid_data$StringencyIndex)
mobility_covid_data$Value <- na.locf(mobility_covid_data$Value)
mobility_covid_data$GovernmentResponseIndex <- na.locf(mobility_covid_data$GovernmentResponseIndex)
mobility_covid_data$ContainmentHealthIndex <-na.locf(mobility_covid_data$ContainmentHealthIndex)
mobility_covid_data$EconomicSupportIndex <- na.locf(mobility_covid_data$EconomicSupportIndex)
mobility_covid_data$`E3_Fiscal measures`<-na.locf(mobility_covid_data$`E3_Fiscal measures`)
mobility_covid_data$`E4_International support` <- na.locf(mobility_covid_data$`E4_International support`)

mobility_covid_data[is.na(mobility_covid_data)]=0

###### This is our final data set, with no missing values and ready for the descriptive statistics#####
#######################and testing our model########################################################

vis_dat(mobility_covid_data[order(date)],warn_large_data = FALSE)
fwrite(mobility_covid_data, file = "data set after part 1.csv")


####### section 2 - data visualization#################################################################


############## Show the mobility value changes in the relvant dates in the 8 countries########

plot1 <-  mobility_covid_data %>%
  filter(!(transportation_type %in% "")) %>%
  ggplot() +
  aes(x = date, y = Value, colour = location) +
  geom_line(size = 0.5) +
  scale_color_hue(direction = 1) +
  labs(
    x = "Date",
    y = "Mobility Value",
    title = "Mobility Value (walking & driving) as collected rom Apple mobility datset",
    subtitle = "The red vertical line represent the date: 12/03/2020"
  ) +
  theme_minimal() +
  theme(legend.position = "none") +
  facet_wrap(vars(location))

######## We added to the plot a vertical line that marks 12/03/2020, the starting of the spread####
######## of covid-19 outside of china. You can see a decline in the Value in all countries######

dates_vline <- as.Date(c("2020-03-12")) 
dates_vline <- which(mobility_covid_data$date %in% dates_vline)

plot1+
  geom_vline(xintercept = as.numeric(mobility_covid_data$date[dates_vline]),
             col = "red", lwd = 0.1)

###########mobility by day of the week#######

value_data <- mobility_covid_data[, .(avr_value = mean(Value, na.rm = T)),
                                  by = .(DayName,transportation_type,location)]


plot2 <- value_data %>%
  filter(!(transportation_type %in% "")) %>%
  filter(!(location %in% "Israel")) %>%
  ggplot() +
  aes(x =factor(DayName, weekdays(min(mobility_covid_data$date) + 0:6))
      , y = avr_value) +
  geom_boxplot(shape = "circle", fill = "#296EA7") +
  labs(
    x = "Week Day",
    y = "Avarage Mobility Value",
    title = "The avarege mobility value by the day of the week",
    subtitle = "The data exclude Israel due to the fact that Israel's weekend is different"
  ) +
  theme_minimal()  
plot2

###### mobility average only for Israel. we did this because the weekend in Israel is in different#######
###### dates compering to other countries     ########################################

Plot3 <- value_data %>%
  filter(!(transportation_type %in% "")) %>%
  filter(location %in% "Israel") %>%
  ggplot() +
  aes(x =factor(DayName, weekdays(min(mobility_covid_data$date) + 0:6)),
      y = avr_value) +
  geom_boxplot(shape = "circle", fill = "#4682B4") +
  labs(
    x = "Week Day",
    y = "Avarage Mobility Value",
    title = "The avarege mobility value by the day of the week",
    subtitle = "This is the mobility value avarage for Israel"
  ) +
  theme_minimal()
Plot3

####################covid data and mobility. we used floor month to make average mobility value############
######### by months. we subset the data by transportation types to see how the usege is different#####
######## compering to different country and months###################

mobility_covid_data$floor_month <- floor_date(mobility_covid_data$date,'month')

month_mean <- mobility_covid_data[, .(avr_value = mean(Value, na.rm = T)),
                                  by = .(transportation_type,location,floor_month)]

plot4 <- month_mean %>%
  filter(!(transportation_type %in% "")) %>%
  ggplot() +
  aes(
    x = floor_month,
    y = avr_value,
    colour = transportation_type
  ) +
  geom_line(size = 0.5) +
  scale_color_hue(direction = 1) +
  labs(
    x = "Month",
    y = "Monthly Motility Average",
    title = "The Average Mobility Value for each country"
  ) +
  theme_minimal() +
  theme(legend.position = "bottom") +
  facet_wrap(vars(location))

plot4


############# Showing the new covid deaths per million by date##############
############ sub-setting by countries#######################################

library(dplyr)
library(ggplot2)

plot5 <- mobility_covid_data %>%
  filter(!(transportation_type %in% "")) %>%
  ggplot() +
  aes(x = date, y = new_deaths_smoothed_per_million, colour = new_deaths_smoothed_per_million) +
  geom_line(size = 0.5) +
  scale_color_gradient(low = "#5F9E01", high = "#FF1400") +
  labs(x = "Date", 
       y = "New Deaths Smoothed Per Million", title = "New Deaths per Million For each country", subtitle = "The red color indicates higher number of deaths during this dates") +
  theme_minimal() +
  theme(legend.position = "bottom") +
  facet_wrap(vars(location), scales = "free_y")

plot5

##############   PART 3 - PREDICTION FOR THE DATES 01/08/2021 - 10/08/2021 ##################

#### we wanted to check if there are differences between driving and walking######
#### we found that the correlation are almost the same so we will use the same model for both #####

walking_data <-mobility_covid_data[transportation_type=="walking"]
draiving <- mobility_covid_data[transportation_type=="driving"]

cor(walking_data[,4:23], use = "complete.obs")
cor(draiving[,4:23],use = "complete.obs")


#####we checked the influence of shifting the variable 10 days forward ######## 
mobility_covid_data[, `:=` (shift_new_cases = shift(new_cases_smoothed_per_million, 10))]
mobility_covid_data[, `:=` (shift_new_deaths = shift(new_deaths_smoothed_per_million, 10))]
mobility_covid_data[, `:=` (shift_reproduction = shift(reproduction_rate, 10))]
mobility_covid_data[, `:=` (shift_positive = shift(positive_rate, 10))]
mobility_covid_data[, `:=` (shift_vaccination = shift(people_vaccinated_per_hundred, 10))]

mobility_covid_data[is.na(mobility_covid_data)]=0

cor(mobility_covid_data[,c(4:23,28:32)], use = "complete.obs")

vis_dat(mobility_covid_data[order(date)],warn_large_data = FALSE)

#####will add transportation type binary variable which shows 1 if the transportation type is walking####

mobility_covid_data$is_walking <- ifelse(mobility_covid_data$transportation_type == 'walking',1,0)

###### training and testing our model######

sample <- sample.int(n = nrow(mobility_covid_data), size = floor(.85*nrow(mobility_covid_data)), replace = F)
trainDB <- mobility_covid_data[sample, ]
testDB  <- mobility_covid_data[-sample, ]

colnames(mobility_covid_data)
train.control <- trainControl(method = "cv",
                              number = 10,
                              savePredictions = TRUE)
colnames(mobility_covid_data)

model1 <- train(Value ~ StringencyIndex+ContainmentHealthIndex+total_cases_per_million+
                  people_vaccinated_per_hundred+hospital_beds_per_thousand+shift_reproduction+
                  is_walking+weekend_days*shift_reproduction, 
                data = trainDB, 
                method = "lm",
                trControl = train.control)

summary(model1)

model1$resample$Rsquared %>% mean()


model2 <- train(Value ~ StringencyIndex+GovernmentResponseIndex+ContainmentHealthIndex+
                  total_cases_per_million+new_deaths_smoothed_per_million+reproduction_rate+
                  positive_rate+total_vaccinations_per_hundred+people_vaccinated_per_hundred+
                  people_fully_vaccinated_per_hundred+new_vaccinations_smoothed_per_million+
                  gdp_per_capita+hospital_beds_per_thousand+weekend_days+is_holiday+
                  shift_reproduction+gdp_per_capita+shift_vaccination+total_vaccinations_per_hundred+
                  people_fully_vaccinated_per_hundred+is_walking, 
                data = trainDB, 
                method = "lm",
                trControl = train.control)

model2$resample$Rsquared %>% mean()


model3 <- train(Value ~ ., 
                data = trainDB, 
                method = "lm",
                trControl = train.control)

model3$resample$Rsquared %>% mean()

###### testing on the test data set which model has the highest R^2######

cor(testDB[, "Value"], predict(model1, testDB))**2
cor(testDB[, "Value"], predict(model2, testDB))**2
cor(testDB[, "Value"], predict(model3, testDB))**2

##### adding prediction column to the main data set#########

mobility_covid_data$predicted_Value <- predict(model3, mobility_covid_data)

subset_data <-mobility_covid_data[date %between%c("2021-08-01","2021-08-10"),
                                  .(date,location,transportation_type,Value,predicted_Value)]

#####exporting the data sets of the training, testing and predictions with our model#####

fwrite(subset_data, file = "prediction for part 3.csv")
fwrite(trainDB, file = "trainset_part3.csv")
fwrite(testDB, file = "testset_part3.csv")
fwrite(mobility_covid_data, file = "final_DB_13_08.csv")


###### PART 5 DATA PREPERATION ####################
rm(list = ls())

   ###### we will add to our original data set the dates for part 5 prediction #####

covid_mobility <- fread("C:\\big data in economics\\final assignment\\final_DB_13_08.csv",
                        header = TRUE)

##### we are removing some variables which we think will nor be relevant to our prediction#####

covid_mobility[,c(5,6,24,27:32,34):=NULL]

##### we are preparing a clean data set with the dates 12.8 - 1.9 for the prediction#####
##### The dates will be for each country and transportation types######

location <- c("Brazil", "Indonesia", "Israel", "Italy", "Mexico", "Russia", "United States", "India")
transportation_type <- c("walking", "driving")
date <- seq(ymd('2021-08-12'),ymd('2021-09-01'), by = 'days')

df <- data.frame(location = rep(location, each = length(transportation_type)*length(date)),
                 date = rep(rep(date, each = length(transportation_type)), length(location)),
                 transportation_type =  rep(transportation_type, length(location)*length(date)))

final_DB <- bind_rows(covid_mobility,df) ## Merging the data sets: the explanatory variable will be NA

###################### estimating the values of the dates we want to predict (12/08 - 01/09)######
################### we use rollmean for the estimation############################################
##Variables: "location", "date", "transportation_type", "Value",                                
##"StringencyIndex", "GovernmentResponseIndex", "ContainmentHealthIndex", "EconomicSupportIndex"                 
##"total_cases_per_million", "new_cases_smoothed_per_million"       
##"new_deaths_smoothed_per_million", "reproduction_rate"                    
##"positive_rate", "total_vaccinations_per_hundred"       
##"people_vaccinated_per_hundred", "people_fully_vaccinated_per_hundred",
##"new_vaccinations_smoothed_per_million", "population_density", "gdp_per_capita", 
##"extreme_poverty", "hospital_beds_per_thousand", "weekend_days","is_holiday", "is_walking"                           

final_DB <- final_DB %>% 
  dplyr::arrange(date) %>% 
  dplyr::group_by(location,transportation_type) %>% 
  dplyr::mutate(Value_rolling = zoo::rollapply(Value, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                StringencyIndex_rolling = zoo::rollapply(StringencyIndex, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                GovernmentResponseIndex_rolling = zoo::rollapply(GovernmentResponseIndex, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                ContainmentHealthIndex_rolling = zoo::rollapply(ContainmentHealthIndex, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                EconomicSupportIndex_rolling  = zoo::rollapply(EconomicSupportIndex, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                total_cases_per_million_rolling = zoo::rollapply(total_cases_per_million, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                new_cases_smoothed_per_million_rolling = zoo::rollapply(new_cases_smoothed_per_million, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                new_deaths_smoothed_per_million_rolling = zoo::rollapply(new_deaths_smoothed_per_million, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                reproduction_rate_rolling = zoo::rollapply(reproduction_rate, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                positive_rate_rolling = zoo::rollapply(positive_rate, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                total_vaccinations_per_hundred_rolling = zoo::rollapply(total_vaccinations_per_hundred, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                people_vaccinated_per_hundred_rolling = zoo::rollapply(people_vaccinated_per_hundred, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                people_fully_vaccinated_per_hundred_rolling = zoo::rollapply(people_fully_vaccinated_per_hundred, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                new_vaccinations_smoothed_per_million_rolling = zoo::rollapply(new_vaccinations_smoothed_per_million, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                population_density_rolling = zoo::rollapply(population_density, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                gdp_per_capita_rolling = zoo::rollapply(gdp_per_capita, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                extreme_poverty_rolling = zoo::rollapply(extreme_poverty, 30, mean,fill = NA, align = "right", na.rm = TRUE),
                hospital_beds_per_thousand_rolling = zoo::rollapply(hospital_beds_per_thousand, 30, mean,fill = NA, align = "right", na.rm = TRUE)) %>% 
  dplyr::ungroup()

final_DB <- as.data.table(final_DB)

##### this data set will include only the new dates for the prediction of part 5 #######

pred_DB <- final_DB[date >= "2021-08-11"]


pred_DB[,(4:21):=NULL] #we remove the empty variable from the data set              

#### setting the old names for the columns so we could merge the old and future data together ######

setnames(pred_DB,"Value_rolling","Value")
setnames(pred_DB,"StringencyIndex_rolling","StringencyIndex")
setnames(pred_DB,"GovernmentResponseIndex_rolling","GovernmentResponseIndex")
setnames(pred_DB,"ContainmentHealthIndex_rolling","ContainmentHealthIndex")
setnames(pred_DB,"EconomicSupportIndex_rolling","EconomicSupportIndex")
setnames(pred_DB,"total_cases_per_million_rolling","total_cases_per_million")
setnames(pred_DB,"new_cases_smoothed_per_million_rolling","new_cases_smoothed_per_million")
setnames(pred_DB,"new_deaths_smoothed_per_million_rolling","new_deaths_smoothed_per_million")
setnames(pred_DB,"reproduction_rate_rolling","reproduction_rate")
setnames(pred_DB,"positive_rate_rolling","positive_rate")
setnames(pred_DB,"total_vaccinations_per_hundred_rolling","total_vaccinations_per_hundred")
setnames(pred_DB,"people_vaccinated_per_hundred_rolling","people_vaccinated_per_hundred")
setnames(pred_DB,"people_fully_vaccinated_per_hundred_rolling","people_fully_vaccinated_per_hundred")
setnames(pred_DB,"new_vaccinations_smoothed_per_million_rolling","new_vaccinations_smoothed_per_million")
setnames(pred_DB,"population_density_rolling","population_density")
setnames(pred_DB,"gdp_per_capita_rolling","gdp_per_capita")
setnames(pred_DB,"extreme_poverty_rolling","extreme_poverty")
setnames(pred_DB,"hospital_beds_per_thousand_rolling","hospital_beds_per_thousand")

#### this data set will include all the data we have (from 2020-01-13 to 2021-08-11)######

old_data <- covid_mobility[date < "2021-08-11"]

merged_data <- rbind(old_data,pred_DB) #merging the future dates with the old ones

### dealing with NA in the categorial variables: weekend_days, is_holiday, is_walking 
Sys.setlocale("LC_TIME", "English")
merged_data$DayName <- weekdays(merged_data$date)

merged_data$weekend_days <- ifelse(merged_data$location == 'Israel',
                                     ifelse(merged_data$DayName%in%c('Friday','Saturday'),1,0),
                                     ifelse(merged_data$DayName%in%c('Saturday','Sunday'),1,0))

merged_data$is_holiday<- na.locf(merged_data$is_holiday) ## there are no holidays in the future dates

merged_data$is_walking <- ifelse(merged_data$transportation_type == 'walking',1,0)

#### setting this variables as factors and not numeric variables#####

merged_data$is_holiday <- as.factor(merged_data$is_holiday)
merged_data$is_walking <- as.factor(merged_data$is_walking)
merged_data$weekend_days <- as.factor(merged_data$weekend_days)

##### we want to check that our data in organized ad the variables are in the right class####

vis_dat(merged_data[order(date)],warn_large_data = FALSE)

#####for the prediction we will use the same model like part 3 of the assignment####
##### we are using all the explanatory variables for the prediction #######
        
          ##### using cross validation to train the model the same way like part 3 #####

sample <- sample.int(n = nrow(merged_data), size = floor(.85*nrow(merged_data)), replace = F)
trainDB <- merged_data[sample, ]
testDB  <- merged_data[-sample, ]


train.control <- trainControl(method = "cv",
                              number = 10,
                              savePredictions = TRUE)

model4 <- train(Value ~ ., 
                data = trainDB, 
                method = "lm",
                trControl = train.control)

model4$resample$Rsquared %>% mean()

#### check the r^2 of the for the test data set ######

cor(testDB[, "Value"], predict(model4, testDB))**2


merged_data$predicted_Value <- predict(model4, merged_data) # theprediction column values

subset_data_final <-merged_data[date %between%c("2021-08-18","2021-09-01"),
                                  .(date,location,transportation_type,predicted_Value)]

     ####### Exporting the relevant data sets for the assignment ######

fwrite(merged_data,"merged train data for part five.csv")
fwrite(subset_data_final,"csv for part five prediction.csv")
                                                                                  