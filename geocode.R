library(googlesheets)
library(stringr)
library(dplyr)
library(tidyr)
library(readr)
library(RPostgreSQL)
library(sqldf)
library(DescTools)
library(gmailr)
library(fts)
library(stringdist)
library(stringi)
library(xtable)
library(mailR)
library(gtools)


library(ggmap)
# get the input data
infile <- "D:\\indiaehome_data/coffee3withcoded_name"
  #"home/satish/website/Indore/coffee3withcoded_name"
data <- read.csv(paste0(infile, '.csv'),header=F)


# get the address list, and append "Ireland" to the end to increase accuracy 
# (change or remove this if your address already include a country etc.)
addresses = data[,2]
city<-ifelse(addresses == 1, "Bhopal",
             ifelse (addresses == 2, "Indore", 
                     ifelse (addresses == 14, "Satna","Rewa")
             )
)
addresses = paste0(data[,11],city ,",madhya pradesh,","India",".")



getGeoDetails <- function(address){   
  #use the gecode function to query google servers
  geo_reply = geocode(address, output='all', messaging=TRUE, override_limit=TRUE)
  #now extract the bits that we need from the returned list
  answer <- data.frame(lat=NA, long=NA, accuracy=NA, formatted_address=NA, address_type=NA, status=NA)
  answer$status <- geo_reply$status
  
  #if we are over the query limit - want to pause for an hour
  while(geo_reply$status == "OVER_QUERY_LIMIT"){
    print("OVER QUERY LIMIT - Pausing for 1 hour at:") 
    time <- Sys.time()
    print(as.character(time))
    Sys.sleep(60*60)
    geo_reply = geocode(address, output='all', messaging=TRUE, override_limit=TRUE)
    answer$status <- geo_reply$status
  }
  
  #return Na's if we didn't get a match:
  if (geo_reply$status != "OK"){
    return(answer)
  }   
  #else, extract what we need from the Google server reply into a dataframe:
  answer$lat <- geo_reply$results[[1]]$geometry$location$lat
  answer$long <- geo_reply$results[[1]]$geometry$location$lng   
  if (length(geo_reply$results[[1]]$types) > 0){
    answer$accuracy <- geo_reply$results[[1]]$types[[1]]
  }
  answer$address_type <- paste(geo_reply$results[[1]]$types, collapse=',')
  answer$formatted_address <- geo_reply$results[[1]]$formatted_address
  
  return(answer)
}

#initialise a dataframe to hold the results
geocoded <- data.frame()
# find out where to start in the address list (if the script was interrupted before):
startindex <- 1
#if a temp file exists - load it up and count the rows!
tempfilename <- paste0(infile, '_temp_geocoded.rds')
if (file.exists(tempfilename)){
  print("Found temp file - resuming from index:")
  geocoded <- readRDS(tempfilename)
  startindex <- nrow(geocoded)
  print(startindex)
}

# Start the geocoding process - address by address. geocode() function takes care of query speed limit.
for (ii in seq(startindex, length(addresses))){
  print(paste("Working on index", ii, "of", length(addresses)))
  #query the google geocoder - this will pause here if we are over the limit.
  result = getGeoDetails(addresses[ii]) 
  print(result$status)     
  result$index <- ii
  #append the answer to the results file.
  geocoded <- rbind(geocoded, result)}

#   geocoded<-geocoded[2:51,]
#   #save temporary results as we are going along
#   saveRDS(geocoded, tempfilename)
# }

#now we add the latitude and longitude to the main data
data$lat <- geocoded$lat
data$long <- geocoded$lat
data$accuracy <- geocoded$accuracy

#finally write it all to the output files
saveRDS(data, paste0("../data/", infile ,"_geocoded.rds"))
write_csv(data, paste0("/",infile ,"_geocoded.csv", sep=""))





#generate code_name

# Rc id generator
idGenerator <- function(n, lengthId) { 
  idList <-paste("indiaedomes",stri_rand_strings(n, lengthId, pattern = "[A-Za-z0-9]"),sep = "")
  return(idList) }


data<-read.csv("D:\\coffee3.csv")


data<-data%>%
  mutate(coded_name=idGenerator(n(),40))

write_csv(data,"D:\\coffee3withcoded_name.csv")
