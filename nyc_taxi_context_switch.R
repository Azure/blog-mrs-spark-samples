# set compute context
devLocal <- FALSE

if (devLocal) {
  filePrefix <- "/home/sshadmin/nyctaxi"
  inputFile  <- "/home/sshadmin/nyctaxi/000000_0"
  rxSetComputeContext("local")
  hdfsFS <- RxNativeFileSystem()
  nTree <- 10
} else {
  filePrefix <- "wasb://nyctaxi@maxkazstorage.blob.core.windows.net"
  inputFile  <- "wasb://nyctaxi@maxkazstorage.blob.core.windows.net/nyc_taxi_joined_tsv"
  sparkContext <- RxSpark(driverMem = "16g",
                          executorCores = 13, executorMem = "15g", numExecutors = 4, executorOverheadMem = "5g",
                          extraSparkConfig = "--conf spark.memory.fraction=0.9 --conf spark.memory.storageFraction=0.3 --conf yarn.nodemanager.pmem-check-enabled=false --conf yarn.nodemanager.vmem-check-enabled=false",
                          consoleOutput=TRUE,
                          nameNode= filePrefix)
  rxSetComputeContext(sparkContext)
  hdfsFS <- RxHdfsFileSystem(hostName = filePrefix)
  nTree <- 1000
}

xdfOutFile       <- file.path(filePrefix, "nyctaxixdf")
taxiSplitXdfFile <- file.path(filePrefix, "taxiSplitXdf")
taxiTrainXdfFile <- file.path(filePrefix, "taxiTrainXdf")
taxiTestXdfFile  <- file.path(filePrefix, "taxiTestXdf")
predictionFile   <- file.path(filePrefix, "predictedRF")

varsToDrop = c("medallion", "hack_license","store_and_fwd_flag",
               "pickup_datetime", "rate_code",
               "dropoff_datetime","pickup_longitude",
               "pickup_latitude", "dropoff_longitude",
               "dropoff_latitude ", "direct_distance", "surcharge",
               "mta_tax", "tolls_amount", "tip_class", "total_amount", "tip_amount")

taxiColClasses <- list(medallion = "character", hack_license = "character",
                       vendor_id =  "factor", rate_code = "factor",
                       store_and_fwd_flag = "character", pickup_datetime = "character",
                       dropoff_datetime = "character", pickup_hour = "numeric",
                       pickup_week = "numeric", weekday = "numeric",
                       passenger_count = "numeric", trip_time_in_secs = "numeric",
                       trip_distance = "numeric", pickup_longitude = "numeric",
                       pickup_latitude = "numeric", dropoff_longitude = "numeric",
                       dropoff_latitude = "numeric", direct_distance = "numeric",
                       payment_type = "factor", fare_amount = "numeric",
                       surcharge = "numeric", mta_tax = "numeric", tip_amount = "numeric",
                       tolls_amount = "numeric", total_amount = "numeric",
                       tipped = "factor", tip_class = "factor")

colInfo <- list()
for (name in names(taxiColClasses)) 
  colInfo[[paste("V", length(colInfo)+1, sep = "")]] <- list(type = taxiColClasses[[name]], newName = name)

taxiDS <- RxTextData(file = inputFile, fileSystem = hdfsFS, delimiter = "\x01", firstRowIsColNames = FALSE, 
                     colInfo = colInfo)

xdfOut <- RxXdfData(file = xdfOutFile, fileSystem = hdfsFS)

taxiDSXdf <- rxImport(inData = taxiDS, outFile = xdfOut,
                      createCompositeSet = TRUE,
                      overwrite = TRUE)

rxHistogram(~tipped|payment_type, taxiDSXdf)

#fileInfo <- rxGetInfo(taxiDSXdf, getVarInfo = TRUE, computeInfo=TRUE, getBlockSizes = TRUE)
#print(fileInfo)

taxiSplitXdf <- RxXdfData(file = taxiSplitXdfFile, fileSystem = hdfsFS);
rxDataStep(inData = taxiDSXdf, outFile = taxiSplitXdf,
           varsToDrop = varsToDrop,
           rowSelection = (passenger_count > 0 & passenger_count < 8 &
                             tip_amount >= 0 & tip_amount <= 40 &
                             fare_amount > 0 & fare_amount <= 200 &
                             trip_distance > 0 & trip_distance <= 100 &
                             trip_time_in_secs > 10 & trip_time_in_secs <= 7200),
           transforms = list( testSplitVar = ( runif( .rxNumRows ) > 0.05 ) ),
           # 25% test, %75 into training
           overwrite = TRUE)

trainDS <- RxXdfData(file = taxiTrainXdfFile,  fileSystem = hdfsFS);
testDS  <- RxXdfData(file= taxiTestXdfFile,  fileSystem = hdfsFS);

rxDataStep( inData = taxiSplitXdf, outFile = trainDS,
            varsToDrop = c( "testSplitVar"),
            rowSelection = ( testSplitVar == 1),
            overwrite = TRUE)
rxDataStep( inData = taxiSplitXdf, outFile = testDS,
            varsToDrop = c( "testSplitVar"),
            rowSelection = ( testSplitVar == 0),
            overwrite = TRUE)

#print(rxGetInfo (trainDS, getVarInfo = TRUE, computeInfo=TRUE, getBlockSizes = TRUE))
#print(rxGetInfo (testDS, getVarInfo = TRUE, computeInfo=TRUE, getBlockSizes = TRUE))

######################################################################################################
## Model building
######################################################################################################

# export data - for MLlib script
rxDataStep(inData = trainDS, outFile = RxTextData("wasb://nyctaxi@maxkazstorage.blob.core.windows.net/trainDumpSplitcsv", fileSystem = hdfsFS), overwrite = TRUE)
rxDataStep(inData = testDS,  outFile = RxTextData("wasb://nyctaxi@maxkazstorage.blob.core.windows.net/testDumpSplitcsv", fileSystem = hdfsFS), overwrite = TRUE)

# benchmark the model
pt1 <- proc.time()
model <- rxDTree(formula = tipped ~ fare_amount + vendor_id +
                   pickup_hour + pickup_week + weekday +
                   passenger_count + trip_time_in_secs +
                   trip_distance, data = trainDS, 
                   maxDepth = 10, maxNumBins = 32, xVal = 0,
                   allowDiskWrite = FALSE)
pt2 <- proc.time()
runtime <- pt2-pt1; 
print (runtime/60)

output <- RxXdfData(file=predictionFile, fileSystem = hdfsFS)
taxiDxPredict <- rxPredict(model, data = testDS,
                           outData = output, type = "class",
                           extraVarsToWrite = as.vector(c("tipped")),
                           overwrite = TRUE)

# export data - for MLlib script
rxDataStep(inData = taxiDxPredict, outFile = RxTextData("wasb://nyctaxi@maxkazstorage.blob.core.windows.net/predictSplitcsv", fileSystem = hdfsFS), overwrite = TRUE)

# benchmark the model
pt1 <- proc.time()
model <- rxDForest(formula = tipped ~ fare_amount + vendor_id +
                   pickup_hour + pickup_week + weekday +
                   passenger_count + trip_time_in_secs +
                   trip_distance, data = trainDS, 
                   maxDepth = 10, maxNumBins = 32, nTree = nTree, importance = TRUE, computeOobError = -1,
                   allowDiskWrite = FALSE)
pt2 <- proc.time()
runtime <- pt2-pt1; 
print (runtime/60)
rxVarImpPlot(model)

output <- RxXdfData(file=predictionFile, fileSystem = hdfsFS)
taxiDxPredict <- rxPredict(model, data = testDS,
                           outData = output, type = "class",
                           extraVarsToWrite = as.vector(c("tipped")),
                           overwrite = TRUE)

# export data - for MLlib script
rxDataStep(inData = taxiDxPredict, outFile = RxTextData("wasb://nyctaxi@maxkazstorage.blob.core.windows.net/predictForestSplitcsv", fileSystem = hdfsFS), overwrite = TRUE)

# compute AUC metric
#rfDF <- rxImport(inData = taxiDxPredict, outFile = NULL)
#rfDF$tipped <- as.numeric(rfDF$tipped) # shifts up by 1
#rfDF$tipped <- ifelse(rfDF$tipped == 1, 0, 1)
#rfDF$predicted_tipped_prob <- rfDF$`1_prob`

#caret::confusionMatrix(rfDF$predicted_tipped, rfDF$tipped)

#rxSetComputeContext("local")
#rocData <- rxRocCurve(actualVarName = "tipped", predVarNames = "predicted_tipped_prob", data = rfDF)


