# RefinitvMatching

### About
This is matching engine for FX orders which accepts csv order files and matches the orders with order book and stores the matched records in a trade book.

### Steps to run in local
* Clone this repo in local workspace
* Run `./gradlew build` to build the jar 
* Run `spark-submit --class processors.FxMatchingProcessor --master [local] RefinitvMatching-1.0-SNAPSHOT.jar --inputPath <input file path>` to run the app locally