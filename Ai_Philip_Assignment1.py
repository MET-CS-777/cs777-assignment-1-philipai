from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p





#Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1") # Note: The use of "appName" may run into "not in the cache" error
    spark = SparkSession.builder.getOrCreate() # new-added initialization method
    
    # GCP renders error message: Can not infer schema from empty dataset
    # rdd = sc.textFile(sys.argv[1])
    # print(f"Sample rows of the input: {rdd.take(5)}")
    # Load Data: "gs://met-cs-777-data/taxi-data-sorted-small.csv.bz2"
    df = spark.read.format('csv').options(header='false', inferSchema='true',  sep =",").load(sys.argv[1])
    df.show(10)
    rdd = df.rdd.map(tuple)
    
     
    #Task 1
    #Your code goes here
    taxi_lines_Corrected_RDD = rdd.filter(correctRows)
    print(f"Sample rows after filtration: {taxi_lines_Corrected_RDD.take(5)}")
    results_1 = taxi_lines_Corrected_RDD.map(lambda row: (row[0], row[1])).groupByKey().mapValues(lambda items: len(items)).top(10, key=lambda field: field[1])
    results_1_rdd = sc.parallelize(results_1)
    print("Task 1 Output: ")
    print(results_1_rdd.collect())
    # results_1_df = spark.createDataFrame(results_1, ["TaxiID", "Number of Drivers"])
    # results_1_df.show()
    

    results_1_rdd.coalesce(1).saveAsTextFile(sys.argv[2])


    #Task 2
    driver_column = "Driver ID"
    trip_duration_column = "Trip Duration by Seconds"
    trip_duration_column2 = "Trip Duration by Minutes"
    total_payment_column = "Totoal Amount"

    # Subset of RDD columns and convert it in Dataframe
    taxi_lines_corrected_df = taxi_lines_Corrected_RDD.map(lambda row: [row[i] for i in [1, 4, 16]]).toDF([driver_column, trip_duration_column, total_payment_column])
    taxi_lines_corrected_df.show(10)

    # Revert to RDD and perform computations
    results_2 = taxi_lines_corrected_df.groupBy(driver_column).agg(sum(trip_duration_column), sum(total_payment_column)).rdd.map(lambda row: (row[0], row[2]/(row[1]/60) ) ).top(10, key=lambda field: field[1])
    results_2_rdd = sc.parallelize(results_2)

    # Display
    print("Task 2 Output: ")
    print(results_2_rdd.collect())
    # results_2 = spark.createDataFrame(results_2, [driver_column, "Average Earning Per Minute"])
    # results_2.show()


    #savings output to argument
    results_2_rdd.coalesce(1).saveAsTextFile(sys.argv[3])


    #Task 3 - Optional 
    #Your code goes here

    #Task 4 - Optional 
    #Your code goes here


    sc.stop()