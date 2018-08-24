import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
import spark.implicits._
val resultsTable = spark.read.json("finaltest")
val ts: Long  = System.currentTimeMillis / 1000
val viewName = "sqlPerformance"
resultsTable.createOrReplaceTempView(viewName)
import org.apache.spark.SparkContext
val sc: SparkContext // should have been created after running spark-shell
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
sqlContext.table(viewName)

