import org.apache.spark.SparkContext
val sc: SparkContext // should have been created after running spark-shell
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// Set TPC-DS experiment
import com.databricks.spark.sql.perf.tpcds.TPCDS
val tpcds = new TPCDS (sqlContext = sqlContext)
// Run the test, the test set is specified as tpcds1_4
val experiment = tpcds.runExperiment(tpcds.tpcds1_4Queries, iterations=10)
