// Set TPC-DS experiment
import com.databricks.spark.sql.perf.tpcds.TPCDS
val tpcds = new TPCDS (sqlContext = sqlContext)
// Run the test, the test set is specified as tpcds1_4
val experiment = tpcds.runExperiment(tpcds.tpcds1_4Queries)