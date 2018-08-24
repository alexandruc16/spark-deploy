import org.apache.spark.SparkContext
val sc: SparkContext // should have been created after running spark-shell
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
// Set TPC-DS experiment
import com.databricks.spark.sql.perf.tpcds.TPCDS
val tpcds = new TPCDS (sqlContext = sqlContext)
val queryNames = List("q65-v1.4", "q68-v1.4", "q46-v1.4", "q19-v1.4", "q59-v1.4", "q79-v1.4", "q73-v1.4", "q34-v1.4", "q98-v1.4", "q89-v1.4", "q63-v1.4", "q53-v1.4", "q52-v1.4", "q42-v1.4", "q55-v1.4", "q3-v1.4", "q43-v1.4", "q7-v1.4", "q27-v1.4", "ss_max-v1.4")
val queries = tpcds.tpcds1_4Queries.filter(q => queryNames.exists(qn => qn.contains(q.name)))
// Run the test, the test set is specified as tpcds1_4
val experiment = tpcds.runExperiment(queries, iterations=5)
