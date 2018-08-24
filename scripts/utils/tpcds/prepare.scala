import org.apache.spark.SparkContext
val sc: SparkContext // should have been created after running spark-shell
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
import com.databricks.spark.sql.perf.tpcds.TPCDSTables
// Tables in TPC-DS benchmark used by experiments.
// dsdgenDir is the directory for dsdgen (you need to compile dsdgen first).
// scalefactor is the amount of data, GB
val tables = new TPCDSTables(sqlContext, "/opt/tpcds-kit/tools", "100G")
// Generate data.
tables.genData("hdfs:///tpcds", "parquet", true, false, false, false)
// Create metastore tables in a specified database for your data.
// After the table is created, automatically switch to the created database "sparktest"
tables.createExternalTables("hdfs:///tpcds", "parquet", "finaltest", false, false)
// Create a temporary table
tables.createTemporaryTables("hdfs:///tpcds", "parquet")
