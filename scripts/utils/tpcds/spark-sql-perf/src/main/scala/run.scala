import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpcds.TPCDS

object RunTPCDS {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Run TPCDS")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        // this is used to implicitly convert an RDD to a DataFrame.
        import sqlContext.implicits._
        
        val tpcds = new TPCDS (sqlContext = sqlContext)
        val tables = new TPCDSTables(sqlContext, "/opt/tpcds-kit/tools", "500G")
        // Create metastore tables in a specified database for your data.
        // After the table is created, automatically switch to the created database "sparktest"
        tables.createExternalTables("hdfs:///tpcds", "parquet", "finaltest", false, false)
        // Create a temporary table
        tables.createTemporaryTables("hdfs:///tpcds", "parquet")
        val queryNames = List("q65-v1.4", "q68-v1.4", "q46-v1.4", "q19-v1.4", "q59-v1.4", "q79-v1.4", "q73-v1.4", "q34-v1.4", "q98-v1.4", "q89-v1.4", "q63-v1.4", "q53-v1.4", "q52-v1.4", "q42-v1.4", "q55-v1.4", "q3-v1.4", "q43-v1.4", "q7-v1.4", "q27-v1.4", "q70-v1.4", "q82-v1.4", "qSsMax-v1.4")
        val queries = tpcds.tpcds1_4Queries.filter(q => queryNames.exists(qn => qn.contains(q.name)))
        // Run the test, the test set is specified as tpcds1_4
        val experiment = tpcds.runExperiment(queries, iterations=5)
        experiment.waitForFinish(24*60*60)
    }
}
