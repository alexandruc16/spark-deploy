import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import com.databricks.spark.sql.perf.tpcds.TPCDSTables

object PrepareTPCDS {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Prepare TPCDS")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        // this is used to implicitly convert an RDD to a DataFrame.
        import sqlContext.implicits._        
        
        // Tables in TPC-DS benchmark used by experiments.
        // dsdgenDir is the directory for dsdgen (you need to compile dsdgen first).
        // scalefactor is the amount of data, GB
        val tables = new TPCDSTables(sqlContext, "/opt/tpcds-kit/tools", "500G")
        // Generate data.
        tables.genData("hdfs:///tpcds", "parquet", true, false, false, false)
        // Create metastore tables in a specified database for your data.
        // After the table is created, automatically switch to the created database "sparktest"
        tables.createExternalTables("hdfs:///tpcds", "parquet", "finaltest", false, false)
        // Create a temporary table
        tables.createTemporaryTables("hdfs:///tpcds", "parquet")
    }
}
