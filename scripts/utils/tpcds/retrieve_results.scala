import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
import spark.implicits._
val tb = spark.read.json("hdfs:///spark/sql/performance")
val filteredTable = tb.drop("configuration", "tags").select($"iteration", $"timestamp", explode($"results")).withColumn("runtime", ($"col.analysisTime" + $"col.parsingTime" + $"col.optimizationTime" + $"col.planningTime" + $"col.executionTime") / 1000.0).select($"timestamp", $"iteration", $"col.name", $"col.analysisTime", $"col.parsingTime", $"col.optimizationTime", $"col.planningTime", $"col.executionTime", $"runtime").groupBy($"timestamp", $"name").agg(collect_list("analysisTime").alias("analysisTimes"), collect_list("parsingTime").alias("parsingTimes"), collect_list("optimizationTime").alias("optimizationTimes"), collect_list("planningTime").alias("planningTimes"), collect_list("executionTime").alias("executionTimes"), collect_list("runtime").alias("runtimes")).sort($"timestamp", $"name")
filteredTable.coalesce(1).write.format("json").save("hdfs://results")
