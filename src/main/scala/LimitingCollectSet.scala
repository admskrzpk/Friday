
object LimitingCollectSet  {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val input = spark.range(50)
    .withColumn("key", $"id" % 5)
    .groupBy($"key")
    .agg(collect_set("id"))
    .toDF("key", "all")
    .withColumn("only_first_three", slice($"all", 1,3))
}
