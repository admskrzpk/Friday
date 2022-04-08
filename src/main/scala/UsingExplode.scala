object UsingExplode  {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val nums = Seq(Seq(1,2,3)).toDF("nums")
  val result1 = nums.withColumn("num", explode($"nums"))

}
