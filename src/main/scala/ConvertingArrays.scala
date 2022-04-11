object ConvertingArrays {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val words = Seq(Array("hello", "world")).toDF("words")
  val solution = words.withColumn("solution", concat_ws(",",col("words")))
  val solution2 = words.as[Seq[String]].map {ss => ss.mkString(" ")}.show(truncate = false)
}
