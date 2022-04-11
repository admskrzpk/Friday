object differenceInDays  {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val dates = Seq(
    "08/11/2015",
    "09/11/2015",
    "09/12/2015").toDF("date_string")

  val result2 = dates
    .withColumn("to_date", to_date($"date_string", "dd/MM/yyyy"))
    .withColumn("diff", datediff(current_date(), $"to_date"))
   .show()
}
