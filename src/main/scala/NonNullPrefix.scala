object NonNullPrefix extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val input = Seq(
    (1, "adam"),
    (1, "Mme"),
    (1, "adam"),
    (1, null),
    (1, null),
    (1, null),
    (2, "adam"),
    (3, null)).toDF("UNIQUE_GUEST_ID", "PREFIX")

  val result = input.groupBy("UNIQUE_GUEST_ID","PREFIX").agg(collect_list("PREFIX"))
  //.na.drop().show()

  //groupBy("UNIQUE_GUEST_ID").agg(max("PREFIX")).na.drop().show()

}