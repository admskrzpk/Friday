
object Upper extends App {
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions.{col, upper}

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  val path = args(0)

  val sample100 = spark
    .read
    .option("header", value = true)
    .option("inferSchema", value = true)
    .csv(path)
    .toDF()

  val columnName = args(1)


//    val result = sample100.withColumn("upper", upper(col("city"))).show()
}
