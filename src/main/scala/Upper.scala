
object Upper extends App {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._


  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  val path = if (args.length > 0) args(0)
  else "C:\\spark\\Friday\\Sample100.csv"
  val columnName = args.tail

  val sample100 = spark
    .read
    .option("header", value = true)
    .option("inferSchema", value = true)
    .csv(path)

  val result = columnName.foldLeft(sample100) {(column,arg) => column.withColumn("upper_" + arg, upper(col(arg)))}.show()
  //val result = sample100.withColumn("upper_" + columnName, upper(col(columnName))).show()
}