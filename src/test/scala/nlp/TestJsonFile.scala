package nlp

import org.apache.spark.sql.SparkSession

/**
  * Created by Joshua on 2019-01-14
  */
object TestJsonFile {
  def main(args: Array[String]): Unit = {
    val appName = "TestJsonFile"
    val spark = SparkSession.builder().master("local").appName(appName).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val path = "/user/zoushuai/news/business/test2"
    val df = spark.read.json(path)
    df.show()
    df.printSchema()
    println(df.count)
  }
}
