package com.privyalgo.mongodb

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}



/**
  * Created by Joshua on 2018-12-06
  */


object TestWriteToMongodb {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("WriteToMongo-SparkConnector")
      .getOrCreate()

    import spark.implicits._
    val inputUri = "mongodb://127.0.0.1:27017/test_repo.table"
    val dt = "2018-12-06"


//    val df = Seq((108947105134L,"alphat"),(108947105135L,"beta")).toDF("_id","testContent")
    val df = Seq((108947105134L,"alpha","dd"),(108947105135L,"beta","eee")).toDF("_id","testContent","notChange")
    val df1 = Seq((108947105134L,"ahpla"),(108947105135L,"ateb")).toDF("_id","testContent")
    df.show(false)
    val num = df.count()
    print(num)
    df.write.options(Map("spark.mongodb.output.uri" -> inputUri))
      .mode("append")
      //      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
