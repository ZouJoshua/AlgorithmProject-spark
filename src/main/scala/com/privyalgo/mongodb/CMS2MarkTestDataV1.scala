package com.privyalgo.mongodb

import org.apache.spark.sql.SparkSession

/**
  * Created by Joshua on 2019-03-15
  */
object CMS2MarkTestDataV1 {
  def main(args: Array[String]): Unit = {
    val appName = "CMSmark2-Test-Data"
    val spark = SparkSession.builder()
      .master("local")
      .appName("ReadMongoSparkConnector")
      .getOrCreate()
    val sc = spark.sparkContext

    val profilePath = "/user/hive/warehouse/apus_dm.db/recommend/article/streaming_files/article_profile_streaming_byRequest"
    val df = spark.read.parquet(profilePath)
    val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*"
    val profile = spark.read.parquet(profilePath).selectExpr("id as resource_id", "algo_profile")
    val ori_df = spark.read.parquet(newsPath)
    val result = ori_df.drop("id","streamingParseTime").join(profile,Seq("resource_id")).withColumn("buss_type", lit(0))
    result.write.save("tmp/cms2marktestdata")





  }
}
