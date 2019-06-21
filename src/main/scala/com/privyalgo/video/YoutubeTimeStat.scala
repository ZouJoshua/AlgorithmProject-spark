package com.privyalgo.video

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import java.util.Date


/**
  * Created by Joshua on 19-5-8
  */
object YoutubeTimeStat {
  def main(args: Array[String]): Unit = {
    val appName = "Youtube-Time-Statistic-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext

    //********************//
    //Youtube视频更新时间处理//
    //********************//

    val y_path = "video/video_data/dt=2019-04-02"
    val fm = new SimpleDateFormat("yyyy-MM")
    val timeUDF = udf{tm:String => fm.format(new Date(tm.toLong))}
    val df = {
      spark.read.parquet(y_path)
        .filter("country = 'IN' and lang= 'en'")
        .filter("inner_type is not null")
        .filter("article_title is not null")
        .filter("text is not null")
        .filter("source_url is not null")
        .filter("category is not null")
        .filter("source_id is not null")
        .filter("resource_type is not null")
        .filter("tags is not null")
        .filter("id is not null")
        .filter("resource_type in (20002,6)")
        .select("id","ptime","utime","ctime","expire_time")
        .withColumn("utime",col("utime")*lit(1000L))
        .withColumn("ctime",col("ctime")*lit(1000L))
        .withColumn("pt",to_date(timeUDF(col("ptime"))))
        .withColumn("ut",to_date(timeUDF(col("utime"))))
        .withColumn("ct",to_date(timeUDF(col("ctime"))))
      }

    // 发布时间
    df.filter("pt >= '2019-01'").groupBy("pt").count.sort(desc("pt")).show(100)


//    抓取时间
    df.filter("ct >= '2019-01'").groupBy("ct").count.sort(desc("ct")).show(100)
//    更新时间
    df.filter("ut >= '2019-01'").groupBy("ut").count.sort(desc("ut")).show(100)

    //****************//
    //美国视频更新时间处理//
    //****************//

    val v_path = "video/video_data/dt=2019-06-12"

    val df1 = {
      spark.read.parquet("video/video_data/dt=2019-04-02")
        .filter("country = 'US' and lang= 'en'")
        .select("id", "ptime", "resource_type","utime", "ctime", "expire_time")
        .withColumn("utime", col("utime") * lit(1000L))
        .withColumn("ctime", col("ctime") * lit(1000L))
        .withColumn("pt", to_date(timeUDF(col("ptime"))))
        .withColumn("ut", to_date(timeUDF(col("utime"))))
        .withColumn("ct", to_date(timeUDF(col("ctime"))))
    }


    // 发布时间
    df1.groupBy("pt","resource_type").count.sort(desc("pt")).show(100)


    //    抓取时间
    df1.groupBy("ct","resource_type").count.sort(desc("ct")).show(100)
    //    更新时间
    df1.groupBy("ut","resource_type").count.sort(desc("ut")).show(100)



  }
}
