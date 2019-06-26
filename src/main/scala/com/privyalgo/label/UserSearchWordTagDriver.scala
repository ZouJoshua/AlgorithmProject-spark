package com.privyalgo.label

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by pangguosheng on 17-7-26.
  */
object UserSearchWordTagDriver {
  def main(args: Array[String]): Unit = {
    var SearchWordTagPath: String = null
    var userSearchWordPath: String = null
    var userSearchWordTagOutputPath: String = null
    val len = args.length

    for (i <- 0 until len) {
      val myVar = args(i)
      myVar match {
        case "-search.word.tag.path" => SearchWordTagPath = args(i + 1)
        case "-user.search.word.path" => userSearchWordPath = args(i + 1)
        case "-user.search.word.tag.output.path" => userSearchWordTagOutputPath = args(i + 1)
        case _ =>
      }
    }

    require(SearchWordTagPath != null, "search word tag cannot be null.[-search.word.tag.path]")
    require(userSearchWordPath != null, "user search word path cannot be null.[-user.search.word.path]")
    require(userSearchWordTagOutputPath != null,
      "user search word tag output path cannot be null.[-user.search.word.tag.output.path]")

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName +
      "_" + userSearchWordTagOutputPath.split("=").last)
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    sqlc.setConf("spark.sql.shuffle.partitions", "1024")

    val searchWordDataFrame = sqlc.read.parquet(SearchWordTagPath)
    /*
    pn, tag
    */
    val userSearchWordDataFrame: DataFrame = sqlc.read.parquet(userSearchWordPath)
      .drop("site")
    /*
    client_id|site|searchword
    */
    val userSearchWord = userSearchWordDataFrame
      .filter("client_id is not null")
      .groupBy("client_id").agg(collect_list("searchword"))

    val uut = new ExtracteUserTag(searchWordDataFrame, userSearchWord)
    val userSearchWordTag = uut.run()
    userSearchWordTag.write.mode("overwrite").parquet(userSearchWordTagOutputPath)

    sc.stop()
  }
}
