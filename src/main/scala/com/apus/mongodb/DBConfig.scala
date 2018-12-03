package com.apus.mongodb

import java.text.SimpleDateFormat

/**
  * Created by Joshua on 2018-11-06
  */

object DBConfig {
  // replace the following with the right values for your MongoDB installation
  // test environment of mongodb
  val host = "10.10.40.122"
  val port = 27017
  val database = "article_repo"
  val readCollection = "operate_res"
  val writeCollection = "article_info"

  // replace the following if using authentication
  // val userName: Option[String] = Some("your user name here")
  val userName: Option[String] = None
  // val password: Option[String] = Some("your password here")
  val password: Option[String] = None

  // mongodb url
  val articleInfoUrl = (userName, password) match {
    case (Some(u), Some(pw)) => s"mongodb://$u:$pw@$host:$port/$database.$writeCollection"
    case _ => s"mongodb://$host:$port/$database.$writeCollection"
  }
  val operateResUrl = (userName, password) match {
    case (Some(u), Some(pw)) => s"mongodb://$u:$pw@$host:$port/$database.$readCollection"
    case _ => s"mongodb://$host:$port/$database.$readCollection"
  }

  // hdfs path
  val operateResSavePath = "/user/zoushuai/news_content/readmongo"
  val writeArticleInfoPath = "/user/zoushuai/news_content/writemongo"
  val oriPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"
  val entitywordsPath = "/user/zhoutong/tmp/entity_and_category"
  val unclassifiedPath = "/user/caifuli/news/tmp/unclassified"

  // date
  val currentTimestamp = System.currentTimeMillis()
  val sdf = new  SimpleDateFormat("yyyy-MM-dd")
  val today = sdf.format(currentTimestamp)


  // mark level for article
  val marklevel = 2


  def printConfig() : Unit = {
    println(s"Connecting to MongoDB at $host:$port using collection '$readCollection' in database '$database'")
    (DBConfig.userName, DBConfig.password) match {
      case (Some(u), Some(pw)) => println(s"using username '$u' and password '$pw'")
      case _ => println("not using any authentication")
    }
  }

  def parseArgs(args: Array[String]) = {
    val variables = scala.collection.mutable.Map.empty[String, String]
    if(args != null) {
      for(i <- 0 until args.length) {
        if(args(i).startsWith("-")) {
          variables(args(i).substring(1)) = args(i + 1)
        }
      }
    }
    variables.foreach(println)
    variables
  }
}
