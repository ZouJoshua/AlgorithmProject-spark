package com.apus.mongodb

/**
  * Created by Joshua on 2018-11-06
  */

object DBConfig {
  // replace the following with the right values for your MongoDB installation

  val host = "10.10.40.122"
//  val host = "127.0.0.1"
  val port = 27017
  val database = "article_repo"
  val readCollection = "operate_res"
  val writeCollection = "article_info"

  // replace the following if using authentication
  // val userName: Option[String] = Some("your user name here")
  val userName: Option[String] = None
  // val password: Option[String] = Some("your password here")
  val password: Option[String] = None

  // hdfs path
  val writeToHdfsPath = "/user/zoushuai/news_content/read_mongo_test"
  val readFromHdfsPath = "/user/zoushuai/news_content/write_mongo_test"

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
