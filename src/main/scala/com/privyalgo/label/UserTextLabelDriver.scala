package com.privyalgo.label

import com.apus.algo.common.util.DataLoader
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算用户兴趣标签的drive类
  * Created by wuxiushan on 17-4-27.
  */
object UserTextLabelDriver {
  def main(args: Array[String]): Unit = {
    var appLabelInput: String = null
    var userAppsInput: String = null
    var outputDir: String = null
    val len = args.length

    for (i <- 0 until len) {
      val myVar = args(i)
      myVar match {
        case "-alIn" => appLabelInput = args(i + 1)
        case "-uaIn" => userAppsInput = args(i + 1)
        case "-out" => outputDir = args(i + 1)
        case _ =>
      }
    }

    if (null == appLabelInput || null == userAppsInput || null == outputDir) {
      println("USG: -alIn appLabelInput -uaIn userAppsInput -out outputDir")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("user label calc main")
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    sqlc.setConf("spark.sql.shuffle.partitions", "1024")

    val combineDF = sqlc.read.parquet(appLabelInput)

    /* 利用用户安装的app列表来计算用户的兴趣标签 */
    val userInstallApp = sqlc.read.parquet(userAppsInput)
    val ul = new UserTextLabelCalc(combineDF, userInstallApp)
    val userLabelDF = ul.run()

    userLabelDF.write.mode("overwrite").parquet(outputDir)

    sc.stop()
  }
}
