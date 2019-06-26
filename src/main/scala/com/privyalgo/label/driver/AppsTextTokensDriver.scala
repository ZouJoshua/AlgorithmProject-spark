package com.privyalgo.label.driver

import com.privyalgo.label.dataextractor.AppGPInfoExtractor
import com.privyalgo.label.lexicalanalyzer.{AppsTextCoreNLPTokens, FormTokenDF}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * app正文描述信息分词程序的drive类
  * Created by wuxiushan on 17-4-28.
  */
object AppsTextTokensDriver {
  def main(args: Array[String]): Unit = {
    var textInputDir: String = null    //JSON形式的文本输入路径
    var textDFInputDir: String = null  //DF形式的GP数据存储路径
    var stopwordsInputDir: String = null
    var outputDir: String = null
    var tfThres: String = null
    val len = args.length

    for (i <- 0 until len) {
      val myVar = args(i)
      myVar match {
        case "-tin" => textInputDir = args(i + 1)
        case "-swin" => stopwordsInputDir = args(i + 1)
        case "-tdfin" => textDFInputDir = args(i + 1)
        case "-tf" => tfThres = args(i + 1)
        case "-out" => outputDir = args(i + 1)
        case _ =>
      }
    }

    if (null == textInputDir || null == stopwordsInputDir || null == textDFInputDir ||null == outputDir || null == tfThres) {
      println("USG: -tin textInputDir -swin stopwordsInputDir -tdfin textDFInputDir -tf tfThres -out outputDir")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("apps text CoreNLP Tokens")
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    sqlc.setConf("spark.sql.shuffle.partitions", "1024")

    /*val text = sc.textFile(textInputDir).repartition(1024)
    val textDF = new AppGPInfoExtractor(text, sqlc).run()
    textDF.write.mode("overwrite").parquet(textDFInputDir)*/
    val textDF = sqlc.read.parquet(textDFInputDir)

    val stop = sc.textFile(stopwordsInputDir)
    val wordArr = stop.collect()
    var stopwordsSet: Set[String] = Set[String]()
    for (word <- wordArr) {
      stopwordsSet += word
    }
    val bcStopWords = sc.broadcast(stopwordsSet)

    val att = new AppsTextCoreNLPTokens(textDF, bcStopWords, tfThres)    //todo:20170620的app数据的tfThres的阈值设为3，是因为有一部分脏数据
    val tokenRes = att.run()
    val tokenDF = new FormTokenDF(tokenRes, sqlc).run()

    tokenDF.write.mode("overwrite").parquet(outputDir)

    sc.stop()
  }
}
