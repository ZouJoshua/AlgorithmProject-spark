package com.privyalgo.label.driver

import com.privyalgo.label.dataextractor.URLandSearchwordExtractor
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by caifuli on 17-7-24.
  */
object URLandSearchwordExtractorDriver {
  def main(args:Array[String]):Unit = {

    var startTime: String = null
    var endTime: String = null
    var outputDir: String = null
    val len = args.length

    for (i <- 0 until len) {
      val myVar = args(i)
      myVar match {
        case "-st" => startTime = args(i + 1)
        case "-et" => endTime = args(i + 1)
        case "-out" => outputDir = args(i + 1)
        case _ =>
      }
    }

    if (null == startTime || null == endTime ||null == outputDir) {
      println("USG: -st startTime -et endTime -out outputDir")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("url and searchword extractor")
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)
    sqlc.setConf("spark.sql.shuffle.partitions", "2400")

    val dateBodenseeDF = sqlc.sql(s"select * from apuscommon.bodensee where dt >= '$startTime' and dt <= '$endTime'")
    dateBodenseeDF.registerTempTable("table")
    val inputBodenseeDF = sqlc.sql("select get_json_object(device_info,'$.1') as client_id,event from table lateral view explode(events) tmp as event where get_json_object(event,'$.2') in ('12000')")
    val dataBodensee = new URLandSearchwordExtractor(inputBodenseeDF, sqlc).runBodensee()

    val inputXALDF = sqlc.read.options(Map("basePath" -> "/user/hive/warehouse/apuscommon.db/xal_basic",
    "mergeSchema" -> "true"))
      .parquet("/user/hive/warehouse/apuscommon.db/xal_basic/dt=2017-*/pn=browser")
      .where(s"dt>'$startTime' and dt<'$endTime'")
    val dataXAL = new URLandSearchwordExtractor(inputXALDF, sqlc).runXAL()

    val data = dataBodensee.unionAll(dataXAL)
    data.write.mode("overwrite").parquet(outputDir)

  }

}
