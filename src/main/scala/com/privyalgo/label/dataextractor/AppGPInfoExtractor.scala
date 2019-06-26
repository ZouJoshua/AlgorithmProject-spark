package com.privyalgo.label.dataextractor

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by caifuli on 17-8-15.
  */
class AppGPInfoExtractor (
                         text: RDD[String],
                         sqlc: HiveContext
                         )extends Serializable{
  import sqlc.implicits._
  def run(): DataFrame = {

    val res = text.map(line => {
      val strJson = JSON.parseObject(line)
      val descStr = strJson.get("desc_info").toString
      val pnStr = strJson.get("pagename").toString
      val download = strJson.get("download_info").toString
      var min_download: Long = 0
      var max_download: Long = 0
      var len: Int = 0
      if(download != ""){
        val downloadArr = download.split("\\-")
        len = downloadArr.length
        try{
          min_download = downloadArr(0).trim.replace(",", "").replace(" ","") .toLong
          max_download = downloadArr(1).trim.replace(",", "").replace(" ","").toLong
        }catch{
          case _: Exception =>
        }

      }

      val class_desc = strJson.get("class_desc").toString
      val orig_lang = strJson.get("orig_lang").toString
      val real_lang = strJson.get("real_lang").toString
      val name = strJson.get("name").toString
      val grade = strJson.get("grade").toString
      val utime = strJson.get("utime").toString.toLong
      val ctime = strJson.get("ctime").toString.toLong
      val update_info = strJson.get("update_info").toString
      val id = strJson.get("id").toString.toLong
      val dt = "2017-06-20"

      (pnStr, orig_lang, real_lang, name, grade, id, class_desc, min_download, max_download,utime, ctime, update_info, descStr, dt)
    }).toDF("pn", "orig_lang","real_lang", "name","grade","id","class_desc","min_download", "max_download","utime","ctime","update_info","desc_info","dt")

    res
  }

}
