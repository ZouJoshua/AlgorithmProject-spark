package com.privyalgo.label.dataextractor

import java.net.{URI, URISyntaxException}

import com.alibaba.fastjson.JSON
import com.google.common.net.InternetDomainName
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by caifuli on 17-7-24.
  */
class URLandSearchwordExtractor (
                                  data: DataFrame,
                                  sqlc: HiveContext
                                )extends Serializable{
  import sqlc.implicits._
  def runBodensee(): DataFrame = {
    val resDF = data.map{ r =>

      var url:String = ""
      var searchword:String = ""
      var site: String = ""

      val client_id = r.getAs[String]("client_id")
      val eventjson = r.getAs[String]("event")

      val urlKey = JSON.parseObject(eventjson).get("2").toString
      if (urlKey == "12000") {
        val urlJson = JSON.parseObject(JSON.parseObject(eventjson).get("3").toString)
        val url_value = urlJson.get("1").toString
        if (url_value == "101") {
          if(urlJson.containsKey("1000")){
            url = urlJson.get("1000").toString
            site = filterSLD(filterIP(extractRoot(url)))
          }
        }
        if (url_value == "100") {
          if(urlJson.containsKey("2")){
            searchword = urlJson.get("2").toString
          }
        }
      }

      (client_id, site, searchword)
    }.toDF("client_id","site","searchword")

    resDF
  }

  def runXAL(): DataFrame = {
    val res = data.map{r =>
      val client_id = r.getAs[String]("client_id_s")
      val url = r.getAs[String]("record__value__url_s")
      var site:String = ""
      try{
        site = filterSLD(filterIP(extractRoot(url)))
      }catch{
        case _: Exception =>
      }
      val searchword = r.getAs[String]("record__value__query_s")

      (client_id, site, searchword)
    }.toDF("client_id","site","searchword")
    res
  }

  //获取站点名
  def extractRoot(url: String): String = {
    val p = "https?://[^/]+".r
    val u = p findAllIn (url) toList
    var res: String = ""
    if (!(u.isEmpty)) {
      res = u(0)
    }
    res
  }

  //过滤掉ip类地址
  def filterIP(url:String):String = {
    val p = "https?://([0-9]{1,3}\\.){3}[0-9]{1,3}(:[0-9]{1,4}){0,1}".r
    var res:String = ""
    val u = p findAllIn (url) toList;
    if(u.isEmpty){
      res = url
    }else{
      for(i <- u){
        res = url.replaceAll(i,"")
      }
    }
    res
  }

  def filterSLD(url:String):String = {

    try{
      val uri = new URI(url)
      val domain = uri.getHost()
      val protocol = url.split("\\/\\/")(0)

      //val name_list = domain.split("\\.")
      val sld = InternetDomainName.from(domain).topPrivateDomain().toString
      val sld_part = sld.split("=")(1)
      val sld_res = sld_part.replaceAll("}","")
      val res = protocol + "//" + sld_res

      res
    }catch{
      case e:  IllegalStateException => {
        println("IllegalState")
        ""
      }
      case e: NullPointerException =>{
        println("NullPointer")
        ""
      }
      case e:URISyntaxException => {
        println("URISyntaxException")
        ""
      }
      case e:IllegalArgumentException =>{
        println("IllegalArgumentException")
        ""
      }
    }
  }

}
