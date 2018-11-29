package com.apus.mongodb

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
  * Created by Joshua on 2018-11-28
  */

object DataProcess {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataProcess-mongodb")
      .getOrCreate()
    import spark.implicits._

    val dt = "2018-11-28"
    val entitywordsPath = "/user/zhoutong/tmp/entity_and_category"
    val unmarkPath = "/user/caifuli/news/tmp/unclassified"
    val savePath = "/user/zoushuai/news_content/writemongo/dt=%s".format(dt)
    val articlePath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"

    // 读取匹配实体词
    val entitywords = spark.read.parquet(entitywordsPath)

    // 读取所有文章
    val articleDF = {
      spark.read.option("basePath",articlePath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
        .selectExpr("resource_id as article_id","html", "country_lang as country_lan")
        .repartition(512)
        .dropDuplicates("html")
    }

    // 读取需人工标注数据
    val unmarkDF = spark.read.json(unmarkPath)
    val unmark_id = unmarkDF.select(col("news_id").cast(StringType),col("resource_id"))
    val unmark = {
      val seqUDF = udf((t: String) => Seq.empty[String])
      unmarkDF.withColumn("article_id", concat_ws("", unmark_id.schema.fieldNames.map(col): _*))
        .selectExpr("article_id", "title","url as article_url", "top_category as one_level", "sub_category as two_level", "third_category as three_level")
        .withColumn("need_double_check",lit(0))
        .withColumn("mark_level", lit(2))
        .withColumn("semantic_keywords",seqUDF(lit(" ")))
    }

    // html加标签 <i class="apus-entity-words">xxx</i>
    val tagKeywordUDF = udf{
      (content:String,keywords:Seq[String]) =>
        var tag_content = " "+content+" "
        if(keywords.nonEmpty){
          keywords.foreach{
            w =>
              // 有一些特殊的词带有符号的, f***ing n****r tl;dr 等
              val w_clean = w.map{
                a=>
                  if ((a >= 65 && a <= 90) || (a >= 97 && a <= 122) || (a>=48 && a<=57)){
                    // 字符是大小写字母或数字
                    a
                  }else "\\\\"+a
              }.mkString("")
              tag_content = tag_content.replaceAll("(?<=[^\\p{L}])(?i)("+w_clean+")(?-i)(?=[^\\p{L}])","<i class=\"apus-entity-words\"> "+w+" </i>")
          }
        } else {tag_content = content}
        tag_content
    }

//    val tagUDF = udf{
//      (article:String, words:Seq[String]) =>
//        var tmp_art = " " + article + " "
//        words.foreach{
//          w =>
//            tmp_art = tmp_art.replaceAll("\\s(?i)" + w + "(?=\\s)", " <i class=\"apus-entity-words\">" + w + "</i> ")
//        }
//        tmp_art
//    }

    // 存储结果

//    val result = {
//      entitywords.join(unmark, Seq("article_id"))
//        .join(articleDF,Seq("article_id"))
//        .withColumn("article",when(col("entity_keywords").isNotNull, tagKeywordUDF(col("html"), col("entity_keywords"))).otherwise(col("html")))
//        .drop("html")
//    }
    val result = {
      val nullUDF = udf((t: Seq[String]) => if(t != null) t else Seq.empty[String])
      entitywords.join(unmark, Seq("article_id"))
        .join(articleDF,Seq("article_id"))
        .withColumn("entity",nullUDF(col("entity_keywords")))
        .drop("entity_keywords")
        .withColumn("article",tagKeywordUDF(col("html"), col("entity")))
        .withColumnRenamed("entity","entity_keywords")
        .drop("html")
    }.distinct
    // 过滤部分数据
    // 1.内容非英文 2.有实体词但是未在article打上标签的数据（匹配到标题）3.过滤掉未找到关键词
//    result.filter(!$"article".contains("apus-entity-words")).filter(size($"entity_keywords") > 0)
    val result_filter_kw = result.filter(size($"entity_keywords") > 0 && length($"article") > 100)
    val result_filtered = result_filter_kw.filter(!(!$"article".contains("apus-entity-words") && size($"entity_keywords") > 0))

    result_filtered.write.mode(SaveMode.Overwrite).save(savePath)
    spark.stop()
  }
}
