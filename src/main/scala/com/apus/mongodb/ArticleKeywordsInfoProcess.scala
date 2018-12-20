package com.apus.mongodb

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, TextNode}

/**
  * Created by Joshua on 2018-12-17
  */
object ArticleKeywordsInfoProcess {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ArticleInfoProcess-mongodb")
      .getOrCreate()
    import spark.implicits._

    val dt = "2018-12-17"

    val entitywordsPath = "/user/zhoutong/tmp/NGramDF_articleID_and_keywords%s".format("/dt=2018-12-1[3-4]")
    val savePath = "/user/zoushuai/news_content/writemongo/dt=%s".format(dt)
    val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"

    // 读取匹配实体词
    val entitywords = spark.read.option("basePath", "/user/zhoutong/tmp/NGramDF_articleID_and_keywords").parquet(entitywordsPath).select("article_id", "entity_keywords")

    // 读取新文章
    val getcontentUDF = udf{(html:String) => Jsoup.parse(html).text()}
    val articleAllDF = {
      val seqUDF = udf((t: String) => Seq.empty[String])
      spark.read.option("basePath",newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-12-1[3-4]")
        .selectExpr("resource_id as article_id", "html", "country_lang as country_lan", "category", "title", "url as article_url", "'' as one_level", "'' as two_level", "'' as three_level")
        .withColumn("need_double_check",lit(0))
        .withColumn("semantic_keywords",seqUDF(lit("")))
        .repartition(512)
        .dropDuplicates("html")
        .filter("country_lan = 'IN_en'")
    }
    val articledf = articleAllDF.withColumn("content", getcontentUDF(col("html"))).dropDuplicates("content")
    articledf.cache
    val articleDF = articledf.filter("length(content) > 100")

    // 解析html，text加上apus标签后，再拼接成html
    val tagMarkUDF = udf{
      (html:String,keywords:Seq[String]) =>
        var tag_content = " "+html+" "
        if(keywords.nonEmpty){
          //          Entities.EscapeMode.base.getMap().clear()
          val doc = Jsoup.parse(tag_content)
          val allElements = doc.body().getAllElements.toArray.map(_.asInstanceOf[Element])
          for(i <- allElements){
            val tnList = i.textNodes().toArray().map(_.asInstanceOf[TextNode])
            for(tn <- tnList) {
              var ori_text = " " + tn.text + " "
              keywords.foreach{
                w =>
                  // 有一些特殊的词带有符号的, f***ing n****r tl;dr 等
                  val w_clean = w.map{
                    a=>
                      if("!\"$()*+.[]\\^{}|".contains(a)){
                        "\\" + a
                        //                        "\\\\" + a
                      } else a
                  }.mkString("")
                  // 匹配 '>','<'里面的内容，防止将html标签里的内容替换掉
                  val toReplace_ = "<i class=\"apus-entity-words\"> "+w+" </i>"
                  val toReplace = java.util.regex.Matcher.quoteReplacement(toReplace_)
                  ori_text = ori_text.replaceAll("(?<=[^\\p{L}])("+w_clean+")(?=([^\\p{L}]|‘s|`s|'s))", toReplace)
                //                  ori_text = ori_text.replaceAll("(?<=[^\\p{L}])(?i)("+w_clean+")(?-i)(?=[^\\p{L}])","<i class=\"apus-entity-words\"> "+w+" </i>")
              }
              tn.text(ori_text)
            }
          }
          tag_content = doc.body().children().toString.replace("&lt;i class=\"apus-entity-words\"&gt;","<i class=\"apus-entity-words\">").replace("&lt;/i&gt;","</i>")
          tag_content
        } else {tag_content = html}
        tag_content
    }

    val result = {
      val nullUDF = udf((t: Seq[String]) => if(t != null) t else Seq.empty[String])
      articleDF.join(entitywords, Seq("article_id"))
        .withColumn("entity",nullUDF(col("entity_keywords")))
        .drop("entity_keywords")
        .withColumn("article",tagMarkUDF(col("html"), col("entity")))
        .withColumnRenamed("entity","entity_keywords")
        .drop("html")
    }.toDF().distinct

    // 过滤部分数据
    // 1.内容非英文 2.有实体词但是未在article打上标签的数据（匹配到标题）3.过滤掉未找到关键词
    val result_filter_kw = result.filter(size($"entity_keywords") > 0 && length($"article") > 100)
    val result_filtered = result_filter_kw.filter(!(!$"article".contains("apus-entity-words") && size($"entity_keywords") > 0))

    result_filtered.write.mode(SaveMode.Overwrite).save(savePath)

    // 添加相似去重检查写入文件
    result_filtered.select("article_id", "content").repartition(1).write.format("json").mode(SaveMode.Overwrite).save("news_content/deduplication/dt=2018-11-29")

    // 重新去重写入
    // TODO:需要先计算重复id
    val dupdf = spark.read.json("news_content/dropdups/dropdups.all_17_5")
    val rewritedf = spark.read.parquet(savePath)
    val resavedf = rewritedf.join(dupdf,Seq("article_id"),"left").filter("dupmark is null")
//      .dropDuplicates("title")
    resavedf.write.mode(SaveMode.Overwrite).save(savePath)

  }
}
