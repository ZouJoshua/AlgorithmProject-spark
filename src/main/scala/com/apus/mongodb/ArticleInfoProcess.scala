package com.apus.mongodb

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, Entities, TextNode}



/**
  * Created by Joshua on 2018-11-28
  */

object ArticleInfoProcess {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ArticleInfoProcess-mongodb")
      .getOrCreate()
    import spark.implicits._

    val variables = DBConfig.parseArgs(args)

    val dt = variables.getOrElse("date", DBConfig.today)
    val entitywordsPath = variables.getOrElse("entity_category_path",DBConfig.entitywordsPath + "/dt=2018-11-2[2-6]")
    val unmarkPath = variables.getOrElse("unclassified_path", DBConfig.unclassifiedPath)
    val artPath = variables.getOrElse("article_info_hdfspath", DBConfig.writeArticleInfoPath)
    val articlePath = DBConfig.oriPath
    val mark_level = variables.getOrElse("mark_level", DBConfig.marklevel.toInt)

    val savePath = artPath + "dt=%s".format(dt)

    // 读取匹配实体词(已计算完500w文章的关键词)
    val entitywords = spark.read.option("basePath", DBConfig.entitywordsPath).parquet(entitywordsPath)

    // 读取原始数据(默认22日-26日全部500W+文章)

    val getcontentUDF = udf{(html:String) => Jsoup.parse(html).text()}
    val articleAllDF = {
      spark.read.option("basePath",articlePath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
        .selectExpr("resource_id as article_id","html", "country_lang as country_lan")
        .repartition(512)
        .dropDuplicates("html")
    }
    val articledf = articleAllDF.withColumn("content", getcontentUDF(col("html"))).dropDuplicates("content")
    articledf.cache
    val articleDF = articledf.filter("length(content) > 100")

    // 读取需人工标注数据
    val unmarkDF = spark.read.json(unmarkPath)
    val unmark_id = unmarkDF.select(col("news_id").cast(StringType),col("resource_id"))
    val unmark = {
      val seqUDF = udf((t: String) => Seq.empty[String])
      unmarkDF.withColumn("article_id", concat_ws("", unmark_id.schema.fieldNames.map(col): _*))
        .selectExpr("article_id", "title", "url as article_url", "top_category as one_level", "sub_category as two_level", "third_category as three_level", "length(content) as article_len","length(html) as html_len")
        .withColumn("need_double_check",lit(0))
        .withColumn("semantic_keywords",seqUDF(lit("")))
        .filter("article_len > 100 or html_len > 100") // 增加过滤文章内容长度小于100字符的
    }

    // html加标签 <i class="apus-entity-words">xxx</i> 用正则替换效率低
//    val tagKeywordUDF = udf{
//      (content:String,keywords:Seq[String]) =>
//        var tag_content = " "+content+" "
//        if(keywords.nonEmpty){
//          keywords.foreach{
//            w =>
//              // 有一些特殊的词带有符号的, f***ing n****r tl;dr 等
//              val w_clean = w.map{
//                a=>
//                  if ((a >= 65 && a <= 90) || (a >= 97 && a <= 122) || (a>=48 && a<=57)){
//                    // 字符是大小写字母或数字
//                    a
//                  }else "\\\\"+a
//              }.mkString("")
//              // 匹配 '>','<'里面的内容，防止将html标签里的内容替换掉
//              tag_content = tag_content.replaceAll("(?<=>[^<]{0,1000}[^\\p{L}])(?i)("+w_clean+")(?-i)(?=[^\\p{L}][^>]{0,1000}<)","<i class=\"apus-entity-words\"> "+w+" </i>")
//          }
//        } else {tag_content = content}
//        tag_content
//    }

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
                        "\\\\" + a
                      } else a
                  }.mkString("")
                  // 匹配 '>','<'里面的内容，防止将html标签里的内容替换掉
                  ori_text = ori_text.replaceAll("(?<=[^\\p{L}])(?i)("+w_clean+")(?-i)(?=[^\\p{L}])","<i class=\"apus-entity-words\"> "+w+" </i>")
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
      unmark.drop("article_len", "html_len").join(entitywords, Seq("article_id"))
        .join(articleDF,Seq("article_id"))
        .withColumn("entity",nullUDF(col("entity_keywords")))
        .drop("entity_keywords")
        .withColumn("article",tagMarkUDF(col("html"), col("entity")))
        .withColumnRenamed("entity","entity_keywords")
        .drop("html")
    }.distinct

    // 过滤部分数据
    // 1.内容非英文 2.有实体词但是未在article打上标签的数据（匹配到标题）3.过滤掉未找到关键词
    val result_filter_kw = result.filter(size($"entity_keywords") > 0 && length($"article") > 100)
    val result_filtered = result_filter_kw.filter(!(!$"article".contains("apus-entity-words") && size($"entity_keywords") > 0))

    result_filtered.write.mode(SaveMode.Overwrite).save(savePath)
    // 添加相似去重检查写入文件
    result_filtered.select("article_id", "content").repartition(1).write.format("json").mode(SaveMode.Overwrite).save("news_content/deduplication/dt=2018-11-29")

    // 重新去重写入
    // TODO:需要先计算重复id
    val dupdf = spark.read.json("news_content/dropdups")
    val rewritedf = spark.read.parquet(savePath)
    val resavedf = rewritedf.join(dupdf,Seq("article_id"),"left").filter("dupmark is null").dropDuplicates("title")
    resavedf.write.mode(SaveMode.Overwrite).save(savePath)

    spark.stop()
  }
}
