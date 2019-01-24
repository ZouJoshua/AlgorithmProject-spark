package com.apus.nlp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, Entities, TextNode}
/**
  * Created by Joshua on 2019-01-23
  */
object NewsMarkAllProcess {

  def main(args: Array[String]): Unit = {

    val appName = "News-SubCategory-MarkCorpus-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext


    //------------------------------------ 需标注的文章处理-----------------------------------------
    //
    def mark_process(spark: SparkSession) = {

      val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"
      val markPath = "/user/caifuli/news/all_data/*_unclassified"
      val entitywordsPath = "/user/zhoutong/tmp/NGramDF_articleID_and_keywords%s".format("/dt=2018-11-2[2-6]")
      val dupsPath = "news_content/dropdups/dropdups.all_150_5"

      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val oriDF = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "country_lang as country_lan", "url as article_url", "title", "html")
          .withColumn("content", getcontentUDF(col("html")))
          .dropDuplicates("article_id", "content", "title")
          .filter("length(content) > 100")
      }

      val markDF = spark.read.json(markPath)
      val markID = markDF.select(col("news_id").cast(StringType), col("resource_id"))
      val markAll = {
        val seqUDF = udf((t: String) => Seq.empty[String])
        val cleanTopUDF = udf{(word: String) => if (word == null || word.replace(" ", "") == "") "others" else word.trim().toLowerCase.replace("sports", "sport").replace("sport","sports").replace("world","international").replace("aoto","auto").replace("technology","tech").replace("tech", "technology")}
        val cleanUDF = udf((level: String) => if (level == null || level.replace(" ", "") == "") "others" else level.trim().toLowerCase) // 增加清洗分类
        markDF.withColumn("article_id", concat_ws("", markID.schema.fieldNames.map(col): _*))
          .selectExpr("article_id", "top_category", "sub_category", "third_category", "length(content) as article_len")
          .withColumn("one_level", cleanTopUDF(col("top_category")))
          .withColumn("two_level", cleanUDF(col("sub_category")))
          .withColumn("three_level", cleanUDF(col("third_category")))
          .withColumn("need_double_check", lit(0))
          .withColumn("semantic_keywords", seqUDF(lit("")))
          .drop("top_category", "sub_category", "third_category")
          .filter("article_len > 100") // 增加过滤文章内容长度小于100字符的
      }.dropDuplicates("article_id")


      // 解析html，text加上apus标签后，再拼接成html
      val tagMarkUDF = udf {
        (html: String, keywords: Seq[String]) =>
          var tag_content = " " + html + " "
          if (keywords.nonEmpty) {
            //          Entities.EscapeMode.base.getMap().clear()
            val doc = Jsoup.parse(tag_content)
            val allElements = doc.body().getAllElements.toArray.map(_.asInstanceOf[Element])
            for (i <- allElements) {
              val tnList = i.textNodes().toArray().map(_.asInstanceOf[TextNode])
              for (tn <- tnList) {
                var ori_text = " " + tn.text + " "
                keywords.foreach {
                  w =>
                    // 有一些特殊的词带有符号的, f***ing n****r tl;dr 等
                    val w_clean = w.map {
                      a =>
                        if ("!\"$()*+.[]\\^{}|".contains(a)) {
                          "\\" + a
                          //                        "\\\\" + a
                        } else a
                    }.mkString("")
                    // 匹配 '>','<'里面的内容，防止将html标签里的内容替换掉
                    val toReplace_ = "<i class=\"apus-entity-words\"> " + w + " </i>"
                    val toReplace = java.util.regex.Matcher.quoteReplacement(toReplace_)
                    ori_text = ori_text.replaceAll("(?<=[^\\p{L}])(" + w_clean + ")(?=([^\\p{L}])|‘s|`s|'s)", toReplace)
                  //                  ori_text = ori_text.replaceAll("(?<=[^\\p{L}])(?i)("+w_clean+")(?-i)(?=[^\\p{L}])","<i class=\"apus-entity-words\"> "+w+" </i>")
                }
                tn.text(ori_text)
              }
            }
            tag_content = doc.body().children().toString.replace("&lt;i class=\"apus-entity-words\"&gt;", "<i class=\"apus-entity-words\">").replace("&lt;/i&gt;", "</i>")
            tag_content
          } else {
            tag_content = html
          }
          tag_content
      }

      val entitywordsDF = spark.read.parquet(entitywordsPath).select("article_id", "entity_keywords")

      val result = {
        val nullUDF = udf((t: Seq[String]) => if (t != null) t else Seq.empty[String])
        markAll.drop("article_len", "html_len").join(entitywordsDF, Seq("article_id"))
          .join(oriDF, Seq("article_id"))
          .withColumn("entity", nullUDF(col("entity_keywords")))
          .drop("entity_keywords")
          .withColumn("article", tagMarkUDF(col("html"), col("entity")))
          .withColumnRenamed("entity", "entity_keywords")
          .drop("html")
      }.distinct

      // 过滤部分不符合条件的数据
      val result_filter_kw = result.filter(size($"entity_keywords") > 0 && length($"article") > 100)
      val result_filtered = result_filter_kw.filter(!(!$"article".contains("apus-entity-words") && size($"entity_keywords") > 0))

      val dupdf = spark.read.json(dupsPath)
      val resavedf = result_filtered.join(dupdf,Seq("article_id"),"left").filter("dupmark is null").drop("dupmark")
      resavedf.write.mode("overwrite").save("news_sub_classification/markCorpus/unmark_all")

    }
  }
}
