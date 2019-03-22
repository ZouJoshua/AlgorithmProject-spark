package com.privyalgo.mongodb

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, TextNode}


/**
  * Created by Joshua on 2019-03-15
  */
object CMS2MarkTestDataV1 {
  def main(args: Array[String]): Unit = {
    val appName = "CMSmark2-Test-Data"
    val spark = SparkSession.builder()
      .master("local")
      .appName("ReadMongoSparkConnector")
      .getOrCreate()
    val sc = spark.sparkContext

    val profilePath = "/user/hive/warehouse/apus_dm.db/recommend/article/streaming_files/article_profile_streaming_byRequest"
    val df = spark.read.parquet(profilePath)
    val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*"
    val profile = df.selectExpr("id as resource_id", "algo_profile","dt")
    val ori_df = spark.read.parquet(newsPath).dropDuplicates("resource_id").drop("id","streamingParseTime")
    val tmp_result = profile.join(ori_df,Seq("resource_id"))
    tmp_result.write.mode("overwrite").save("news_content/cms2marktest")

    val  keywordType = {
      new StructType()
        .add("score",DoubleType)
        .add("word",StringType)
    }
    val topicType = {
      new StructType()
        .add("topic",IntegerType)
        .add("weight",DoubleType)
    }

    val categoryType = {
      new StructType()
        .add("top_category",StringType)
        .add("top_category_id", IntegerType)
        .add("top_category_proba",DoubleType)
        .add("sub_category",StringType)
        .add("sub_category_id", IntegerType)
        .add("sub_category_proba",DoubleType)
    }

    val nlp_keywordType = {
      new StructType()
        .add("weight",DoubleType)
        .add("keyword",StringType)
    }

    val algo_profileSchema =
      StructType(Seq(
        StructField("review_status",ArrayType(IntegerType)),
        StructField("textlang", StringType),
        StructField("category",categoryType),
        StructField("status",StringType),
        StructField("tags", ArrayType(StructType(Seq(StructField("weight", DoubleType),StructField("keyword", StringType)
        )))),
        StructField("title_keywords", ArrayType(StructType(Seq(StructField("weight", DoubleType),StructField("keyword", StringType)
        )))),
        StructField("content_keywords", ArrayType(StructType(Seq(StructField("weight", DoubleType), StructField("keyword", StringType)
        )))),
        StructField("topic_64",ArrayType(topicType)),
        StructField("topic_128",ArrayType(topicType)),
        StructField("topic_256",ArrayType(topicType)),
        StructField("topic_512",ArrayType(topicType)),
        StructField("topic_1000",ArrayType(topicType))
      ))

    val accountType = {
      new StructType()
        .add("id",LongType)
        .add("name", StringType)
    }

    val image_originType = {
      new StructType()
        .add("size",IntegerType)
        .add("width",IntegerType)
        .add("url",StringType)
        .add("height",IntegerType)
    }

    val imagesType = {
      new StructType()
        .add("origin",image_originType)
        .add("detail_large",image_originType)
        .add("list_large",image_originType)
        .add("list_small",image_originType)
    }

    val share_linkType = {
      new StructType()
        .add("1", StringType)
        .add("2", StringType)
        .add("4", StringType)
        .add("10", StringType)
    }

    val extraType = {
      new StructType()
        .add("id",IntegerType)
        .add("image_source", ArrayType(StringType))
    }

    // 解析html，text加上apus标签后，再拼接成html
    val tagMarkUDF1 = udf{
      (html:String,keywords: Seq[String]) =>
        var tag_content = " "+html+" "
        if(html!=null&&html!=""&&keywords.nonEmpty){
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


    val saveDf = {
      spark.read.parquet("news_content/cms2marktest")
        .withColumn("nlp_keywords_new", from_json(col("nlp_keywords"),ArrayType(nlp_keywordType)))
        .withColumn("account_new",from_json(col("account"), accountType))
        .withColumn("images_new",from_json(col("images"), ArrayType(imagesType)))
        .withColumn("algo_profile_new",from_json(col("algo_profile"),algo_profileSchema))
        .withColumn("share_link_new", from_json(col("share_link"), share_linkType))
        .withColumn("extra_new", from_json(col("extra"), extraType))
        .selectExpr("inner_type", "rqstid","url","title","country","lang","source","summary","introduction",
        "generate_url","account_new as account","resource_id","article","dt",
        "algo_profile_new as algo_profile","images_new as images",
        "repeat_content","nlp_keywords_new as nlp_keywords",
        "share_link_new as share_link", "extra_new as extra",
        "cast(category as int) category", "cast(sub_class as int) sub_class",
        "cast(sub_category as int) sub_category","cast(third_category as int) third_category",
        "cast(create_time as long) create_time","cast(pub_time as long) pub_time",
        "cast(words as int) words", "cast(image_count as int) image_count",
        "cast(thumbnail_count as int) thumbnail_count","cast(status as int) status",
        "cast(news_type as int) news_type", "cast(latest_interest as int) latest_interest",
        "cast(expire_time as long) expire_time", "cast(resource_type as int) resource_type",
        "cast(is_hotnews as int) is_hotnews","cast(is_newarticle as int) is_newarticle",
        "cast(is_partner as int) is_partner", "cast(origin_category as int) origin_category",
        "cast(source_id as long) source_id")
    }

    val nullUDF = udf((t: Seq[String]) => if(t != null) t else Seq.empty[String])
    val article_process = saveDf.select("resource_id","article.html","algo_profile.content_keywords.keyword").withColumn("keywords",nullUDF(col("keyword"))).drop("keyword")
    val article_new = article_process.withColumn("html_with_tag", when(col("html").isNotNull&&col("html") != "", tagMarkUDF1(col("html"),col("keywords")))).drop("keywords","html")

    val result = saveDf.join(article_new, Seq("resource_id")).withColumnRenamed("article", "article_new")
    val articleStructCols = result.select("article_new.*").columns.filter(_!="html").map(name => col("article_new."+name))
//    val algo_profileStructCols = result.select("algo_profile_new.*").columns.map(name => col("algo_profile_new."+name))

    val result_new = {
      result
        .withColumn("article", struct((articleStructCols:+col("html_with_tag").as("html")):_*)).drop("article_new","algo_profile_new","html_with_tag")
    }

    result_new.coalesce(1).write.mode("overwrite").save("news_content/writemongo/cms2marktest")
    val out = result_new.withColumn("buss_type", lit(0)).withColumn("batch_id",lit(2019031801))






  }
}
