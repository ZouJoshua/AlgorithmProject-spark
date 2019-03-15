package com.privyalgo.mongodb

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, TextNode}


/**
  * Created by Joshua on 2019-03-08
  */


object CMS2MarkTestData {


  def main(args: Array[String]): Unit = {
    val appName = "CMSmark2-Test-Data"
    val spark = SparkSession.builder()
      .master("local")
      .appName("ReadMongoSparkConnector")
      .getOrCreate()
    val sc = spark.sparkContext


    //------------------------------------ 处理测试数据-----------------------------------------
    //
//    val profilePath = "news_content/cmsmark2test"
//    val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*"
//    val profile = spark.read.parquet(profilePath).selectExpr("id as resource_id", "algo_profile")
//    val ori_df = spark.read.parquet(newsPath)
//    val result = ori_df.drop("id","streamingParseTime").join(profile,Seq("resource_id")).withColumn("buss_type", lit(1))
//    result.coalesce(1).write.save("tmp/cms2marktestdata")

    //------------------------------------ 写入mongodb-----------------------------------------
    //

    val dt = "2019-03-08"
//    val inputPath = "/user/zoushuai/news_content/cms2marktest"
    val inputPath = "/user/zoushuai/news_content/cms2marktestdata"
    val outputUri = "mongodb://127.0.0.1:27017/news.article_info_doc"

    val  keywordType = {
      new StructType()
        .add("score",DoubleType)
        .add("word",StringType)
    }
    val algo_profileType = {
      new StructType()
        .add("review_status",ArrayType(IntegerType))
        .add("category",StringType)
        .add("sub_category", StringType)
        .add("status",StringType)
        .add("title_keywords",ArrayType(keywordType))
        .add("content_keywords",ArrayType(keywordType))
    }


    val structSchema =
      StructType(Seq(
        StructField("review_status",ArrayType(IntegerType)),
        StructField("category",StringType),
        StructField("sub_category",StringType),
        StructField("status",StringType),
        StructField("tags", ArrayType(StructType(Seq(StructField("weight", DoubleType),StructField("keyword", StringType)
        )))),
        StructField("title_keywords", ArrayType(StructType(Seq(StructField("weight", DoubleType),StructField("keyword", StringType)
        )))),
        StructField("content_keywords", ArrayType(StructType(Seq(StructField("weight", DoubleType), StructField("keyword", StringType)
        ))))
      ))


    val nlp_keywordType = {
      new StructType()
        .add("weight",DoubleType)
        .add("keyword",StringType)
    }

    val accountType = {
      new StructType()
        .add("id",LongType)
        .add("name", StringType)
    }

    val originType = {
      new StructType()
        .add("size",DoubleType)
        .add("width",DoubleType)
        .add("url",StringType)
        .add("height",DoubleType)
    }

    val imagesType = {
      new StructType()
        .add("origin",originType)
    }

    val share_linkType = {
      new StructType()
        .add("1", StringType)
        .add("2", StringType)
        .add("4", StringType)
    }

    val extraType = {
      new StructType()
        .add("_id",StringType)
    }


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


    val saveDf = {
      spark.read.parquet(inputPath)
        .withColumn("nlp_keywords_new", from_json(col("nlp_keywords"),ArrayType(nlp_keywordType)))
        .withColumn("account_new",from_json(col("account"), accountType))
        .withColumn("images_new",from_json(col("images"), ArrayType(imagesType)))
        .withColumn("algo_profile_new",from_json(col("algo_profile"),algo_profileType))
        .withColumn("share_link_new", from_json(col("share_link"), share_linkType))
        .withColumn("buss_type", lit(0))
//        .withColumn("extra_new", from_json(col("extra"), extraType))
        .selectExpr("inner_type", "rqstid","url","title","country","lang","source","summary","introduction","buss_type",
          "generate_url","account","resource_id","article",
          "algo_profile_new as algo_profile","images_new as images",
          "repeat_content","nlp_keywords_new as nlp_keywords",
          "share_link_new as share_link", "extra",
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

    val article_process = saveDf.select("resource_id","article.html","algo_profile.content_keywords.word")
    val article_new = article_process.withColumn("html_with_tag", tagMarkUDF(col("html"),col("word"))).drop("word","html")

    val result = saveDf.join(article_new, Seq("resource_id")).withColumnRenamed("article", "article_new").withColumnRenamed("algo_profile","algo_profile_new")
    val articleStructCols = result.select("article_new.*").columns.filter(_!="html").map(name => col("article_new."+name))
    val algo_profileStructCols = result.select("algo_profile_new.*").columns.map(name => col("algo_profile_new."+name))

    val result_new = {
      result.withColumn("algo_profile",struct((algo_profileStructCols:+col("algo_profile_new.title_keywords").as("tags")):_*))
          .withColumn("article", struct((articleStructCols:+col("html_with_tag").as("html")):_*)).drop("article_new","algo_profile_new","html_with_tag")
        }

    val profileDf = result_new.select(col("algo_profile").cast(structSchema),col("resource_id"))

    val out = result_new.drop("algo_profile").join(profileDf, Seq("resource_id"))
    print(out.printSchema())
    val num = out.count
    // 写入mongodb
    out.write.options(Map("spark.mongodb.output.uri" -> outputUri))
//      .mode("append")
            .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    println("\nSuccessfully write %1$s data to mongodb".format(num))
    spark.stop()

  }
}
