package com.apus.mark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, TextNode}

/**
  * Created by Joshua on 2019-02-18
  */
object NewsMarchMarkProcess {

  def read_ori_article(spark: SparkSession,
                       articlePath: String,
                       dt:String):DataFrame = {
    val getcontentUDF = udf{(html:String) => Jsoup.parse(html).text()}
    val articleAllDF = {
      spark.read.option("basePath",articlePath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
        .selectExpr("resource_id as article_id","article", "country","lang")
        .withColumn("country_lan",concat_ws("_",col("country"),col("lang")))
        .withColumn("html", col("article.html"))
        .repartition(256)
        .drop("article", "country", "lang")
        .dropDuplicates("article_id", "html")
    }
    val articledf = articleAllDF.withColumn("content", getcontentUDF(col("html"))).dropDuplicates("content")
    articledf.cache
    val articleDF = articledf.filter("length(content) > 100")
    articleDF
  }

  def read_mark_article(spark:SparkSession,
                        markPath1:String,
                        markPath2:String):DataFrame = {
    // 读取需人工标注数据
    val markDF1 = spark.read.json(markPath1).filter("one_level in ('lifestyle','sports','international')").selectExpr("article_id", "url as article_url", "title", "content", "one_level", "sub_category","sub_category_id", "sub_category_proba")
    val markDF2 = spark.read.json(markPath2).filter("one_level in ('lifestyle','sports','international')").select("article_id", "article_url", "title", "content", "one_level", "sub_category","sub_category_id", "sub_category_proba")
    val markDF = markDF1.union(markDF2).distinct()

    val markall = {
      val seqUDF = udf((t: String) => Seq.empty[String])
      val others = Seq("domestic others", "sports others","business others","technology others","auto others","entertainment others")
      val cleanUDF = udf((word: String) => if (word == null || word.replace(" ", "") == "" || others.contains(word)) "others" else word.trim().toLowerCase) // 增加清洗分类
      markDF.selectExpr("article_id", "title", "article_url", "one_level", "sub_category", "sub_category_proba", "length(content) as article_len")
        .withColumn("two_level", cleanUDF(col("sub_category")))
        .withColumn("three_level", lit("others"))
        //        .withColumn("three_level", cleanUDF(col("third_category")))
        .withColumn("need_double_check", lit(0))
        .withColumn("semantic_keywords", seqUDF(lit("")))
        .drop("sub_category")
        .filter("article_id is not null")
        .filter("article_len > 100") // 增加过滤文章内容长度小于100字符的
    }.dropDuplicates("article_id")
    markall
  }

  def read_march_mark_article(spark:SparkSession,
                        markPath:String):DataFrame = {
    // 读取需人工标注数据
    val markDF = spark.read.json(markPath).selectExpr("article_id", "url as article_url", "title", "content", "top_category as one_level", "sub_category as two_level", "sub_category_proba")
    val lifestyle = markDF.filter("one_level = 'lifestyle'").filter("two_level not in ('health','travel')").filter("sub_category_proba < 0.7").select("article_id", "article_url", "title", "content", "one_level", "two_level")
    val business = markDF.filter("one_level = 'business'").filter("two_level not in ('economy','career','stock','market','industry','company','others')").filter("sub_category_proba < 0.6").select("article_id", "article_url", "title", "content", "one_level", "two_level")
    val national = markDF.filter("one_level = 'national'").filter("two_level = 'others'").filter("sub_category_proba < 0.5").select("article_id", "article_url", "title", "content", "one_level", "two_level")
    val auto = markDF.filter("one_level = 'auto'").filter("two_level = 'others'").select("article_id", "article_url", "title", "content", "one_level", "two_level")
    val entertainment = markDF.filter("one_level = 'entertainment'").filter("two_level in ('others')").filter("sub_category_proba < 0.7").select("article_id", "article_url", "title", "content", "one_level", "two_level")
    val tech = markDF.filter("one_level = 'technology'").filter("two_level in ('gadget','others')").filter("sub_category_proba < 0.5").select("article_id", "article_url", "title", "content", "one_level", "two_level")
    val all = lifestyle.union(business).union(national).union(auto).union(entertainment).union(tech)

    val markall = {
      val seqUDF = udf((t: String) => Seq.empty[String])
      all.selectExpr("article_id", "title", "article_url", "one_level", "two_level", "length(content) as article_len")
        .withColumn("three_level", lit("others"))
        .withColumn("need_double_check", lit(0))
        .withColumn("semantic_keywords", seqUDF(lit("")))
        .filter("article_id is not null")
        .filter("article_len > 100") // 增加过滤文章内容长度小于100字符的
    }.dropDuplicates("article_id")
    markall
  }

  def mark_html_with_entitykeywords(articleDF: DataFrame,
                                    mark: DataFrame,
                                    entitywords: DataFrame): DataFrame = {
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
                  ori_text = ori_text.replaceAll("(?<=[^\\p{L}])("+w_clean+")(?=([^\\p{L}])|‘s|`s|'s)", toReplace)
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
      mark.drop("article_len").join(entitywords, Seq("article_id"))
        .join(articleDF,Seq("article_id"))
        .withColumn("entity", nullUDF(col("entity_keywords")))
        .drop("entity_keywords")
        .withColumnRenamed("html","article")
//        .withColumn("article", tagMarkUDF(col("html"), col("entity")))
        .withColumnRenamed("entity","entity_keywords")
        //.drop("html")
    }.distinct
    result
  }
  def dfZipWithIndex(df: DataFrame,
                     offset: Int = 1,
                     colName: String = "id",
                     inFront: Boolean = true): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(
          (if (inFront) Seq(ln._2 + offset) else Seq())
            ++ ln._1.toSeq ++
            (if (inFront) Seq() else Seq(ln._2 + offset))
        )
      ),
      StructType(
        (if (inFront) Array(StructField(colName,LongType,false)) else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]() else Array(StructField(colName,LongType,false)))
      )
    )
  }


  def main(args: Array[String]): Unit = {
    val appName = "NewsMarkProcesser"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val dt = "2019-02-18"
    val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
    val entitywordsPath1 = "/user/zhoutong/tmp/NGramDF_articleID_and_keywords%s".format("/dt=2018-11-2[2-6]")
    val entitywordsPath2 = "/user/hive/warehouse/apus_ai.db/recommend/article/article_profile/dt=2019-01-*"
    val markPath1 = "news_sub_classification/predict/predict_history_unmark_res"
    val markPath2 = "news_sub_classification/predict/predict_lifestyle_world_res"
    val tmpSavePath = "/user/zoushuai/news_content/writemongo/tmp/dt=%s".format(dt)
    val savePath = "/user/zoushuai/news_content/writemongo/March_mark"
    val dupsPath = "news_content/dropdups/dropdups.all_150_5"

    //三月剩余30w标注
    val marchMarkPath = "news_sub_classification/predict/predict_unmark_part*"
    val marchSavePath = "/user/zoushuai/news_content/writemongo/March_mark_last"


    //------------------------------------1 读取原始文章-----------------------------------------
    //
    val articleDF = read_ori_article(spark, newsPath, dt)
    //------------------------------------2 处理标注文章-----------------------------------------
    //
    // 三月标注前10w
    // val markDF = read_mark_article(spark, markPath1, markPath2)
    // 三月标注后30w
    val markDF = read_march_mark_article(spark, marchMarkPath)



    //------------------------------------3 标注关键词并-----------------------------------------
    //
    val entitywordsDF1 = spark.read.parquet(entitywordsPath1).select("article_id", "entity_keywords")
    val entitywordsDF2 = spark.read.parquet(entitywordsPath2).selectExpr("id as article_id", "keywords.word as entity_keywords")

    val entitywordsDF = entitywordsDF1.union(entitywordsDF2).distinct()

    val result = mark_html_with_entitykeywords(articleDF, markDF, entitywordsDF)
    // 过滤部分不符合条件的数据
    val result_filter_kw = result.filter(size($"entity_keywords") > 0 && length($"article") > 100)
    val result_filtered = result_filter_kw.filter(!(!$"article".contains("apus-entity-words") && size($"entity_keywords") > 0))

    //------------------------------------4 去重并保存结果-----------------------------------------
    // 去重
    val dupdf = spark.read.json(dupsPath)
    val resavedf = result_filtered.join(dupdf,Seq("article_id"),"left").filter("dupmark is null").dropDuplicates("title")

    resavedf.write.mode(SaveMode.Overwrite).save(savePath)

  }
}
