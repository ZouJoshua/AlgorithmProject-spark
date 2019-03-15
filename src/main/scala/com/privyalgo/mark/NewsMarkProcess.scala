package com.privyalgo.mark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, TextNode}

/**
  * Created by Joshua on 2019-01-08
  */
object NewsMarkProcess {

  def read_ori_article(spark: SparkSession,
                       articlePath: String,
                       dt:String):DataFrame = {
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
    articleDF
  }

  def read_mark_article(spark:SparkSession,
                        markPath:String):DataFrame = {
    // 读取需人工标注数据
    val markDF = spark.read.json(markPath)

    val mark_id = markDF.select(col("news_id").cast(StringType),col("resource_id"))
    val markall = {
      val seqUDF = udf((t: String) => Seq.empty[String])
      val cleanUDF = udf((level: String) => if (level == null || level.replace(" ", "") == "") "others" else level.trim().toLowerCase) // 增加清洗分类
      markDF.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
        .selectExpr("article_id", "title", "url as article_url", "top_category", "sub_category", "third_category", "length(content) as article_len", "length(html) as html_len")
        .withColumn("one_level", cleanUDF(col("top_category")))
        .withColumn("two_level", cleanUDF(col("sub_category")))
        .withColumn("three_level", lit("others"))
        //        .withColumn("three_level", cleanUDF(col("third_category")))
        .withColumn("need_double_check", lit(0))
        .withColumn("semantic_keywords", seqUDF(lit("")))
        .drop("top_category", "sub_category", "third_category")
        .filter("article_len > 100 or html_len > 100") // 增加过滤文章内容长度小于100字符的
    }.dropDuplicates("article_id")
    markall
  }

  def read_lifestyle_mark_article(spark:SparkSession,
                                  markPath:String):DataFrame = {
    // 读取需人工标注数据
    val markDF = spark.read.json(markPath)

    val mark_id = markDF.select(col("news_id").cast(StringType),col("resource_id"))
    val markall = {
      val seqUDF = udf((t: String) => Seq.empty[String])
      val cleanUDF = udf((level: String) => if(level == null || level.replace(" ","") == "") "others" else level.trim().toLowerCase.replace(" ",""))// 增加清洗分类
      markDF.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
        .selectExpr("article_id", "title", "url as article_url", "top_category", "sub_category", "third_category", "length(content) as article_len","length(html) as html_len")
        .withColumn("one_level", cleanUDF(col("top_category")))
        .withColumn("two_level", cleanUDF(col("sub_category")))
        .withColumn("three_level", lit("others"))
        //        .withColumn("three_level", cleanUDF(col("third_category")))
        .withColumn("need_double_check",lit(0))
        .withColumn("semantic_keywords",seqUDF(lit("")))
        .drop("top_category", "sub_category", "third_category")
        .filter("article_len > 100 or html_len > 100") // 增加过滤文章内容长度小于100字符的
    }.dropDuplicates("article_id")
    markall
  }

  def read_national_mark_article(spark:SparkSession,
                                  markPath:String):DataFrame = {
    // 读取需人工标注数据
    import spark.implicits._
    val markDF = spark.read.json(markPath)

    val mark_id = markDF.select(col("news_id").cast(StringType),col("resource_id"))
    val markall = {
      val seqUDF = udf((t: String) => Seq.empty[String])
      val cleanUDF = udf((level: String) => if(level == null || level.replace(" ","") == "") "others" else level.trim().toLowerCase)// 增加清洗分类
      markDF.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
        .selectExpr("article_id", "title", "url as article_url", "top_category", "sub_category", "third_category", "length(content) as article_len","length(html) as html_len")
        .withColumn("one_level", cleanUDF(col("top_category")))
        .withColumn("two_level", cleanUDF(col("sub_category")))
        .withColumn("three_level", lit("others"))
//        .withColumn("three_level", cleanUDF(col("third_category")))
        .withColumn("need_double_check",lit(0))
        .withColumn("semantic_keywords",seqUDF(lit("")))
        .drop("top_category", "sub_category", "third_category")
        .filter("article_len > 100 or html_len > 100") // 增加过滤文章内容长度小于100字符的
        .filter("two_level  not in ('usa', 'company', 'economy', 'industry', 'money')")  // 删除不在国内类别里的类
    }

    // 清洗不必要的分类名
    val replacedf = markall.map{
      row =>
        val id = row.getAs[String]("article_id")
        val rep_tmp = row.getAs[String]("two_level")
          .replace("socity", "society").replace("weather", "climate")
          .replace("poeple", "people").replace("region", "religion")
          .replace("politics", "politic").replace("politic", "politics")
          .replace("government jobs", "government-job").replace("government-jobs", "government-job")
          .replace("government-job", "government-jobs")
        (id,rep_tmp)
    }.toDF("article_id", "two_level")

    val mark = markall.drop("two_level").join(replacedf, Seq("article_id"))
    mark
  }

  def read_mark_article_replace_onelevel(spark:SparkSession,
                               markPath: String):DataFrame = {
    // 读取需人工标注数据
    import spark.implicits._
    val markDF = spark.read.json(markPath)

    val mark_id = markDF.select(col("news_id").cast(StringType),col("resource_id"))
    val markall = {
      val seqUDF = udf((t: String) => Seq.empty[String])
      val cleanUDF = udf((level: String) => if(level == null || level.replace(" ","") == "") "others" else level.trim().toLowerCase)// 增加清洗分类
      markDF.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
        .selectExpr("article_id", "title", "url as article_url", "top_category", "sub_category", "third_category", "length(content) as article_len","length(html) as html_len")
        .withColumn("one_level", cleanUDF(col("top_category")))
        .withColumn("two_level", cleanUDF(col("sub_category")))
        .withColumn("three_level", lit("others"))
//        .withColumn("three_level", cleanUDF(col("third_category")))
        .withColumn("need_double_check",lit(0))
        .withColumn("semantic_keywords",seqUDF(lit("")))
        .drop("top_category", "sub_category", "third_category")
        .filter("article_len > 100 or html_len > 100") // 增加过滤文章内容长度小于100字符的
    }.dropDuplicates("article_id")

    // 清洗不必要的分类名
    val replacedf = markall.map{
      row =>
        val id = row.getAs[String]("article_id")
        val rep_tmp = row.getAs[String]("one_level")
          .replace("sports", "sport").replace("sport", "sports") // 体育类修改分类
          .replace("technology", "tech")  // 科技类修改分类名
          .replace("international", "world")  // 国际类修改分类名
        (id, rep_tmp)
    }.toDF("article_id", "one_level")

    val mark = markall.drop("one_level").join(replacedf, Seq("article_id"))
    mark
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
      mark.drop("article_len", "html_len").join(entitywords, Seq("article_id"))
        .join(articleDF,Seq("article_id"))
        .withColumn("entity", nullUDF(col("entity_keywords")))
        .drop("entity_keywords")
        .withColumn("article", tagMarkUDF(col("html"), col("entity")))
        .withColumnRenamed("entity","entity_keywords")
        .drop("html")
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

    val dt = "2019-01-08"
    //("national","entertainment","sports", "business","international","lifestyle","technology","auto","science")
    val category = "international"
    val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"
    val entitywordsPath = "/user/zhoutong/tmp/NGramDF_articleID_and_keywords%s".format("/dt=2018-11-2[2-6]")
    val markPath = if(category == "science") "/user/caifuli/news/all_data/%s".format(category) else "/user/caifuli/news/all_data/%s_classified".format(category)
    val tmpSavePath = "/user/zoushuai/news_content/writemongo/tmp/dt=%s".format(dt)
    val savePath = "/user/zoushuai/news_content/writemongo/%s".format(category)
    val dupsPath = "news_content/dropdups/dropdups.all_150_5"

    //------------------------------------1 读取原始文章-----------------------------------------
    //
    val articleDF = read_ori_article(spark, newsPath, dt)
    //------------------------------------2 处理标注文章-----------------------------------------
    //
    val markDF = {
      if(Seq("sports","technology", "international").contains(category)){
        read_mark_article_replace_onelevel(spark, markPath)}
      else if(category == "national"){
        read_national_mark_article(spark, markPath)}
      else if(category == "lifestyle"){
        read_lifestyle_mark_article(spark, markPath)
      }
      else{
        read_mark_article(spark, markPath)
      }
    }

    //------------------------------------3 标注关键词并-----------------------------------------
    //
    val entitywordsDF = spark.read.parquet(entitywordsPath).select("article_id", "entity_keywords")
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
