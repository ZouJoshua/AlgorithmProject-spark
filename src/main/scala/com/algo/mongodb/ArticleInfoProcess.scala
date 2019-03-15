package com.algo.mongodb

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, Entities, TextNode}



/**
  * Created by Joshua on 2018-11-28
  */

object ArticleInfoProcess {

  def dfZipWithIndex(df: DataFrame,
                     offset: Int = 1,
                     colName: String = "id",
                     inFront: Boolean = true
                    ) : DataFrame = {
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

    val spark = SparkSession.builder()
      .appName("ArticleInfoProcess-mongodb")
      .getOrCreate()
    import spark.implicits._

    val variables = DBConfig.parseArgs(args)

    val dt = variables.getOrElse("date", DBConfig.today)
    val entitywordsPath = variables.getOrElse("entity_category_path",DBConfig.entitywordsPath + "/dt=2018-11-2[2-6]")
    val markPath = variables.getOrElse("unclassified_path", DBConfig.unclassifiedPath)
    val artPath = variables.getOrElse("article_info_hdfspath", DBConfig.writeArticleInfoPath)
    val articlePath = DBConfig.oriPath
    val mark_level = variables.getOrElse("mark_level", DBConfig.marklevel.toInt)

    val savePath = artPath + "dt=%s".format(dt)

    // 读取匹配实体词(已计算完500w文章的关键词)
    val entitywords = spark.read.option("basePath", DBConfig.entitywordsPath).parquet(entitywordsPath).select("article_id", "entity_keywords")

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
    val markDF = spark.read.json(markPath)

    val mark_id = markDF.select(col("news_id").cast(StringType),col("resource_id"))
    val markall = {
      val seqUDF = udf((t: String) => Seq.empty[String])
      val cleanUDF = udf((level: String) => if(level == null || level.replace(" ","") == "") "others" else level.trim().toLowerCase)// 增加清洗分类
      markDF.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
        .selectExpr("article_id", "title", "url as article_url", "top_category", "sub_category", "third_category", "length(content) as article_len","length(html) as html_len")
        .withColumn("one_level", cleanUDF(col("top_category")))
        .withColumn("two_level", cleanUDF(col("sub_category")))
        .withColumn("three_level", cleanUDF(col("third_category")))
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
    mark.cache()

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

    // 分量吐出
    val reli = resavedf.filter("two_level = 'religion'")
    val crim = resavedf.filter("two_level = 'crime'")
    val poli = resavedf.filter("two_level = 'politics'")
    val educ = resavedf.filter("two_level = 'education'")
    val gove = resavedf.filter("two_level = 'government-jobs'")
    val law = resavedf.filter("two_level = 'law'")
    val other = resavedf.filter("two_level not in ('religion', 'crime', 'politics', 'education', 'government-jobs', 'law')")

    val reli_index = dfZipWithIndex(reli)
    val crim_index = dfZipWithIndex(crim)
    val poli_index = dfZipWithIndex(poli)
    val educ_index = dfZipWithIndex(educ)
    val gove_index = dfZipWithIndex(gove)
    val other_index = dfZipWithIndex(other)

    reli_index.write.mode(SaveMode.Overwrite).save("news_content/writemongo/dt=2018-12-26/reli")
    crim_index.write.mode(SaveMode.Overwrite).save("news_content/writemongo/dt=2018-12-26/crim")
    poli_index.write.mode(SaveMode.Overwrite).save("news_content/writemongo/dt=2018-12-26/poli")
    educ_index.write.mode(SaveMode.Overwrite).save("news_content/writemongo/dt=2018-12-26/educ")
    gove_index.write.mode(SaveMode.Overwrite).save("news_content/writemongo/dt=2018-12-26/gove")
    law.write.mode(SaveMode.Overwrite).save("news_content/writemongo/dt=2018-12-26/law")
    other_index.write.mode(SaveMode.Overwrite).save("news_content/writemongo/dt=2018-12-26/other")


    spark.stop()
  }
}
