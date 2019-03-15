package com.privyalgo.nlp

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, Entities, TextNode}


/**
  * Created by Joshua on 2018-12-20
  */
object NewsDeduplication {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NewsDeduplication")
      .getOrCreate()
    import spark.implicits._

    // 读取原始数据(默认22日-26日全部500W+文章)
    val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"
    val getcontentUDF = udf{(html:String) => Jsoup.parse(html).text()}
    val articleAllDF = {
      spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
        .selectExpr("resource_id as article_id", "html", "country_lang as country_lan")
        .repartition(512)
        .dropDuplicates("html")
    }
    val articledf = articleAllDF.withColumn("content_all", getcontentUDF(col("html"))).dropDuplicates("content_all")
    articledf.cache

    // 读取分类数据
    val markPath = "/user/caifuli/news/data/classified"
    val markDF = spark.read.json(markPath)
    val mark_id = markDF.select(col("news_id").cast(StringType),col("resource_id"))
    val mark = {
      val seqUDF = udf((t: String) => Seq.empty[String])
      markDF.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
        .withColumn("article_old", concat_ws("", Seq($"content", $"html"): _*))
        .selectExpr("article_id", "url as article_url", "title", "article_old", "top_category as one_level", "sub_category as two_level", "third_category as three_level", "length(content) as article_len","length(html) as html_len")
        .withColumn("need_double_check",lit(0))
        .withColumn("semantic_keywords",seqUDF(lit("")))
//        .filter("article_len > 100 or html_len > 100") // 增加过滤文章内容长度小于100字符的
    }

    // 选取article_id,article 进行去重
    val mark_article = {
      mark.join(articledf, Seq("article_id"),"left").map{
        row =>
          val id = row.getAs[String]("article_id")
          val article = row.getAs[String]("content_all")
          val content = if(article != null){
            article
          }else{
            row.getAs[String]("article_old").replaceAll("[\r\n\t]", " ")
              .replaceAll("(&amp;|&#13;|&nbsp;)","")
          }
          (id, content)
      }.toDF("article_id", "article")
    }
    mark_article.repartition(1).write.format("json").mode(SaveMode.Overwrite).save("news_content/deduplication/dt=2018-12-20")

    // 去重做完，保存数据
    val dupdf = spark.read.json("news_content/dropdups/dropdups.all_150_5")  // 去重结果
    val write_to_mongo = {
      mark.join(articledf, Seq("article_id"),"left")
        .join(dupdf,Seq("article_id"),"left").filter("dupmark is null")
    }
    write_to_mongo.write.mode(SaveMode.Overwrite).save("news_content/writemongo/tmp/dt=2018-12-21")

  }
}
