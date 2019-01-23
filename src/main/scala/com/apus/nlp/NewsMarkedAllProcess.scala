package com.apus.nlp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.jsoup.Jsoup


/**
  * Created by Joshua on 2019-01-23
  */
object NewsMarkedAllProcess {

  def main(args: Array[String]): Unit = {
    val appName = "News-SubCategory-MarkedCorpus-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext


    //------------------------------------ 已完成标注的文章处理-----------------------------------------
    //
    // 分两部分数据
    // 1. 一部分为标注系统后台数据
    // 2. 一部分为人工标注处理的数据
    def marked_process(spark: SparkSession) = {
      val dt = "2019-01-23"
      val markedpath = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df_cms = spark.read.parquet(markedpath).filter("one_level in ('entertainment', 'national', 'sports', 'lifestyle', 'World', 'business', 'tech', 'science', 'auto', 'sport')")
      //    df_cms.write.mode("overwrite").save("news_sub_classification/trainCorpus/marked/markedcms")

      val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"

      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "url", "title", "html")
          .withColumn("content", getcontentUDF(col("html")))
          .drop("html")
          .dropDuplicates("article_id", "content", "title")
      }

      val cleanTopUDF = udf{(word: String) => word.toLowerCase().replace("sports", "sport").replace("sport","sports").replace("world","international")}

      val df1 = {
        val replaceUDF = udf{
          (word1: String, word2:String) =>
            val words_list = Seq("crime","law","others", "accidents", "public benefit", "events", "food security", "people or groups")
            if(word1=="society"&&words_list.contains(word2)) word2 else word1
        }
        df_cms.filter("one_level = 'national'").filter("two_level is not null and three_level is not null").dropDuplicates("article_id")
          .withColumnRenamed("one_level","one_level_new")
          .withColumnRenamed("two_level","two_level_new")
          .withColumn("one_level", cleanTopUDF(col("one_level_new")))
          .withColumn("two_level",replaceUDF(col("two_level_new"),col("three_level")))
          .select("article_id","one_level","two_level","three_level")
      }

      val df2 = {
        val replaceUDF = udf{
          (word1: String, word2:String) =>
            if(word2!=null) word2 else word1
        }
        df_cms.filter("one_level = 'sports'").filter("two_level is not null and three_level is not null").dropDuplicates("article_id")
          .withColumnRenamed("one_level","one_level_new")
          .withColumnRenamed("two_level","two_level_new")
          .withColumn("one_level", cleanTopUDF(col("one_level_new")))
          .withColumn("two_level",replaceUDF(col("two_level_new"),col("three_level")))
          .select("article_id","one_level","two_level","three_level")
      }

      val df3 = {
        val gossip_check_path = "news_content/sub_classification/entertainment/gossip_check"
        val gossip_check_df = spark.read.json(gossip_check_path).dropDuplicates("article_id")
        val mark = gossip_check_df.withColumn("mark",lit(1)).select("article_id","mark")
        val res = df_cms.join(mark,Seq("article_id"),"left").filter("mark is null").drop("mark").select("article_id", "one_level", "two_level", "three_level")
        val gossip_df = gossip_check_df.select("article_id", "one_level", "two_level", "three_level")
        val df_cms_new = gossip_df.union(res).distinct()

        val replaceUDF = udf{
          (word1: String, word2:String) =>
            if(word1!=null) word1.replace("celebrity", "celebrity&gossip").replace("cartoon&comics","comic").replace("comics","comic").replace("comic","cartoon&comics") else word2
        }
        df_cms_new.filter("one_level = 'entertainment'and two_level != 'gossip'").filter("two_level is not null").dropDuplicates("article_id")
          .withColumnRenamed("one_level","one_level_new")
          .withColumnRenamed("two_level","two_level_new")
          .withColumn("one_level", cleanTopUDF(col("one_level_new")))
          .withColumn("two_level",replaceUDF(col("two_level_new"),col("three_level")))
          .select("article_id","one_level","two_level","three_level")
      }

      val df4 = {
        val tech_check_path = "news_content/sub_classification/tech/tech_check"
        val cleanUDF = udf{(word: String) => word.toLowerCase().replace("mobile phone","mobile-phone").replace("apps", "app").replace(" ", "").replace("mobile-phone", "mobile phone")}
        spark.read.json(tech_check_path)
          .filter("top_category in ('tech', 'Tech','tech')")
          .withColumnRenamed("news_id", "article_id")
          .withColumn("one_level", cleanUDF(col("top_category")))
          .withColumn("two_level", cleanUDF(col("sub_category")))
          .withColumn("three_level", lit("others"))
          .filter("two_level in ('mobile phone','app','internet','gadget','computer','sci-tech','reviews','tablet','software','others','biography','tv')")
          .select("article_id","one_level", "two_level", "three_level").dropDuplicates("article_id")
      }

      val df5 = {
        val auto_path = "news_sub_classification/trainCorpus/marked/auto_classified"
        val auto_ori = spark.read.json(auto_path)
        val mark_id = auto_ori.select(col("news_id").cast(StringType),col("resource_id"))
        val cleanUDF = udf{(word: String) => word.toLowerCase().replace(" ","").replace("aoto","auto")}
        auto_ori.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
          .withColumn("one_level", cleanUDF(col("top_category")))
          .withColumn("two_level", cleanUDF(col("sub_category")))
          .withColumnRenamed("third_category", "three_level")
          .filter("two_level in ('car','motor','cycling','bike','others')")
          .select("article_id","one_level", "two_level", "three_level")
      }
      val df6 = {
        val business_check_path1 = "news_sub_classification/trainCorpus/marked/business_check1"
        val business_check_path2 = "news_sub_classification/trainCorpus/marked/business_check2"
        val cleanUDF = udf{(word: String) =>
          word.toLowerCase()
            .replace("personal finance", "personal-finance")
            .replace("real estate","real-estate").replace("real-eatate", "real-estate")
            .replace("stocks","stock").replace("banking","bank").replace("bank","banking")
            .replace("markets", "market").replace("starups", "startups")
            .replace("oil price","oil-price").replace("blokchain","blockchain")
            .replace(" ","")
            .replace("investment","invest")
            .replace("commodity", "commodities")
            .replace("carrer", "career")
        }
        val df1 = {
          spark.read.json(business_check_path1)
            .filter("top_category = 'business'")
            .withColumnRenamed("news_id", "article_id")
            .withColumnRenamed("top_category","one_level")
            .withColumn("two_level",cleanUDF(col("sub_category")))
            .withColumn("three_level", lit("others"))
            .filter("one_level = 'business'")
            .select("article_id","one_level", "two_level", "three_level")
        }
        val df2 = {
          spark.read.json(business_check_path2)
            .withColumnRenamed("news_id", "article_id")
            .withColumn("three_level", lit("others"))
            .withColumnRenamed("top_category","one_level")
            .withColumn("two_level",cleanUDF(col("sub_category")))
            .filter("one_level = 'business'")
            .select("article_id","one_level","two_level", "three_level")
        }
        df1.union(df2).distinct()
          .dropDuplicates("article_id").filter("two_level != ''").filter("two_level not in ('reviews', 'government-jobs', 'society', 'traffic', 'business', 'fortune', 'politic', 'politics', 'fund', 'computer', 'military', 'sci-tech', 'education', 'music', 'festival', 'yoga', 'traing', 'software', 'app', 'science', 'mobilephone', 'culture', 'governmentjobs', 'hacks', 'agriculture')")
          .map{row =>
            val id = row.getAs[Int]("article_id").toString
            val one = row.getAs[String]("one_level")
            val two = row.getAs[String]("two_level")
            val three = row.getAs[String]("three_level")
            (id,one,two,three)
          }.toDF("article_id","one_level","two_level","three_level")
      }

      val others = {
        val cleanUDF = udf{(word: String) =>
          word.replace("IT","internet").replace("PCs","computer").toLowerCase()
            .replace("mobile phones","mobile phone").replace("televisions", "tv")
            .replace("gadgets", "gadget").replace("markets","market").replace("oil price","oil-price")
            .replace("motor & bike", "motor&bike").replace("laptops","computer").replace("tablets","computer")
            .replace("mobile apps", "app").replace("gaming","game").replace("apps","app")
        }
        df_cms.filter("one_level in ('tech','business','auto')").filter("two_level is not null")
          .withColumnRenamed("two_level","two_level_new")
          .withColumn("two_level", cleanUDF(col("two_level_new")))
          .select("article_id","one_level","two_level","three_level")
      }

      val df = df1.union(df2).union(df3).union(df4).union(df5).union(df6).union(others).distinct()
      val result = df.join(ori_df,Seq("article_id"))
      result.write.mode("overwrite").save("news_sub_classification/trainCorpus/marked_clean")
    }

  }
}
