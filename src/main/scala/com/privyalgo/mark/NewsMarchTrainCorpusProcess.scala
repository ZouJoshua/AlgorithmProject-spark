package com.privyalgo.mark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.jsoup.Jsoup


/**
  * Created by Joshua on 2019-01-23
  */
object NewsMarchTrainCorpusProcess {

  def main(args: Array[String]): Unit = {
    val appName = "News-March-TrainCorpus-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext


    //------------------------------------ 已完成标注的文章处理-----------------------------------------
    //
    // 分两部分数据
    // 1. 一部分为标注系统后台数据(更新截止到3月7日)
    // 2. 一部分为人工标注处理的数据
    def train_corpus_process(spark: SparkSession) = {

      // 人工标注处理数据
      val science_path = "/user/caifuli/news/all_data/science"
      val business_check_path1 = "news_sub_classification/trainCorpus/marked/business_check1"
      val business_check_path2 = "news_sub_classification/trainCorpus/marked/business_check2"
      val auto_path = "news_sub_classification/trainCorpus/marked/auto_classified"
      val gossip_check_path = "news_sub_classification/trainCorpus/marked/gossip_check"
      val tech_check_path = "news_sub_classification/trainCorpus/marked/tech_check"


      // 原始文章
      val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
          .selectExpr("resource_id as article_id", "article", "title", "url")
      }

      // cms标注文章
      val dt = "2019-03-14"
      val markedpath = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)

      val cleanUDF = udf{(category:String, word: String) =>
        if (category=="business"&&(word!=""||word!=null))
          word.toLowerCase()
            .replace("personal finance", "personal-finance")
            .replace("real estate","real-estate").replace("real-eatate", "real-estate")
            .replace("stocks","stock").replace("banking","bank").replace("bank","banking")
            .replace("markets", "market").replace("starups", "startups")
            .replace("oil price","oil-price")
            .replace("blokchain","blockchain")
            .replace(" ","")
            .replace("investment","invest")
            .replace("commodity", "commodities")
            .replace("carrer", "career")
        else if(category=="tech"&&(word!=""||word!=null))
          word.replace("mobile phones","mobile phone").replace("gadgets","gadget")
            .replace("mobile apps","app").replace("mobile app","app").replace("apps","app")
            .replace("laptops","laptop").replace("laptop","computer").replace("PCs","computer").replace("tablets","tablet").replace("tablet","computer")
            .replace("televisions", "tv").replace("IT","internet")
        else if(category=="auto"&&(word!=""||word!=null))
          word.toLowerCase().replace(" ","").replace("aoto","auto")
            .replace("autoshow","auto show")
            .replace("autoindustry","auto industry")
            .replace("air-taxi","others")
            .replace("mobilephone","others")
        else if(category=="entertainment"&&(word!=""||word!=null))
          word.toLowerCase().replace("cartoon&comics","comic").replace("comics","comic").replace("comic","cartoon&comics")
        else
          word
      }

      val cleanTopUDF = udf{(word: String) => word.toLowerCase().replace("sports", "sport").replace("sport","sports").replace("world","international")}

      val df_cms = {
        spark.read.parquet(markedpath)
          .filter("one_level in ('entertainment', 'national', 'sports', 'lifestyle', 'World', 'business', 'tech', 'science', 'auto', 'sport')")
          .drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time")
          .filter("two_level is not null")
          .withColumnRenamed("one_level","one_level_new")
          .withColumn("one_level",cleanTopUDF(col("one_level_new")))
          .withColumnRenamed("two_level","two_level_new")
          .withColumn("two_level",cleanUDF(col("one_level"),col("two_level_new")))
          //          .drop("three_level").withColumn("three_level",lit("others"))
          .select("article_id","one_level", "two_level", "three_level")
      }
      //    df_cms.write.mode("overwrite").save("news_sub_classification/trainCorpus/marked/markedcms")

      // 人工线下标注

      val manual_label_science = {
        val science_ori = spark.read.json(science_path)
        val science_cleanUDF = udf{(word: String) => word.toLowerCase().replace(" ","")}
        val mark_id = science_ori.select(col("news_id").cast(StringType),col("resource_id"))
        science_ori.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
          .withColumn("one_level", science_cleanUDF(col("top_category")))
          .withColumn("two_level", science_cleanUDF(col("sub_category")))
          .withColumnRenamed("third_category", "three_level")
          .filter("two_level != ''")
          .select("article_id","one_level", "two_level", "three_level")
      }


      val manual_label_tech = {
        val clean_techUDF = udf{(word: String) => word.toLowerCase().replace("mobile phone","mobile-phone").replace("apps", "app").replace(" ", "").replace("mobile-phone", "mobile phone")}
        spark.read.json(tech_check_path)
          .filter("top_category in ('tech', 'Tech','tech')")
          .withColumnRenamed("news_id", "article_id")
          .withColumn("one_level", clean_techUDF(col("top_category")))
          .withColumn("two_level", clean_techUDF(col("sub_category")))
          .withColumn("three_level", lit("others"))
          .select("article_id","one_level", "two_level", "three_level").dropDuplicates("article_id")
      }

      val manual_label_gossip = spark.read.json(gossip_check_path).dropDuplicates("article_id").select("article_id","one_level", "two_level", "three_level")

      val manual_label_auto = {
        val auto_ori = spark.read.json(auto_path)
        val auto_cleanUDF = udf{(word: String) => word.toLowerCase().replace(" ","").replace("aoto","auto")}
        val mark_id = auto_ori.select(col("news_id").cast(StringType),col("resource_id"))
        auto_ori.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
          .withColumn("one_level", auto_cleanUDF(col("top_category")))
          .withColumn("two_level", auto_cleanUDF(col("sub_category")))
          .withColumnRenamed("third_category", "three_level")
          .filter("two_level != ''")
          .select("article_id","one_level", "two_level", "three_level")
      }

      val manual_label_business = {
        val df1 = {
          spark.read.json(business_check_path1)
            .filter("top_category = 'business'")
            .withColumnRenamed("news_id", "article_id")
            .withColumnRenamed("top_category","one_level")
            .withColumn("two_level",cleanUDF(col("one_level"),col("sub_category")))
            .withColumn("three_level", lit("others"))
            .filter("one_level = 'business'")
            .select("article_id","one_level", "two_level", "three_level")
        }
        val df2 = {
          spark.read.json(business_check_path2)
            .withColumnRenamed("news_id", "article_id")
            .withColumn("three_level", lit("others"))
            .withColumnRenamed("top_category","one_level")
            .withColumn("two_level",cleanUDF(col("one_level"), col("sub_category")))
            .filter("one_level = 'business'")
            .select("article_id","one_level","two_level", "three_level")
        }
        df1.union(df2).distinct()
          .dropDuplicates("article_id").filter("two_level != ''")
          .select("article_id","one_level","two_level","three_level")
      }.filter("two_level in ('company','industry', 'stock', 'market', 'money', 'banking', 'invest', 'economy'" +
        "'personal-finance', 'commodities', 'career', 'tax', 'oil-price', 'real-estate', 'trading'," +
        "'gold', 'people', 'law', 'crime', 'property', 'insurance', 'index', 'regulation', 'finance'" +
        "'startups', 'bond', 'e-commerce','others','auction','consumption', 'credit', 'blockchain')")


      val df = df_cms.union(manual_label_science).union(manual_label_tech).union(manual_label_auto).union(manual_label_gossip).union(manual_label_business)

      val result = df.join(ori_df,Seq("article_id")).withColumn("content", getcontentUDF(col("article.html"))).drop("article")
      // 2019-03-14标注结果整理
      result.write.mode("overwrite").save("news_sub_classification/trainCorpus/march_marked_clean_314")

      val out = spark.read.parquet("news_sub_classification/trainCorpus/march_marked_clean_314")
      out.filter("one_level = 'business'").coalesce(1).write.format("json").mode("overwrite").save("news_sub_classification/trainCorpus/march_marked_clean/business")
      out.filter("one_level = 'sports'").coalesce(1).write.format("json").mode("overwrite").save("news_sub_classification/trainCorpus/march_marked_clean/sports")
      out.filter("one_level = 'national'").coalesce(1).write.format("json").mode("overwrite").save("news_sub_classification/trainCorpus/march_marked_clean/national")
      out.filter("one_level = 'international'").coalesce(1).write.format("json").mode("overwrite").save("news_sub_classification/trainCorpus/march_marked_clean/international")
      out.filter("one_level = 'auto'").coalesce(1).write.format("json").mode("overwrite").save("news_sub_classification/trainCorpus/march_marked_clean/auto")
      out.filter("one_level = 'lifestyle'").coalesce(1).write.format("json").mode("overwrite").save("news_sub_classification/trainCorpus/march_marked_clean/lifestyle")
      out.filter("one_level = 'entertainment'").coalesce(1).write.format("json").mode("overwrite").save("news_sub_classification/trainCorpus/march_marked_clean/entertainment")
      out.filter("one_level = 'tech'").coalesce(1).write.format("json").mode("overwrite").save("news_sub_classification/trainCorpus/march_marked_clean/tech")
      out.filter("one_level = 'science'").coalesce(1).write.format("json").mode("overwrite").save("news_sub_classification/trainCorpus/march_marked_clean/science")


    }

  }
}
