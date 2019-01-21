package com.apus.nlp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, TextNode}

/**
  * Created by Joshua on 2019-01-07
  */
object NewsSubCategory {
  def main(args: Array[String]): Unit = {
    val appName = "NewsSubCategoryProcesser"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext


    val dt = "2019-01-18"
    val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"

    //------------------------------------1 处理国内二级分类标注数据（4个分类） -----------------------------------------
    // 1.去除three_level 为null
    // 2.抽取three_level 为crime、law
    // 3.抽取two_level 为politics、education
    // 4.其他放为others(量不足)
    def national_data_processer_v1(spark: SparkSession,
                                   newsPath: String,
                                   dt: String = "2019-01-08") = {
      val df = spark.read.parquet("/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=2019-01-07")
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]").selectExpr("resource_id as article_id", "html", "title").withColumn("content", getcontentUDF(col("html"))).drop("html")
      val nonull_df = df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time").filter("one_level = 'national'").filter("three_level is not null")
      val class_df = nonull_df.join(ori_df, Seq("article_id"))
      val crime_df = class_df.filter("three_level = 'crime'").drop("two_level").withColumn("two_level", lit("crime"))
      val law_df = class_df.filter("three_level = 'law'").drop("two_level").withColumn("two_level", lit("law"))
      val politics_df = class_df.filter("two_level = 'politics'")
      val education_df = class_df.filter("two_level = 'education'")
      crime_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/crime")
      law_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/law")
      politics_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/politics")
      education_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/education")
    }

    //------------------------------------2 处理国内二级分类标注数据（5个分类） -----------------------------------------
    //  1.去除three_level 为null
    //  2.抽取three_level 和two_level 为crime、law
    //  3.抽取two_level 为politics、education
    //  4.其他放为others(two_level: environment\medical\military\traffic\national economy
    //        three_level:people or groups\accidents\public benefit\events\others)
    def national_data_processer_v2(spark: SparkSession,
                                   newsPath: String,
                                   dt: String = "2019-01-08") = {
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df = spark.read.parquet(path)
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]").selectExpr("resource_id as article_id", "html", "title").withColumn("content", getcontentUDF(col("html"))).drop("html")
      val nonull_df = df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time").filter("one_level = 'national'").filter("three_level is not null")
      val class_df = nonull_df.join(ori_df, Seq("article_id"))
      val crime_df = class_df.filter("two_level = 'crime' or three_level = 'crime'").drop("two_level").withColumn("two_level", lit("crime"))
      val law_df = class_df.filter("two_level = 'law' or three_level = 'law'").drop("two_level").withColumn("two_level", lit("law"))
      val politics_df = class_df.filter("two_level = 'politics'")
      val education_df = class_df.filter("two_level = 'education'")
      val others_df1 = {
        class_df.filter("two_level = 'society' and three_level in ('others','people or groups','accidents','public benefit','events','food security')")
          .drop("two_level")
          .withColumn("two_level", lit("society others"))
          .select("article_id", "title", "content", "one_level", "two_level", "three_level", "choose_keywords", "manual_keywords")
      }
      others_df1.cache
      val others_df2 = {
        class_df.filter("two_level in ('environment','medical','military', 'traffic','national economy')")
          .drop("two_level")
          .withColumn("two_level", lit("society others"))
          .select("article_id", "title", "content", "one_level", "two_level", "three_level", "choose_keywords", "manual_keywords")
      }
      others_df2.cache
      val others = others_df1.union(others_df2).distinct()
      println(">>>>>>>>>>正在写入数据")
      crime_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/crime")
      law_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/law")
      politics_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/politics")
      education_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/education")
      others.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/others")
      println(">>>>>>>>>>写入数据完成")
    }

    //------------------------------------3 处理国内二级分类标注数据（5个分类） -----------------------------------------
    //  1.去除three_level 为null(三级为null表示文章有问题，比如不完整等)
    //  2.抽取three_level 和two_level 为crime、law
    //  3.抽取two_level 为politics、education
    //  4.其他放为others(two_level: environment\medical\military\traffic\national economy
    //        three_level: accidents\public benefit\events\others)
    def national_data_processer_v3(spark: SparkSession,
                                   newsPath: String,
                                   dt: String = "2019-01-08") = {
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df = spark.read.parquet(path)
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "html", "title", "url")
          .withColumn("content", getcontentUDF(col("html")))
          .drop("html").dropDuplicates("article_id")
      }
      val nonull_df = df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time").filter("one_level = 'national'").filter("two_level is not null and three_level is not null").dropDuplicates("article_id")
      val class_df = nonull_df.join(ori_df, Seq("article_id"))
      val crime_df = {
        class_df.filter("two_level = 'society' and three_level = 'crime'")
          .drop("two_level")
          .withColumn("two_level", lit("crime"))
          .select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
      }
      val law_df = {
        class_df.filter("two_level = 'society' and three_level = 'law'")
          .drop("two_level")
          .withColumn("two_level", lit("law"))
          .select("article_id",  "url", "title", "content", "one_level", "two_level", "three_level")
      }
      val politics_df = {
        class_df.filter("two_level = 'politics'")
          .select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
      }
      val education_df = {
        class_df.filter("two_level = 'education'")
          .select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
      }
      val others_df1 = {
        val nameUDF = udf((t: String) => if (t != null) t else "others")
        class_df.filter("two_level = 'society' and three_level in ('others', 'accidents', 'public benefit', 'events', 'food security', 'people or groups')")
          .drop("two_level")
          .withColumn("two_level", lit("society others"))
          .select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
      }
      others_df1.cache
      val others_df2 = {
        class_df.filter("two_level in ('environment', 'medical', 'military', 'traffic', 'national economy')")
          .drop("two_level")
          .withColumn("two_level", lit("society others"))
          .select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
      }
      others_df2.cache
      val all_main = crime_df.union(law_df).union(politics_df).union(education_df).select("article_id", "url","title", "content", "one_level", "two_level", "three_level")
      val others = others_df1.union(others_df2).select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
      val all = all_main.union(others).distinct()
      println(">>>>>>>>>>正在写入数据")
      all.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/national_all")
      println(">>>>>>>>>>写入数据完成")

    }

    //------------------------------------4 处理娱乐分类标注数据（） -----------------------------------------
    //

    def entertainment_data_processer_v1(spark: SparkSession,
                                   newsPath: String,
                                   dt: String = "2019-01-09") = {
      val gossip_check_path = "news_content/sub_classification/entertainment/gossip_check"
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df = spark.read.parquet(path)
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "html", "url", "title")
          .withColumn("content", getcontentUDF(col("html")))
          .drop("html")
      }
      val nonull_df = df.select("article_id", "one_level", "two_level", "three_level").filter("one_level = 'entertainment'").dropDuplicates("article_id")
      val class_df = nonull_df.join(ori_df, Seq("article_id"))
      val gossip_check_df = spark.read.json(gossip_check_path).dropDuplicates("article_id")
      val mark = gossip_check_df.withColumn("mark",lit(1)).select("article_id","mark")
      val res = class_df.join(mark,Seq("article_id"),"left").filter("mark is null").drop("mark").select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
      val gossip_df = gossip_check_df.join(ori_df, Seq("article_id")) .select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
      val correct_df = gossip_df.union(res).distinct()
      val all_main = {
        val renameUDF = udf{(t:String) => t.replace("celebrity", "celebrity&gossip").replace("cartoon&comics","comic").replace("comics","comic").replace("comic","cartoon&comics")}
        correct_df.filter("two_level in ('bollywood','celebrity','tv','movie','music','comic','comics','hollywood','cartoon&comics')")
            .withColumn("two_level_new", renameUDF(col("two_level")))
            .drop("two_level")
            .withColumnRenamed("two_level_new", "two_level")
            .select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
      }
      val others = {
        correct_df.filter("two_level in ('art+culture+history','dance', 'others','variety show','performance')")
          .drop("two_level")
          .withColumn("two_level", lit("others"))
          .select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
      }
      val all = all_main.union(others).distinct()
      println(">>>>>>>>>>正在写入数据")
      all.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/entertainment/entertainment_all")
      println(">>>>>>>>>>写入数据完成")
    }

    //------------------------------------5 处理财经分类标注数据（） -----------------------------------------
    //

    def business_data_processer(spark: SparkSession,
                                        newsPath: String,
                                        dt: String = "2019-01-14") = {
      import spark.implicits._
      val business_check_path1 = "news_content/sub_classification/business/business_check1"
      val business_check_path2 = "news_content/sub_classification/business/business_check2"

      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "html", "title")
          .withColumn("content", getcontentUDF(col("html")))
          .drop("html")
      }
      val cleanUDF = udf{(word: String) =>
        word.toLowerCase()
          .replace("personal finance", "finance").replace("personal-finance", "finance")
          .replace("finance","personal-finance")
          .replace("real estate","real-estate").replace("real-eatate", "real-estate")
          .replace("stocks","stock").replace("banking","bank").replace("bank","banking")
          .replace("markets", "market")
          .replace("oil price","oil-price")
          .replace(" ","")
          .replace("investment","invest")
          .replace("commodity", "commodities")
      }
      val df1 = {
        spark.read.json(business_check_path1)
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

      val df = {
        df1.union(df2).distinct()
          .dropDuplicates("article_id").filter("two_level != ''")
          .map{row =>
            val id = row.getAs[Int]("article_id").toString
            val one = row.getAs[String]("one_level")
            val two = row.getAs[String]("two_level")
            val three = row.getAs[String]("three_level")
            (id,one,two,three)
          }.toDF("article_id","one_level","two_level","three_level")
      }

      val result = {
        val others = Seq("personal-finance", "commodities", "career", "tax", "oil-price", "real-estate", "trading", "gold", "people", "law", "crime", "property", "insurance", "index", "startups", "bond", "e-commerce")
        val replaceUDF = udf{(word:String) => if(others.contains(word)) "others" else word}
        df.join(ori_df,Seq("article_id"))
          .filter("two_level in ('company', 'economy', 'industry', 'stock', 'market', 'money', 'banking', 'invest', 'personal-finance', 'commodities', 'career', 'tax', 'oil-price', 'real-estate', 'trading', 'gold', 'people', 'law', 'crime', 'property', 'insurance', 'index', 'startups', 'bond', 'e-commerce')")
          .withColumn("two_level_new", replaceUDF(col("two_level")))
          .drop("two_level")
          .withColumnRenamed("two_level_new", "two_level")
      }
      println(">>>>>>>>>>正在写入数据")
      result.write.mode("overwrite").save("news_content/sub_classification/tmp/business_all")
      val redf = spark.read.parquet("news_content/sub_classification/tmp/business_all")
      redf.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/business/business_all")
      println(">>>>>>>>>>写入数据完成")
    }

    //------------------------------------5 处理财经分类标注数据（版本2） -----------------------------------------
    // finance: money\banking\oil-price\gold
    // investment: invest\personal-finance\real-estate\carrer\property
    // market: commodities\market\stock\trading
    // industry economic: company\people

    def business_data_processer_v1(spark: SparkSession,
                                newsPath: String,
                                dt: String = "2019-01-14") = {
      import spark.implicits._
      val business_check_path1 = "news_content/sub_classification/business/business_check1"
      val business_check_path2 = "news_content/sub_classification/business/business_check2"

      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "html", "title")
          .withColumn("content", getcontentUDF(col("html")))
          .drop("html")
      }
      val cleanUDF = udf{(word: String) =>
        word.toLowerCase()
          .replace("personal finance", "personal-finance")
          .replace("real estate","real-estate").replace("real-eatate", "real-estate")
          .replace("stocks","stock").replace("banking","bank").replace("bank","banking")
          .replace("markets", "market").replace("starups", "startups")
          .replace("oil price","oil-price")
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

      val df = {
        df1.union(df2).distinct()
          .dropDuplicates("article_id").filter("two_level != ''")
          .map{row =>
            val id = row.getAs[Int]("article_id").toString
            val one = row.getAs[String]("one_level")
            val two = row.getAs[String]("two_level")
            val three = row.getAs[String]("three_level")
            (id,one,two,three)
          }.toDF("article_id","one_level","two_level","three_level")
      }

      val result = {
        val finance = Seq("finance", "money", "banking", "oil-price", "gold")
        val investment = Seq("invest", "personal-finance", "real-estate", "career","property")
        val market = Seq("commodities", "market", "stock", "trading")
        val industry_economic = Seq("company", "people", "industry")
        val replaceUDF = udf{
          (word:String) =>
          if(finance.contains(word)) "finance"
          else if(investment.contains(word)) "investment"
          else if(market.contains(word)) "market"
          else if(industry_economic.contains(word)) "industry economic"
          else "others"
        }
        df.join(ori_df,Seq("article_id"))
          .filter("two_level in ('company', 'industry', 'stock', 'market', 'money', 'banking', 'invest', 'personal-finance', 'commodities', 'career', 'tax', 'oil-price', 'real-estate', 'trading', 'gold', 'people', 'law', 'crime', 'property', 'insurance', 'index', 'startups', 'bond', 'e-commerce')")
          .withColumn("two_level_new", replaceUDF(col("two_level")))
          .drop("two_level")
          .withColumnRenamed("two_level_new", "two_level")
      }
      println(">>>>>>>>>>正在写入数据")
      result.write.mode("overwrite").save("news_content/sub_classification/tmp/business_all")
      val redf = spark.read.parquet("news_content/sub_classification/tmp/business_all")
      redf.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/business/business_all")
      println(">>>>>>>>>>写入数据完成")
    }

    //------------------------------------5 处理财经分类标注数据（版本3） -----------------------------------------
    // finance: money\banking\oil-price\gold
    // investment: invest\personal-finance\real-estate\carrer\property
    // market: commodities\market\stock\trading
    // industry economic: company\people\industry

    def business_data_processer_v2(spark: SparkSession,
                                   newsPath: String,
                                   dt: String = "2019-01-16") = {
      import spark.implicits._
      val business_check_path1 = "news_content/sub_classification/business/business_check1"
      val business_check_path2 = "news_content/sub_classification/business/business_check2"

      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "html", "title", "url")
          .withColumn("content", getcontentUDF(col("html")))
          .drop("html")
      }
      val cleanUDF = udf{(word: String) =>
        word.toLowerCase()
          .replace("personal finance", "personal-finance")
          .replace("real estate","real-estate").replace("real-eatate", "real-estate")
          .replace("stocks","stock").replace("banking","bank").replace("bank","banking")
          .replace("markets", "market").replace("starups", "startups")
          .replace("oil price","oil-price")
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

      val df = {
        df1.union(df2).distinct()
          .dropDuplicates("article_id").filter("two_level != ''")
          .map{row =>
            val id = row.getAs[Int]("article_id").toString
            val one = row.getAs[String]("one_level")
            val two = row.getAs[String]("two_level")
            val three = row.getAs[String]("three_level")
            (id,one,two,three)
          }.toDF("article_id","one_level","two_level","three_level")
      }

      val result = {
        val finance = Seq("finance", "money", "banking", "oil-price", "gold")
        val investment = Seq("invest", "personal-finance", "real-estate", "career","property")
        val market = Seq("commodities", "market", "stock", "trading")
        val industry_economic = Seq("people", "industry")
        val replaceUDF = udf{
          (word:String) =>
            if(finance.contains(word)) "finance"
            else if(investment.contains(word)) "investment"
            else if(market.contains(word)) "market"
            else if(industry_economic.contains(word)) "industry economic"
            else "others"
        }
        val all = df.join(ori_df,Seq("article_id"))
        val company = all.filter("two_level = 'company'").limit(12000).select("article_id","url","title","content","one_level","two_level","three_level")

        val others = all.filter("two_level in ('industry', 'stock', 'market', 'money', 'banking', 'invest', 'personal-finance', 'commodities', 'career', 'tax', 'oil-price', 'real-estate', 'trading', 'gold', 'people', 'law', 'crime', 'property', 'insurance', 'index', 'startups', 'bond', 'e-commerce')")
          .withColumn("two_level_new", replaceUDF(col("two_level")))
          .drop("two_level")
          .withColumnRenamed("two_level_new", "two_level")
            .select("article_id","url","title","content","one_level","two_level","three_level")
        company.union(others)
      }
      println(">>>>>>>>>>正在写入数据")
      result.write.mode("overwrite").save("news_content/sub_classification/tmp/business_all")
      val redf = spark.read.parquet("news_content/sub_classification/tmp/business_all")
      redf.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/business/business_all")
      println(">>>>>>>>>>写入数据完成")
    }


    //------------------------------------6 处理科技分类标注数据（） -----------------------------------------
    //
    def tech_data_processer(spark: SparkSession,
                                newsPath: String,
                                dt: String = "2019-01-21") = {
      import spark.implicits._
      val tech_check_path = "news_content/sub_classification/tech/tech_check"

      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "html", "title", "url")
          .withColumn("content", getcontentUDF(col("html")))
          .drop("html")
      }
      val df1 = {
        val cleanUDF = udf{(word: String) => word.toLowerCase().replace("mobile phone","mobile-phone").replace("apps", "app").replace(" ", "").replace("mobile-phone", "mobile phone")}
        spark.read.json(tech_check_path)
          .filter("top_category in ('tech', 'Tech','tech')")
          .withColumnRenamed("news_id", "article_id")
          .withColumn("one_level", cleanUDF(col("top_category")))
          .withColumn("two_level", cleanUDF(col("sub_category")))
          .withColumn("three_level", lit("others"))
          .select("article_id","one_level", "two_level", "three_level").dropDuplicates("article_id")
        }

      val result = {
        val others = Seq("sci-tech", "internet", "reviews", "tablet", "software", "others")
        val replaceUDF = udf{(word:String) => if(others.contains(word)) "others" else word}
        df1.join(ori_df,Seq("article_id"))
          .filter("two_level in ('mobile phone','app', 'gadget','computer','sci-tech', 'internet', 'reviews', 'tablet', 'software', 'others')")
          .withColumn("two_level_new", replaceUDF(col("two_level")))
          .drop("two_level")
          .withColumnRenamed("two_level_new", "two_level")
      }
      println(">>>>>>>>>>正在写入数据")
      result.write.mode("overwrite").save("news_content/sub_classification/tmp/tech_all")
      val redf = spark.read.parquet("news_content/sub_classification/tmp/tech_all")
      redf.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/tech/tech_all")
      println(">>>>>>>>>>写入数据完成")
    }

    //------------------------------------7 体育分类标注数据（） -----------------------------------------
    //
    def sports_data_processer(spark: SparkSession,
                            newsPath: String,
                            dt: String = "2019-01-15") = {
      import spark.implicits._

      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df = spark.read.parquet(path)
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "html", "title")
          .withColumn("content", getcontentUDF(col("html")))
          .drop("html")
      }
      val sports_df = df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time").filter("one_level = 'sports'")
      val sports_result_df = {
        val others = Seq("horse racing", "athletics", "kabaddi", "volleybal", "chess")
        val replaceUDF = udf((word:String) => if(others.contains(word)) "others" else word)
        sports_df.join(ori_df, Seq("article_id"))
          .withColumnRenamed("two_level","three_level_new")
          .withColumn("two_level", replaceUDF(col("three_level")))
          .drop("three_level")
          .withColumnRenamed("three_level_new", "three_level")
      }
      println(">>>>>>>>>>正在写入数据")
      sports_result_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/sports/sports_all")
      println(">>>>>>>>>>写入数据完成")
    }

    //------------------------------------8 汽车分类标注数据（） -----------------------------------------
    //
    def auto_data_processer(spark: SparkSession,
                              newsPath: String,
                              dt: String = "2019-01-15") = {
      import spark.implicits._

      val auto_path = "/user/caifuli/news/all_data/auto_classified"
      val auto_ori = spark.read.json(auto_path)

      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "html", "title", "url")
          .withColumn("content", getcontentUDF(col("html")))
          .drop("html")
      }

      val auto_process_df = {
        val mark_id = auto_ori.select(col("news_id").cast(StringType),col("resource_id"))
        val cleanUDF = udf{(word: String) => word.toLowerCase().replace(" ","").replace("aoto","auto")}
        auto_ori.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
          .withColumn("one_level", cleanUDF(col("top_category")))
          .withColumn("two_level", cleanUDF(col("sub_category")))
          .withColumnRenamed("third_category", "three_level")
          .select("article_id","one_level", "two_level", "three_level")
      }

      val auto = {
        val two_wheels = Seq("motor","cycling","bike")
        val replaceUDF = udf((word:String) => if(two_wheels.contains(word)) "others" else word)
        auto_process_df.join(ori_df, Seq("article_id"))
          .filter("two_level in ('car','motor','cycling','bike')")
          .withColumn("two_level_new", replaceUDF(col("two_level")))
          .drop("two_level")
          .withColumnRenamed("two_level_new", "two_level")
      }
      println(">>>>>>>>>>正在写入数据")
      auto.write.mode("overwrite").save("news_content/sub_classification/tmp/auto_all")
      val redf = spark.read.parquet("news_content/sub_classification/tmp/auto_all")
      redf.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/auto/auto_all")
      println(">>>>>>>>>>写入数据完成")
    }
  }
}
