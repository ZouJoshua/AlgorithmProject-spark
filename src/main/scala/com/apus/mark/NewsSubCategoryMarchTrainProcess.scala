package com.apus.mark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.jsoup.Jsoup

/**
  * Created by Joshua on 2019-02-21
  */
object NewsSubCategoryMarchTrainProcess {
  def main(args: Array[String]): Unit = {

    val appName = "News-SubCategory-March-MarkedCorpus-Process"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext


    val dt = "2019-02-21"
    val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"

    //------------------------------------1 更新国内二级分类 -----------------------------------------
    // 1.去除three_level 为null
    // 2.抽取three_level 为crime、law
    // 3.抽取two_level 为politics、education
    // 4.其他放为others(量不足)

    def update_national_process(spark: SparkSession,
                                   newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                                   dt: String="2019-02-21") = {


    }


    //------------------------------------3 更新科技二级分类 -----------------------------------------
    // 1.去除two_level 为null
    // 2.取出internet
    // 3.其他放为others(量不足)
    def update_tech_process(spark: SparkSession,
                                newsPath: String="/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged",
                                dt: String="2019-02-21") = {

      val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
          .selectExpr("resource_id as article_id", "article", "title", "url")
          .withColumn("content", getcontentUDF(col("article.html")))
      }

      // 历史标注数据处理
      val tech_check_path = "news_content/sub_classification/tech/tech_check"
      val tech_old = {
        val cleanUDF = udf{(word: String) => word.toLowerCase().replace("mobile phone","mobile-phone").replace("apps", "app").replace(" ", "").replace("mobile-phone", "mobile phone")}
        spark.read.json(tech_check_path)
          .filter("top_category in ('tech', 'Tech','tech')")
          .withColumnRenamed("news_id", "article_id")
          .withColumn("one_level", cleanUDF(col("top_category")))
          .withColumn("two_level", cleanUDF(col("sub_category")))
          .withColumn("three_level", lit("others"))
          .select("article_id","one_level", "two_level", "three_level").dropDuplicates("article_id")
      }

      // 年前新标注数据处理
      val dt = "2019-02-22"
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df = spark.read.parquet(path)
      val tech_new = {
        df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time")
          .filter("one_level = 'tech'")
          .select("article_id","one_level", "two_level", "three_level")
      }

      // 合并数据
      val tech = tech_old.union(tech_new).distinct()

      val result = {
        val others = Seq("mobile phone", "others", "app", "internet", "gadget", "computer","sci-tech")
        val groupUDF = udf{(word:String) => if (!others.contains(word)) "others" else word}
        val replaceUDF = udf{
          (word:String) =>
          word.replace("mobile phones","mobile phone").replace("gadgets","gadget")
            .replace("mobile apps","app").replace("mobile app","app").replace("apps","app")
            .replace("laptops","laptop").replace("laptop","computer").replace("PCs","computer")
            .replace("televisions", "tv").replace("IT","internet").replace("tv", "sci-tech")
            .replace("tablets","tablet")
        }
        tech.join(ori_df,Seq("article_id")).filter("two_level is not null")
          .filter("two_level not in ('lifestylehacks','stock', 'invest','science','tech', 'industry')")
          .withColumn("two_level_new", groupUDF(replaceUDF(col("two_level"))))
          .drop("three_level")
          .withColumn("three_level", groupUDF(replaceUDF(col("two_level"))))
          .drop("two_level")
          .withColumnRenamed("two_level_new", "two_level")
      }
      println(">>>>>>>>>>正在写入数据")
      result.write.mode("overwrite").save("news_content/sub_classification/tmp/tech_all")
      val redf = spark.read.parquet("news_content/sub_classification/tmp/tech_all")
      redf.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/tech/tech_update")
      println(">>>>>>>>>>写入数据完成")

    }

    def update_business_process(spark: SparkSession,
                                newsPath: String="/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged",
                                dt: String="2019-02-21") = {

      val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
          .selectExpr("resource_id as article_id", "article", "title", "url")
          .withColumn("content", getcontentUDF(col("article.html")))
      }

      // 历史标注数据处理
      val business_check_path1 = "news_content/sub_classification/business/business_check1"
      val business_check_path2 = "news_content/sub_classification/business/business_check2"


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

      val business_old = {
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

      // 年前新标注数据处理
      val dt = "2019-02-22"
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val cms_df = spark.read.parquet(path)
      val business_new = {
        cms_df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time")
          .filter("one_level = 'business'")
          .select("article_id","one_level", "two_level", "three_level")
      }

      // 合并数据
      val business = business_old.union(business_new).distinct()

      val result = {
        val finance = Seq("finance", "money", "banking", "oil-price", "gold")
        val investment = Seq("invest", "personal-finance", "real-estate", "career","property")
        val market = Seq("commodities", "market", "stock", "trading")
        val industry_economic = Seq("people", "industry")
        val groupUDF = udf{
          (word:String) =>
            if(finance.contains(word)) "finance"
            else if(investment.contains(word)) "investment"
            else if(market.contains(word)) "market"
            else if(industry_economic.contains(word)) "industry economic"
            else "others"
        }
        val replaceUDF = udf{
          (word:String) =>
            word.toLowerCase().replace("markets","market").replace("market","markets")
              .replace("oil price","oil-price").replace("blokchain","blockchain").replace("fund","fond")
        }

        val all = business.join(ori_df,Seq("article_id")).withColumn("two_level_new", replaceUDF(col("two_level"))).drop("two_level")
        val company = all.filter("two_level = 'company'").limit(12000).select("article_id","url","title","content","one_level","two_level","three_level")

        val others = all.filter("two_level in ('industry', 'stock', 'market', 'money', 'banking', 'invest', 'personal-finance', 'commodities', 'career', 'tax', 'oil-price', 'real-estate', 'trading', 'gold', 'people', 'law', 'crime', 'property', 'insurance', 'index', 'startups', 'bond', 'e-commerce')")
          .withColumn("two_level_new", groupUDF(col("two_level")))
          .drop("two_level")
          .withColumnRenamed("two_level_new", "two_level")
          .select("article_id","url","title","content","one_level","two_level","three_level")
        company.union(others)
      }




      println(">>>>>>>>>>正在写入数据")
      result.write.mode("overwrite").save("news_content/sub_classification/tmp/business_all")
      val redf = spark.read.parquet("news_content/sub_classification/tmp/business_all")
      redf.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/business/business_update")
      println(">>>>>>>>>>写入数据完成")

    }

    def update_entertainment_process(spark: SparkSession,
                                newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                                dt: String="2019-02-21") = {


    }



  }
}
