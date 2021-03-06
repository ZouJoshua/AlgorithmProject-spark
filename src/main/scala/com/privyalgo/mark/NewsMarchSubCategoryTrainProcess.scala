package com.privyalgo.mark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.jsoup.Jsoup

/**
  * Created by Joshua on 2019-02-21
  */
object NewsMarchSubCategoryTrainProcess {
  def main(args: Array[String]): Unit = {

    val appName = "News-March-SubCategory-MarkedCorpus-Process"
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

      val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
          .selectExpr("resource_id as article_id", "article", "title", "url")
      }
      // 历史标注均在cms中
      val dt = "2019-03-12"
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df = spark.read.parquet(path)
      val national_cms_df = {
        df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time")
          .filter("one_level = 'national'")
          .select("article_id","one_level", "two_level", "three_level")
      }

      // 写入文件
      val national = {
        national_cms_df.join(ori_df, Seq("article_id"))
          .filter("two_level is not null").filter("three_level is not null")
          .withColumn("content", getcontentUDF(col("article.html"))).drop("article")
      }
      national.write.mode("overwrite").save("news_content/sub_classification/tmp/national_update_all")

      val result = {
        val main_words = Seq("crime", "law", "government-jobs")
        val other_words = Seq("national economy")
        val groupUDF = udf{(word1:String, word2:String) => if (main_words.contains(word2)) word2.replace("policies and regulations","policies&regulations") else if(other_words.contains(word1)) "others" else word1}

        val all = {
          spark.read.parquet("news_content/sub_classification/tmp/national_update_all")
            .withColumn("two_level_new", groupUDF(col("two_level"), col("three_level")))
            .drop("two_level")
            .withColumnRenamed("two_level_new", "two_level")
        }
        val politics_limit = all.filter("two_level not in ('government-jobs','environment','others','military','traffic','medical')").filter("two_level = 'politics' and three_level in ('people or groups','others')").limit(20000).select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val society_limit = all.filter("two_level not in ('government-jobs','environment','others','military','traffic','medical')").filter("two_level = 'society' and three_level = 'people or groups'").limit(10000).select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val crime_limit = all.filter("two_level not in ('government-jobs','environment','others','military','traffic','medical')").filter("two_level = 'crime'").limit(15000).select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val law_limit = all.filter("two_level not in ('government-jobs','environment','others','military','traffic','medical')").filter("two_level = 'law'").limit(15000).select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val others_limit = all.filter("two_level not in ('government-jobs','environment','others','military','traffic','medical')").filter("two_level != 'law' and two_level != 'crime' and three_level not in ('people or groups','others')").select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val main_category = all.filter("two_level in ('government-jobs','environment','others','military','traffic','medical')").select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val out = main_category.union(politics_limit).union(society_limit).union(others_limit).union(crime_limit).union(law_limit).distinct()
        out
      }

      println(">>>>>>>>>>正在写入数据")
      result.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/national_update")
      println(">>>>>>>>>>写入数据完成")



    }


    //------------------------------------2 更新科技二级分类 -----------------------------------------
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
      val dt = "2019-03-12"
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df = spark.read.parquet(path)
      val tech_new = {
        df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time")
          .filter("one_level = 'tech'")
          .select("article_id","one_level", "two_level", "three_level")
      }

      val replaceUDF = udf{
        (word:String) =>
          word.replace("mobile phones","mobile phone").replace("gadgets","gadget")
            .replace("mobile apps","app").replace("mobile app","app").replace("apps","app")
            .replace("laptops","laptop").replace("laptop","computer").replace("PCs","computer").replace("tablets","tablet").replace("tablet","computer")
            .replace("televisions", "tv").replace("IT","internet")
      }

      // 合并数据
      val tech_tmp = tech_old.union(tech_new).distinct()
      val tech = {
        tech_tmp.join(ori_df, Seq("article_id"))
          .filter("two_level is not null")
          .filter("two_level not in ('lifestylehacks','stock', 'invest','science','tech', 'industry')")
          .withColumn("two_level_new", replaceUDF(col("two_level")))
          .drop("two_level").withColumnRenamed("two_level_new", "two_level")
          .withColumn("content", getcontentUDF(col("article.html"))).drop("article")
      }
      // 先写入文件处理
      tech.write.mode("overwrite").save("news_content/sub_classification/tmp/tech_update_all")

      val result = {
        val main_words = Seq("mobile phone", "others", "app", "internet", "gadget", "computer","intelligent hardware", "wearable device")
        val gadget_words = Seq("intelligent hardware", "wearable device")
        val groupUDF = udf{(word:String) => if (!main_words.contains(word)) "others" else if (gadget_words.contains(word)) "gadget" else word}

        val all = spark.read.parquet("news_content/sub_classification/tmp/tech_update_all")
        all.withColumn("two_level_new", groupUDF(col("two_level")))
          .drop("three_level")
          .withColumn("three_level", groupUDF(col("two_level")))
          .drop("two_level")
          .withColumnRenamed("two_level_new", "two_level")
      }

      println(">>>>>>>>>>正在写入数据")
      result.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/tech/tech_update")
      println(">>>>>>>>>>写入数据完成")

    }


    //------------------------------------3 更新财经二级分类 -----------------------------------------
    // 1.去除two_level 为null
    // 2.去掉电子商务、犯罪、law ，样本比较杂
    // 3.其他放为others(量不足)
    def update_business_process(spark: SparkSession,
                                newsPath: String="/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged",
                                dt: String="2019-02-21") = {

      val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
          .selectExpr("resource_id as article_id", "article", "title", "url")
          .dropDuplicates("article_id")
      }

      // 历史标注数据处理
      val business_check_path1 = "news_content/sub_classification/business/business_check1"
      val business_check_path2 = "news_content/sub_classification/business/business_check2"


      val cleanUDF = udf{(word: String) => if (word!=""||word!=null)
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
      else ""
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
          .select("article_id","one_level","two_level","three_level")
      }.filter("two_level in ('company','industry', 'stock', 'market', 'money', 'banking', 'invest', 'economy'" +
        "'personal-finance', 'commodities', 'career', 'tax', 'oil-price', 'real-estate', 'trading'," +
        "'gold', 'people', 'law', 'crime', 'property', 'insurance', 'index', 'regulation', 'finance'" +
        "'startups', 'bond', 'e-commerce','others','auction','consumption', 'credit', 'blockchain')")

      // 年前新标注数据处理
      val dt = "2019-03-12"
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val cms_df = spark.read.parquet(path)
      val business_new = {
        cms_df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time")
          .filter("one_level = 'business'").filter("two_level is not null")
            .withColumnRenamed("two_level","two_level_new")
            .withColumn("two_level",cleanUDF(col("two_level_new")))
//          .drop("three_level").withColumn("three_level",lit("others"))
          .select("article_id","one_level", "two_level", "three_level")
      }

      // 合并数据
      val business_tmp = business_old.union(business_new).distinct()
      val business = business_tmp.join(ori_df,Seq("article_id")).withColumn("content", getcontentUDF(col("article.html"))).drop("article")
      // 先写入文件处理
      business.write.mode("overwrite").save("news_content/sub_classification/tmp/business_all")

      val result = {
        val company = Seq("startups")
        val last = Seq("crime", "law", "e-commerce")
        val stock = Seq("bond","stock")
        val career = Seq("people")
        val market = Seq("regulation")
        val economy = Seq("index","personal-finance")
        val invest = Seq("consumption", "property")
        val others = Seq("auction", "blockchain", "credit")
        val groupUDF = udf {
          (word: String) =>
            if (company.contains(word)) "company"
            else if (last.contains(word)) ""
            else if (invest.contains(word)) "invest"
            else if (market.contains(word)) "market"
            else if (stock.contains(word)) "stock&bond"
            else if (career.contains(word)) "career"
            else if (economy.contains(word)) "economy"
            else if (others.contains(word)) "others"
            else word
        }

        val all = {
          spark.read.parquet("news_content/sub_classification/tmp/business_all").drop("three_level")
            .withColumnRenamed("two_level", "three_level")
            .withColumn("two_level", groupUDF(col("three_level"))).filter("two_level != ''")
            .selectExpr("article_id", "url", "title", "content", "one_level", "two_level", "three_level","length(content) as len")
        }
        val company_limit = all.filter("two_level = 'company'").filter("len < 6000").limit(15000).select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val banking_limit = all.filter("two_level = 'banking'").filter("len < 6000").limit(15000).select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val others_limit = all.filter("two_level not in ('company','banking')").filter("len < 7000").select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val out = company_limit.union(banking_limit).union(others_limit).distinct()
        out
      }
      println(">>>>>>>>>>正在写入数据")
      result.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/business/business_update")
//      result.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/business/business_update_tmp")
      println(">>>>>>>>>>写入数据完成")

    }

    //------------------------------------4 更新娱乐二级分类 -----------------------------------------
    //
    // 3.其表演、综艺节目放others
    def update_entertainment_process(spark: SparkSession,
                                newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                                dt: String="2019-03-13") = {

      val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
          .selectExpr("resource_id as article_id", "article", "title", "url")
          .dropDuplicates("article_id")
      }

      // 历史标注数据处理（包含部分cms的数据，放到年前新标注里面统一处理）
      val gossip_check_path = "news_content/sub_classification/entertainment/gossip_check"
      val gossip_check_df = spark.read.json(gossip_check_path).dropDuplicates("article_id").select("article_id","one_level", "two_level", "three_level")

      val cleanUDF = udf{(word: String) => if (word!=""||word!=null)
        word.toLowerCase()
          .replace("cartoon&comics","comic").replace("comics","comic").replace("comic","cartoon&comics")
      else ""
      }
      // 年前新标注数据处理
      val dt = "2019-03-12"
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df = spark.read.parquet(path)
      val entertainment_new_old = {
        df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time")
          .filter("one_level = 'entertainment'")
          .filter("two_level is not null")
          .dropDuplicates("article_id")
          .withColumnRenamed("two_level","two_level_old")
          .withColumn("two_level",cleanUDF(col("two_level_old")))
          .filter("two_level != ''")
          .select("article_id","one_level", "two_level", "three_level")
      }

      // 合并数据
      val entertainment_tmp = entertainment_new_old.union(gossip_check_df).distinct()
      val entertainment = entertainment_tmp.join(ori_df,Seq("article_id")).withColumn("content", getcontentUDF(col("article.html"))).drop("article")
      // 先写入文件处理
      entertainment.write.mode("overwrite").save("news_content/sub_classification/tmp/entertainment_all")

      val result = {
        val art = Seq("art+culture+history")
        val ce_go = Seq("celebrity","gossip")
        val other = Seq("books","performance","variety show")
        val groupUDF = udf {
          (word: String) =>
            if (art.contains(word)) "arts&culture"
            else if (ce_go.contains(word)) "celebrity&gossip"
            else if (other.contains(word)) "others"
            else word
        }
        val all = {
          spark.read.parquet("news_content/sub_classification/tmp/entertainment_all")
            .withColumnRenamed("two_level", "two_level_new")
            .withColumn("two_level", groupUDF(col("two_level_new")))
            .drop("two_level_new")
            .selectExpr("article_id", "url", "title", "content", "one_level", "two_level", "three_level","length(content) as len")
        }
        val movie = all.filter("two_level = 'movie'").limit(15000).filter("len < 6000").select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val celebrity = all.filter("two_level = 'celebrity&gossip'").limit(15000).filter("len < 6000").select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val bollywood = all.filter("two_level = 'bollywood'").limit(15000).filter("len < 6000").select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val tv = all.filter("two_level = 'tv'").limit(15000).filter("len < 6000").select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val music = all.filter("two_level = 'music'").limit(15000).filter("len < 6000").select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val others = all.filter("two_level not in ('movie','celebrity&gossip','bollywood','tv','music')").select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val out = movie.union(celebrity).union(bollywood).union(tv).union(music).union(others)
      out
      }

      println(">>>>>>>>>>正在写入数据")
      result.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/entertainment/entertainment_update")
      println(">>>>>>>>>>写入数据完成")

    }

    //------------------------------------5 更新汽车二级分类 -----------------------------------------
    // 1.拆分汽车、两轮车
    def update_auto_process(spark: SparkSession,
                                     newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                                     dt: String="2019-02-28") ={

      val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
          .selectExpr("resource_id as article_id", "article", "title", "url")
          .dropDuplicates("article_id")
      }

      // 历史标注数据处理
      val auto_path = "/user/caifuli/news/all_data/auto_classified"
      val auto_ori = spark.read.json(auto_path)
      val cleanUDF = udf{(word: String) => word.toLowerCase().replace(" ","").replace("aoto","auto")}

      val auto_old = {
        val mark_id = auto_ori.select(col("news_id").cast(StringType),col("resource_id"))
        auto_ori.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
          .withColumn("one_level", cleanUDF(col("top_category")))
          .withColumn("two_level", cleanUDF(col("sub_category")))
          .withColumnRenamed("third_category", "three_level")
          .filter("two_level != ''")
          .select("article_id","one_level", "two_level", "three_level")
      }

      // 年前新标注数据处理
      val dt = "2019-03-12"
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val cms_df = spark.read.parquet(path)
      val auto_new = {
        cms_df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time")
          .filter("one_level = 'auto'").filter("two_level is not null")
          .withColumnRenamed("two_level","two_level_new")
          .withColumn("two_level",cleanUDF(col("two_level_new")))
          //          .drop("three_level").withColumn("three_level",lit("others"))
          .select("article_id","one_level", "two_level", "three_level")
      }

      // 合并数据
      val auto_tmp = auto_old.union(auto_new).distinct()
      val auto = auto_tmp.join(ori_df,Seq("article_id")).withColumn("content", getcontentUDF(col("article.html"))).drop("article")
      // 先写入文件处理
      auto.write.mode("overwrite").save("news_content/sub_classification/tmp/auto_all")


      val result = {
        val other = Seq("air-taxi","motor&bike")
        val groupUDF = udf {
          (word: String) =>
            if (other.contains(word)) "others"
            else word.replace("autoindustry","auto industry").replace("autoshow","auto show").replace("cycling","bike")
        }
        val all = {
          spark.read.parquet("news_content/sub_classification/tmp/auto_all")
            .filter("two_level != 'mobilephone'")
            .withColumnRenamed("two_level", "two_level_new")
            .withColumn("two_level", groupUDF(col("two_level_new")))
            .drop("two_level_new")
        }
        all
      }
      println(">>>>>>>>>>正在写入数据")
      result.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/auto/auto_update_tmp")
      println(">>>>>>>>>>写入数据完成")

      // 单独分出自行车和motor

      val df = spark.read.json("news_content/sub_classification_check/tmp/auto_bike*")

      val groupUDF = udf {
        (word: String,predict:String) =>
          if(predict=="motor"||word.contains("motorcycle")||word.contains("motorcycle")) "motor"
          else if (predict=="bike"||word.contains("bike")) "bike"
          else if(word.contains("cycling"))  "bike"
          else "others"
      }
      val bike = {
        df.filter("two_level = 'others'").withColumn("two_level_new",groupUDF(col("content"),col("predict_two_level")))
          .drop("two_level")
          .withColumnRenamed("two_level_new","two_level")
          .select("article_id","url","title","content","one_level","two_level","three_level")
      }

      val result_new = {
        val main_category = df.filter("two_level != 'others'").select("article_id","url","title","content","one_level","two_level","three_level")
        val out = main_category.union(bike).distinct()
        val other = Seq("auto industry","auto show")
        val groupNewUDF = udf {
          (word: String) =>
            if (other.contains(word)) "others"
            else word
        }
        out.withColumnRenamed("two_level", "two_level_new")
          .withColumn("two_level", groupNewUDF(col("two_level_new")))
          .drop("two_level_new")
      }
      println(">>>>>>>>>>正在写入数据")
      result_new.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/auto/auto_update")
      println(">>>>>>>>>>写入数据完成")

    }

    //------------------------------------6 更新体育二级分类 -----------------------------------------
    // 1.去除three_level 为null
    // 2.抽取three_level 为crime、law
    // 3.抽取two_level 为politics、education
    // 4.其他放为others(量不足)

    def update_sports_process(spark: SparkSession,
                                newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                                dt: String="2019-03-12") = {

      val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
          .selectExpr("resource_id as article_id", "article", "title", "url")
      }
      // 历史标注均在cms中
      val dt = "2019-03-12"
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df = spark.read.parquet(path)
      val sports_cms_df = {
        df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time")
          .filter("one_level = 'sports'")
          .select("article_id","one_level", "two_level", "three_level")
      }

      // 写入文件
      val sports = {
        sports_cms_df.join(ori_df, Seq("article_id"))
          .filter("two_level is not null").filter("three_level is not null")
          .withColumn("content", getcontentUDF(col("article.html"))).drop("article")
      }
      sports.write.mode("overwrite").save("news_content/sub_classification/tmp/sports_all")

      val result = {

        val other = Seq("volleybal")
        val groupNewUDF = udf {
          (word: String) =>
            if (other.contains(word)) "others"
            else word
        }

        val all = {
          spark.read.parquet("news_content/sub_classification/tmp/sports_all")
            .withColumnRenamed("two_level", "two_level_new")
            .withColumn("two_level",groupNewUDF(col("three_level")))
            .drop("three_level")
            .withColumnRenamed("two_level_new","three_level")

        }
        val football_limit = all.filter("two_level = 'football'").limit(15000).select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val cricket_limit = all.filter("two_level  = 'cricket'").limit(15000).select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val main_category = all.filter("two_level not in ('football','football')").select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val out = main_category.union(football_limit).union(cricket_limit).distinct()
//        out
        all
      }

      println(">>>>>>>>>>正在写入数据")
      result.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/sports/sports_update")
      println(">>>>>>>>>>写入数据完成")

  }


    //------------------------------------7 更新生活二级分类 -----------------------------------------
    // beauty 放入时尚
    // 动物、植物、宠物放入nature/pets(第二次迭代去掉，人工标注错误率大，e.g. 原文为狮子座的香奈儿，珠宝 人工标为动物。机器分类正确，
    // 文章讲芦荟可以用做卸妆等材料。主要讲护肤品，人工标注为植物,机器预测正确
    // shopping guide(第二次迭代去掉，主要为衣物和3c，容易和珠宝服饰混)
    // 健康分出weight loss&diet、fitness&yoga
    // 时尚分出skin care&makeup
    // 剩下的放others(offbeat\DIY\humor\shopping guide\envts)

    def update_lifestyle_process(spark: SparkSession,
                              newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                              dt: String="2019-03-12") = {

      val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
          .selectExpr("resource_id as article_id", "article", "title", "url")
      }
      // 历史标注均在cms中
      val dt = "2019-03-12"
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df = spark.read.parquet(path)
      val lifestyle_cms_df = {
        df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time")
          .filter("one_level = 'lifestyle'")
          .select("article_id","one_level", "two_level", "three_level")
      }

      // 写入文件
      val lifestyle = {
        lifestyle_cms_df.join(ori_df, Seq("article_id"))
          .filter("two_level is not null")
          .withColumn("content", getcontentUDF(col("article.html"))).drop("article")
      }
      lifestyle.write.mode("overwrite").save("news_content/sub_classification/tmp/lifestyle_all")

      val result = {

        val main_word = Seq("health", "fashion&trends", "travel", "food&wine", "relationships", "parenting", "beauty", "shopping guide", "nature&pets",
          "astrology","books","arts&culture","shopping guide","home&garden")
        val health_word = Seq("fitness & yoga", "weight loss & diet", "fitness", "yoga","diet", "weight loss")
        val fashion_word = Seq("skin care","makeup")
        val groupUDF = udf{
          (word1:String, word2:String) =>
            if(!main_word.contains(word1)) "others"
            else if(word1 == "health"&&health_word.contains(word2)) word2.replace("fitness & yoga","fitness").replace("yoga", "fitness").replace("fitness", "fitness&yoga").replace("weight loss & diet","weight loss").replace("diet","weight loss").replace("weight loss", "weight loss&diet")
            else if(word1 == "fashion&trends"&& fashion_word.contains(word2)) word2.replace("skin care","makeup").replace("makeup", "skin care&makeup")
            else word1
        }
        val replaceUDF = udf{(word: String) =>
          word.toLowerCase()
            .replace("fashion & trends", "fashion").replace("fashion","fashion&trends").replace("beauty","fashion&trends")
            .replace("arts & culture","arts").replace("arts", "art").replace("culture", "art").replace("art","arts&culture")
            .replace("food & wine","food").replace("food","food&wine").replace("home & garden","home").replace("home", "home&garden")
            .replace("love&sex", "relationships").replace("nature-animals", "nature").replace("nature-plants","nature").replace("pets", "nature").replace("nature","nature&pets")
            .replace("shopping guide(clothes &3c)", "shopping guide").replace("shopping guide","shopping").replace("shopping", "shopping guide")
            .replace("reigion & spirituality", "reigion&spirituality")
            .replace("zodiac", "astrology")
        }

        val all = {
          spark.read.parquet("news_content/sub_classification/tmp/lifestyle_all")
            .filter("three_level is not null")
            .withColumnRenamed("two_level","two_level_old")
//            .withColumn("two_level", replaceUDF(col("two_level_old")))
            .withColumn("two_level", groupUDF(replaceUDF(col("two_level_old")), col("three_level")))
            .filter("two_level not in ('shopping guide')")
            .select("article_id","url","title","content","one_level","two_level","three_level")
        }
        all
      }

      println(">>>>>>>>>>正在写入数据")
      result.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/lifestyle/lifestyle_update")
      println(">>>>>>>>>>写入数据完成")

    }

    //------------------------------------8 更新世界二级分类 -----------------------------------------


    def update_international_process(spark: SparkSession,
                                 newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                                 dt: String="2019-03-12") = {

      val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
          .selectExpr("resource_id as article_id", "article", "title", "url")
      }
      // 历史标注均在cms中
      val dt = "2019-03-12"
      val path = "/user/hive/warehouse/apus_ai.db/recommend/article/readmongo/dt=%s".format(dt)
      val df = spark.read.parquet(path)
      val international_cms_df = {
        df.drop("_class", "_id", "article_doc_id", "is_right", "op_time", "server_time")
          .filter("one_level = 'World'").filter("two_level is not null").filter("three_level is not null")
          .select("article_id","one_level", "two_level", "three_level")
      }

      // 写入文件
      val international = {
        international_cms_df.join(ori_df, Seq("article_id"))
          .withColumn("content", getcontentUDF(col("article.html"))).drop("article")
      }
      international.write.mode("overwrite").save("news_content/sub_classification/tmp/international_all")

      val result = {
        val main_word = Seq("Politics", "politic", "Society", "Military", "others", "Terrorism",
          "Environment","weather")
        //        val three_level_word = Seq("Crime","Policies and Regulations","Diplomacy","Government")
        val three_level_word = Seq("Crime")
        val replaceUDF = udf{(word1: String, word2:String) =>
          if(!main_word.contains(word1)) ""
          else if (three_level_word.contains(word2))
            word2.toLowerCase().replace("policies and regulations","policies&regulations")
              .replace("government","diplomacy")
              .replace("diplomacy","government&diplomacy")
          else word1.toLowerCase()
            .replace("weather", "environment").replace("politics","politic")
            .replace("politic","politics")
        }

        val all = {
          spark.read.parquet("news_content/sub_classification/tmp/international_all")
            .drop("one_level")
            .withColumn("one_level",lit("international"))
            .withColumnRenamed("two_level","two_level_old")
            .withColumn("two_level", replaceUDF(col("two_level_old"),col("three_level")))
            .filter("two_level != ''")
            .selectExpr("article_id","url","title","content","one_level","two_level","three_level", "length(content) as len")
        }
        val politics_limit = all.filter("two_level = 'politics'").filter("len < 6000").limit(20000).select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val society_limit = all.filter("two_level  = 'society'").filter("len < 6000").limit(20000).select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val main_category = all.filter("two_level not in ('politics','society')").select("article_id", "url", "title", "content", "one_level", "two_level", "three_level")
        val out = main_category.union(politics_limit).union(society_limit).distinct()
          out
//        all
      }

      println(">>>>>>>>>>正在写入数据")
      result.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/international/international_update")
      println(">>>>>>>>>>写入数据完成")

    }


    //------------------------------------9 更新科学分类 -----------------------------------------


    def update_science_process(spark: SparkSession,
                                     newsPath: String="/user/hive/warehouse/apus_dw.db/dw_news_data_hour",
                                     dt: String="2019-03-19") = {

      val newsPath = "/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged"
      val getcontentUDF = udf { (html: String) => Jsoup.parse(html).text() }
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_ai.db/recommend/article/article_data_merged/dt=*")
          .selectExpr("resource_id as article_id", "article", "title", "url")
      }
      // 历史标注文件
      val dt = "2019-03-19"
      val science_path = "/user/caifuli/news/all_data/science"
      val science_ori = spark.read.json(science_path)
      val cleanUDF = udf{(word: String) => word.toLowerCase().replace(" ","")}

      val science = {
        val mark_id = science_ori.select(col("news_id").cast(StringType),col("resource_id"))
        science_ori.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
          .withColumn("one_level", cleanUDF(col("top_category")))
          .withColumn("two_level", cleanUDF(col("sub_category")))
          .withColumnRenamed("third_category", "three_level")
          .filter("two_level != ''")
          .select("article_id","one_level", "two_level", "three_level")
      }
      // 写入文件
      val science_tmp = {
        science.join(ori_df, Seq("article_id"))
          .withColumn("content", getcontentUDF(col("article.html"))).drop("article")
      }
      science_tmp.write.mode("overwrite").save("news_content/sub_classification/tmp/science_all")

      val result = {
        val all = {
          spark.read.parquet("news_content/sub_classification/tmp/science_all")
        }
        all
      }

      println(">>>>>>>>>>正在写入数据")
      result.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/science/science_update")
      println(">>>>>>>>>>写入数据完成")

    }

  }
}
