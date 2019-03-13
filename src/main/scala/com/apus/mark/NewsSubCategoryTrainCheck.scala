package com.apus.mark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
  * Created by Joshua on 2019-01-16
  */
object NewsSubCategoryTrainCheck {
  def main(args: Array[String]): Unit = {

    //------------------------------------1 财经数据分类查看 -----------------------------------------
    //
    def business_fix(spark: SparkSession,
                       newsPath: String,
                       dt: String = "2019-01-15") = {
      val newsPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"
      val ori_df = {
        spark.read.option("basePath", newsPath).parquet("/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=2018-11-2[2-6]")
          .selectExpr("resource_id as article_id", "url")
      }

      val train = spark.read.json("news_content/subcategory_check/business_v1_train")
      //      val train_df = train.join(ori_df, Seq("article_id"))
      val train_df = spark.read.json("news_content/subcategory_check/business_v1_train")
      val out1 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'market' and predict_two_level = 'industry economic'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out2 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'investment' and predict_two_level = 'industry economic'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out3 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'company' and predict_two_level = 'industry economic'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out4 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'others' and predict_two_level = 'industry economic'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out5 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'others' and predict_two_level = 'investment'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out6 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'industry economic' and predict_two_level = 'company'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out7 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'industry economic' and predict_two_level = 'company'").limit(5)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out8 = {
        train_df.filter("two_level != predict_two_level")
          .filter("two_level = 'others' and predict_two_level = 'market'").limit(10)
          .select("article_id", "url", "title", "content", "two_level", "predict_two_level", "predict_two_level_proba")
      }
      val out = out1.union(out2).union(out3).union(out4).union(out5).union(out6).union(out7).union(out8)
      out.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/tmp/out3")

    }


    //------------------------------------2 世界数据查看国家 -----------------------------------------
    //
    // US,China,Pakistan,Korea,North,India,Russia,UK,Syria,Iran,Saudi,Israel,UN

    def world_check_country(spark: SparkSession) = {
      val path = "/user/caifuli/news/all_data/international_classified"

      val markDF = spark.read.json(path)

      val mark_id = markDF.select(col("news_id").cast(StringType), col("resource_id"))
      val markall = {
        val seqUDF = udf((t: String) => Seq.empty[String])
        val cleanUDF = udf((level: String) => if (level == null || level.replace(" ", "") == "") "others" else level.trim().toLowerCase) // 增加清洗分类
        markDF.withColumn("article_id", concat_ws("", mark_id.schema.fieldNames.map(col): _*))
          .selectExpr("article_id", "title", "url as article_url", "top_category", "sub_category", "third_category", "length(content) as article_len", "length(html) as html_len")
      }.dropDuplicates("article_id", "title")
      val title_df = {
        markall.rdd.flatMap {
          row =>
            val title = row.getAs[String]("title").split(" ")
            title
        }.map(word => (word, 1)).filter(_._1.length > 1).filter(_._1.slice(0, 1).map(_.toByte).toList(0) <= 90).reduceByKey(_ + _).sortBy(_._2, ascending = false)
      }
      title_df.coalesce(1).saveAsTextFile("news_content/tmp/country_count")
    }


    //------------------------------------3 娱乐数据清除重新训练模型 -----------------------------------------
    //
    def entertainment_check(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/entertainment*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.6").filter("two_level in ('celebrity&gossip','bollywood','tv','movie')").select("article_id")
      val drop2 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.6").filter("two_level in ('celebrity&gossip','bollywood','tv','movie')").select("article_id")
      val drop = drop1.union(drop2).withColumn("drop",lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/entertainment/entertainment_all")
      val new_ori = ori_df.join(drop, Seq("article_id"), "left").filter("drop is null").drop("drop")
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/entertainment/entertainment_all_v1")
    }

    def entertainment_update_check(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/entertainment_update*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.4").filter("two_level in ('celebrity', 'gossip','bollywood','tv','movie')").select("article_id")
      val drop2 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.7").filter("two_level in ('celebrity', 'gossip','bollywood','tv','movie')").select("article_id")
      val drop = drop1.union(drop2).withColumn("drop",lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/entertainment/entertainment_update_tmp")
      val new_ori = ori_df.join(drop, Seq("article_id"), "left").filter("drop is null").drop("drop")
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/entertainment/entertainment_all_v1")
    }


    //------------------------------------4 科技数据清除重新训练模型 -----------------------------------------
    //

    def tech_check(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/tech*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.5").filter("two_level in ('mobile phone')").select("article_id")
      val drop2 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.7").filter("two_level in ('mobile phone','gadget')").select("article_id")
      val drop = drop1.union(drop2).withColumn("drop",lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/tech/tech_all")
      val new_ori = ori_df.join(drop, Seq("article_id"), "left").filter("drop is null").drop("drop")
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/tech/tech_all_v1")

    }

    def tech_check_update(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/tech_update*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.6").filter("two_level in ('mobile phone','others')").select("article_id")
      val drop2 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.3").filter("two_level not in ('mobile phone','others')").select("article_id")
      val drop3 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.5").select("article_id")
      val drop = drop1.union(drop2).union(drop3).withColumn("drop",lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/tech/tech_update")
      val new_ori = ori_df.join(drop, Seq("article_id"), "left").filter("drop is null").drop("drop")
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/tech/tech_update_v1")

    }

    //------------------------------------5 汽车数据清除重新训练模型 -----------------------------------------
    //

    def auto_check(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/auto*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.6").select("article_id")
      val drop2 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.7").select("article_id")
      val drop = drop1.union(drop2).withColumn("drop", lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/auto/auto_all")
      val new_ori = ori_df.join(drop, Seq("article_id"), "left").filter("drop is null").drop("drop")
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/auto/auto_all_v1")
    }

    def auto_update_check(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/auto_update*")
      val drop1 = df.filter("two_level = predict_two_level and two_level != 'others'").filter("predict_two_level_proba < 0.55").select("article_id")
      val drop2 = df.filter("two_level != predict_two_level and two_level != 'others'").filter("predict_two_level_proba > 0.65").select("article_id")
      val drop = drop1.union(drop2).withColumn("drop", lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/auto/auto_update")
      val new_ori = ori_df.join(drop, Seq("article_id"), "left").filter("drop is null").drop("drop")
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/auto/auto_update_v1")
    }


    //------------------------------------6 财经数据清除重新训练模型 -----------------------------------------
    //

    def business_check(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/business*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.4").select("article_id")
      val drop2 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.8").select("article_id")
      val drop = drop1.union(drop2).withColumn("drop", lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/business/business_all")
      val new_ori = ori_df.join(drop, Seq("article_id"), "left").filter("drop is null").drop("drop")
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/business/business_all_v1")
    }

    def business_update_check(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/business_update*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.4").filter("two_level in ('economy','stock&bond','market','invest')").select("article_id")
      val drop2 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.65").select("article_id")
      val drop = drop1.union(drop2).withColumn("drop", lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/business/business_update")
      val two_level_replace = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.7").selectExpr("article_id","predict_two_level as two_level")
      val ori_replace = ori_df.drop("two_level").join(two_level_replace, Seq("article_id")).select("article_id","url","title","content","one_level","two_level","three_level")
      val new_ori = ori_df.join(drop, Seq("article_id"), "left").filter("drop is null").drop("drop").select("article_id","url","title","content","one_level","two_level","three_level")
      val result = new_ori.union(ori_replace).distinct()
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/business/business_update_v1")
//      result.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/business/business_update_tmp_v1")
    }

    //------------------------------------7 国内数据清除重新训练模型 -----------------------------------------
    //

    def national_check(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/national*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.5").filter("two_level != 'society others'").select("article_id")
      val drop2 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.6").filter("two_level != 'society others'").select("article_id")
      val drop = drop1.union(drop2).withColumn("drop", lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/national/national_all")
      val new_ori = ori_df.join(drop, Seq("article_id"), "left").filter("drop is null").drop("drop")
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/national_all_v1")
    }


    def national_update_check(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/national_update*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.6").filter("two_level in ('society','politics')").select("article_id")
      val drop2 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.6").select("article_id")
      val drop = drop1.union(drop2).withColumn("drop", lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/national/national_update")
      val new_ori = ori_df.join(drop, Seq("article_id"), "left").filter("drop is null").drop("drop")
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/national/national_update_v1")
    }

    //------------------------------------8 生活数据清除重新训练模型 -----------------------------------------
    //
    def lifestyle_check(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/lifestyle*")
//      val df = spark.read.json("news_content/sub_classification_check/tmp/lifestyle*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.5").filter("two_level in ('health','others','weight loss&diet','parenting','skin care&makeup')").select("article_id")
      val drop2 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.5").filter("two_level not in  ('health','others','weight loss&diet','parenting','skin care&makeup')").select("article_id")
      val drop3 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.4").filter("two_level in ('health','others','weight loss&diet','parenting','skin care&makeup')").select("article_id")
      val drop4 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba < 0.6").filter("two_level not in ('health','others','weight loss&diet','parenting','skin care&makeup')").select("article_id")

      val drop_df = drop1.union(drop2).union(drop3).union(drop4).withColumn("drop", lit(1))
//      val drop_df = drop1.union(drop3).withColumn("drop", lit(1))

      val ori_df = spark.read.json("news_content/sub_classification/lifestyle/lifestyle_all")
//      val two_level_replace = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.6").selectExpr("article_id","predict_two_level as two_level")
//      val ori_replace = ori_df.drop("two_level").join(two_level_replace, Seq("article_id")).select("article_id","url","title","content","one_level","two_level","three_level")
      val new_ori = ori_df.join(drop_df, Seq("article_id"), "left").filter("drop is null").drop("drop").select("article_id","url","title","content","one_level","two_level","three_level")
//      val result = new_ori.union(ori_replace).distinct()
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/lifestyle/lifestyle_all_v1")
    }

    //------------------------------------8 生活数据清除重新训练模型(二次迭代) -----------------------------------------
    //
    def lifestyle_check_v1(spark: SparkSession) = {
//      val df = spark.read.json("news_content/sub_classification_check/lifestyle*")
      val df = spark.read.json("news_content/sub_classification_check/tmp/lifestyle*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.4").select("article_id")
      val drop2 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba < 0.5").select("article_id")
      val drop_df = drop1.union(drop2).withColumn("drop", lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/lifestyle/lifestyle_all_v1")
      ori_df.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/lifestyle/lifestyle_all_v2")
    }

    //------------------------------------9 世界数据清除重新训练模型 -----------------------------------------
    //
    def international_check(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/international*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.6").filter("two_level in ('politics','society')").select("article_id")
      val drop2 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.5").filter("two_level not in ('politics','society')").select("article_id")
      val drop3 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.7").filter("two_level in ('politics','society')").select("article_id")
      val drop4 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.7").filter("two_level not in ('politics','society')").select("article_id")
      val drop_df = drop1.union(drop2).union(drop3).union(drop4).withColumn("drop", lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/international/international_all_7")
      val new_ori = ori_df.join(drop_df, Seq("article_id"), "left").filter("drop is null").drop("drop")
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/international/international_all_tmp_v1")
    }

    //------------------------------------10 体育数据清除重新训练模型 -----------------------------------------
    //
    def sports_update_check(spark: SparkSession) = {
      val df = spark.read.json("news_content/sub_classification_check/sports_update*")
      val drop1 = df.filter("two_level = predict_two_level").filter("predict_two_level_proba < 0.7").filter("two_level in ('football','cricket','auto racing','cricket')").select("article_id")
      val drop2 = df.filter("two_level != predict_two_level").filter("predict_two_level_proba > 0.65").select("article_id")
      val drop_df = drop1.union(drop2).withColumn("drop", lit(1))
      val ori_df = spark.read.json("news_content/sub_classification/sports/sports_update")
      val new_ori = ori_df.join(drop_df, Seq("article_id"), "left").filter("drop is null").drop("drop")
      new_ori.coalesce(1).write.format("json").mode("overwrite").save("news_content/sub_classification/sports/sports_update_v1")
    }



  }
}