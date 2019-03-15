package com.algo.nlp

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.clulab.processors.Document
import org.clulab.processors.fastnlp.FastNLPProcessor
import org.jsoup.Jsoup


/**
  * Created by Joshua on 2018-12-25
  */
object NewsNgrams {

  def ngramsSlice(inp_seq: Seq[String], minSize: Int = 1, maxSize: Int= 4): Seq[String] = {
    var NGrams_result = Seq.empty[String]

    for(j <- minSize to maxSize) {
      for(i <- inp_seq.indices){
        val b = i
        val e = if(i+j < inp_seq.size) i+j else inp_seq.size
        //        println("slice: ",inp_seq.slice(b,e).toArray.deep)
        //        println("string: ",inp_seq.slice(b,e).mkString(" "))
        if(inp_seq.slice(b,e).length == j){
          NGrams_result = NGrams_result :+ inp_seq.slice(b,e).mkString(" ")
        }
      }
    }
    NGrams_result
  }

  def getLonger(keyword:Seq[String]):Seq[String] = {
    if(keyword.nonEmpty){
      // 取长不取短
      val tmp_filtered = keyword.map(" "+_+" ")
      val result_filtered = tmp_filtered.filter(w => !tmp_filtered.diff(Seq(w)).exists(_.contains(w))).map(_.trim)
      result_filtered
    }else keyword
  }




  def main(args: Array[String]): Unit = {
    val appName = "NewsNgrams"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val dt = "2018-11-22"
    val dataPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour"
    val dataDtPath = "/user/hive/warehouse/apus_dw.db/dw_news_data_hour/dt=%s".format(dt)
    val stopwordsPath = "/user/zoushuai/news_content/stopwords.txt"

    //------------------------------------1 加载停用词-----------------------------------------
    // Load the stop words
    // List found on: /user/zoushuai/news_content/stopwords.txt
    val stopwords_bd = {
      val stopwords = {
        println("loading stopwords ...")
        val stopwords_fromFile = sc.textFile(stopwordsPath).collect().tail.toSet
        val stopwords_manully = {
          Set("-rsb-","-lsb-","-rrb-","-lrb-","\\|","--") ++
            Set("to","as","in","from","for","on","of").map(_+" the") ++
            Set("first","second","third").map("the "+_)
        }
        (stopwords_fromFile++stopwords_manully).map((_,1.0)).toMap
      }
      sc.broadcast(stopwords)
    }

    //------------------------------------2 获取ngram核心函数-----------------------------------------
    /**
      * spark: SparkSession
      * dataPath：原始数据路径
      * savePath：存储路径
      * stopwords_bd: 停用词broadcast
      */
    def getNGramDF(spark: SparkSession,
                   dataPath: String,
                   savePath: String = "NgramsDf",
                   stopwords_bd: Broadcast[Map[String,Double]]) = {
      val sc = spark.sparkContext
      import spark.implicits._

      // load dataDF
      val inpDF = {
        val html_udf = udf { (html: String) => Jsoup.parse(html).text() }
        spark.read.parquet(dataDtPath).filter("country_lang = 'IN_en'")
          .selectExpr("resource_id as article_id", "url as article_url", "html", "title", "tags")
          .withColumn("article", html_udf(col("html")))
          .dropDuplicates("article")
      }

      val N_gramDF_ = ngramWithManuallySplit_v2(inpDF, stopwords_bd)
      // write
      N_gramDF_.cache
      N_gramDF_.write.mode("overwrite").parquet(savePath)
    }

    //------------------------------------3 分词方法（1）-----------------------------------------
    // FastNLPProcessor split AND generate N_gram | 词形还原归一,去除停用词,去除数字
    /**
      * inpDF: SparkSession
      * stopwords_bd: 停用词broadcast
      */

    def ngramsWithFastNLP(inpDF:DataFrame, stopwords_bd:Broadcast[Map[String,Double]]):DataFrame = {
      inpDF.rdd.mapPartitions{
        rs =>
          val proc = new FastNLPProcessor()
          rs.map{
            r =>
              val article_id = r.getAs[String]("article_id")
              val text = r.getAs[String]("article")
              val doc: Document = proc.mkDocument(text)
              proc.tagPartsOfSpeech(doc)
              proc.lemmatize(doc)
              doc.clear()
              val lemmas = doc.sentences.flatMap{
                sentence =>
                  sentence.lemmas.getOrElse(Seq.empty[String]).asInstanceOf[Array[String]]
              }.filter(x => !stopwords_bd.value.contains(x)).filter(x => "[0-9,\\.]+".r.findAllIn(x).isEmpty)
              val ngram = ngramsSlice(lemmas, 1, 4)
              (article_id, text, ngram)
          }
      }.toDF("article_id","article","ngrams").filter("size(ngrams) > 0")
    }




    //------------------------------------3 分词方法（2）-----------------------------------------
    // Manual split AND generate N_gram | 去除数字,长度为1的字符 |
    // Ngram后再启用停用词因为类似作品名会有 "pump it up",可以去掉一元词"up"不能去掉三元词;
    // 去掉了长度不足3或超过40的 n元词
    /**
      * inpDF: SparkSession
      * stopwords_bd: 停用词broadcast
      */

    def ngramWithManuallySplit(inpDF:DataFrame, stopwords_bd:Broadcast[Map[String,Double]]):DataFrame = {
      val max_str_length = 40 // 四元词加上空格最长认为不超过40, 超长的如the caste/other reservation certificates也不适合做标签用
      val min_word_lenth = 3 // n元词中,一个词最小长度3个字符 (不然会有很多姓名中间的 Le Li Aj这种一元的,当他们组成三元词的时候长度自然就够了

      inpDF.rdd.mapPartitions{
        rs =>
          rs.map{
            r =>
              val article_title = r.getAs[String]("title")
              val title_ngram = {
                val clean_title=article_title.replaceAll("((?<=\\s+)[^\\p{L}\"\']+(?=\\s*)|(?<=\\s*)[^\\p{L}\"\']+(?=\\s+))"," ").replace("\\s+"," ")
                val ngram = ngramsSlice(clean_title.split(" "), 1, 4).filter(_.length > 1)
                val quotes_in_title = "(\"[^\"]+\"|\'[^\']+\')".r.findAllIn(article_title).map(str=> str.slice(1,str.length-1).trim).toSeq.filter(_.length<=max_str_length)
                val abbrs = "(?<=\\s+)\\p{Lu}[^\\sa-z]+".r.findAllIn(article_title).toSeq
                ngram ++ quotes_in_title ++ abbrs
              }.map(_.replaceAll("^[^\\p{L}0-9]+|[^\\p{L}0-9]+$"," ").replaceAll("\\s+"," ").trim/*.toLowerCase*/)
                .filter(!stopwords_bd.value.contains(_))
                .filter(str => !str.split(" ").forall(x => stopwords_bd.value.contains(x))) // 不能完全由停用词组成

              val article_id = r.getAs[String]("article_id")
              val text = r.getAs[String]("article")

              // text前后加上空格有助于匹配到行首的符号如开头是:(Photo: Marvel Entertainment)
              // 去掉前后有分隔符的标点符号(为了避免把 I'm your're替换掉,也避免把缩写的点替换掉 如 S.H.I.E.L.D)
              //
              // (分隔符前不是字母|分隔符后不是字母) | \w包含下划线和数字, \p{P}又不包含竖线"|"
              val clean_text = {
                (" "+text+" ")
                  .replaceAll("(type src=)?http(s)?://[^\\s+\"]+", " ")
                  .replaceAll("\\w+(-)?\\w+=\"[^\"]+(?=\")", " ")
                  .replaceAll("(\\s*[^\\p{L}]+(?=\\s+)|(?<=\\s+)[^\\p{L}]+\\s*)", " ")
                  .replaceAll("\\s+"," ")
              }
              val content_ngram = {
                val ngram = ngramsSlice(clean_text.split(" ").filter(_.length>1)/*.map(_.toLowerCase)*/, 1, 4).filter(!stopwords_bd.value.contains(_)).filter(x => x.length<=max_str_length && x.length>=min_word_lenth)
                val certain_words = {
                  // 匹配引号引用的词,去掉引用词前后的符号 | 长度限制,引号里可能是html标记
                  val quotes="(\"[^\"]+\"|\'[^\']+\')".r.findAllIn(text).map(str=> str.slice(1,str.length-1).trim).toSeq.filter(_.length<=max_str_length)
                  // 匹配全大写的缩写 如 MD&M  S.H.I.E.L.D SABIC
                  val abbrs = "(?<=\\s+)\\p{Lu}[^\\sa-z]+".r.findAllIn(article_title).toSeq
                  quotes ++ abbrs
                }
                ngram ++ certain_words
              }.map(_.replaceAll("^[^\\p{L}0-9]+|[^\\p{L}0-9]+$"," ").replaceAll("\\s+"," ").trim/*.toLowerCase*/)
                .filter(!stopwords_bd.value.contains(_))
                .filter(str => !str.split(" ").forall(x => stopwords_bd.value.contains(x))) // 不能完全由停用词组成
              (article_id, text, content_ngram, title_ngram, content_ngram ++ title_ngram)
          }
      }.toDF("article_id", "article", "content_ngram", "title_ngram", "n_gram").filter("size(n_gram) > 0")
    }


    //------------------------------------3 分词方法（3）-----------------------------------------
    // pre:
    //    按后有分隔符的句号作为split条件,分割句子 | split("\\.(?=\\s)") | 注:有些没有打句号的,比如很短的各个目录和标签,可以通过html标签各自的内容分开获取,但是jsoup在获取<div>中嵌套有<span>时,<span>包裹的会成为数组另一个元素,因为div的ownText不包括span里的,然而实际文章中<span>包裹的其实还是div里内容的一部分,比如说日期
    // 整体策略:
    //    a = 直接空格分词做N-gram | 去除首尾空格
    //    b = 替换特殊符号做N-gram | 去除首尾空格
    //    result = (a+b.diff(a)).intersect(wikiDict)
    // 替换规则:
    //    将 前(或后)是分隔符的[非字母数字] 替换为空格 | "((?<=\\s+)[^\\p{L}\\p{N}]+(?=\\s*)|(?<=\\s*)[^\\p{L}\\p{N}]+(?=\\s+))"
    // 过滤策略:
    //    n-gram按空格拆分不能全由停用词组成 | .filter(str => !str.split(" ").forall(x => stopwords_bd.value.contains(x)))
    //    n-gram一(多)元词,整个词必须有字母 | .filter(str =>  "\\p{L}".r.findAllIn(str).nonEmpty)
    //    取消此条,因为有 Fast & Furious 这种文章里就用&符号或者数字的,n-gram按空格拆分每一个词都要含有字母 | .filter(str =>  str.split(" ").forall(x => "\\p{L}".r.findAllIn(x).nonEmpty))
    //    取消此条,考虑 &TV 这种就是字符加上字母的一元词, n-gram一元词不能是直接字符结尾 |
    //    n-gram词的字符长度限制在 3 ~ 40
    // 特殊匹配:
    //    匹配全大写且前后有分隔符的,作为缩写词,如 S.H.I.E.L.D X-23 MD&M | "(?<=\\s+)\\p{Lu}[^\\sa-z]+" | 注:这里[^\\sa-z]已经确定大写字母后面不能跟空格了,所以没必要写(?=\\s)
    //    匹配单(双)引号内的内容 | "(\"[^\"]+\"|\'[^\']+\')" | 注:和括号匹配不同,这里不能用预匹配,因为正反引号是相同的,如果预匹配不把引号用掉的话,会被下一个字符匹配上比如,abc"def"ghjkl"mn",应该是"def" "mn",但是采用预匹配得到"def" "ghjkl" "mn"
    //    匹配括号内的内容 |  "(?<=\\()[^)]+(?=\\))"

    /**
      * inpDF: 原始数据
      * stopwords_bd: 停用词broadcast
      */
    def ngramWithManuallySplit_v2(inpDF:DataFrame, stopwords_bd:Broadcast[Map[String,Double]]):DataFrame = {
      val max_str_length = 90 // 四元词加上空格最长认为不超过90, 最长的单词是肺尘病长度为45
      val min_word_lenth = 3 // n元词中,一个词最小长度3个字符 (不然会有很多姓名中间的 Le Li Aj这种一元的,当他们组成三元词的时候长度自然就够了

      // val r = inpDF.sample(false,0.1).head
      inpDF.rdd.mapPartitions{
        rs =>
          rs.map{
            r =>
              val article_id = r.getAs[String]("article_id")
              val article_title = " " + r.getAs[String]("title") + " "
              val text = " " + r.getAs[String]("article") + " "

              val title_Ngram_ = {
                val direct_ngram = ngramsSlice(article_title.split(" "))
                val replaced_ngram = {
                  val replaced = {
                    (" " + article_title + " ")
                      .replaceAll("((?<=\\s+)[^\\p{L}\\p{N}]+(?=\\s*)|(?<=\\s*)[^\\p{L}\\p{N}]+(?=\\s+))", " ")
                      .replaceAll("\\s+"," ")
                      .trim.split(" ")
                  }
                  ngramsSlice(replaced)
                }.diff(direct_ngram)
                val quotes = "(\"[^\"]+\"|\'[^\']+\')".r.findAllIn(article_title).map(s => s.slice(1,s.length-1).trim).toSeq.filter(_.length<=max_str_length)
                val abbrs = "(?<=\\s+)\\p{Lu}[^\\sa-z]+".r.findAllIn(article_title).toSeq
                val nested = "(?<=\\()[^)]+(?=\\))".r.findAllIn(article_title).toSeq
                val tmpResult = direct_ngram ++ replaced_ngram ++ quotes ++ abbrs ++ nested
                tmpResult
                  .filter(str =>  "\\p{L}".r.findAllIn(str).nonEmpty)
                  .filter(str => !str.split(" ").forall(x => stopwords_bd.value.contains(x.toLowerCase)))
                  .map(_.trim/*.toLowerCase*/)
                  .filter(str => str.length >= min_word_lenth && str.length <= max_str_length)
              }
              // 临时处理 's 的情况,不能直接替换掉,因为有电影名是带's的,构造一个不带's的新词
              val noun = title_Ngram_.map{
                w =>
                  val nounOption = "(?<=\\s+)\\p{Lu}+(?=[‘`']s)".r.findAllIn(w)
                  if(nounOption.nonEmpty) nounOption.mkString("") else null
              }.filter(_!=null)
              val nounLongerOne_InTitle = getLonger(noun)
              val title_Ngram = nounLongerOne_InTitle++title_Ngram_

              val sentence_list = text.split("\\.(?=\\s)")
              val text_NGram_ = {
                val direct_ngram = sentence_list.flatMap(s => ngramsSlice(s.trim.split(" ")))
                val replaced_ngram = sentence_list.flatMap{
                  sentence =>
                    val replaced = {
                      (" "+sentence+" ")
                        .replaceAll("((?<=\\s+)[^\\p{L}\\p{N}]+(?=\\s*)|(?<=\\s*)[^\\p{L}\\p{N}]+(?=\\s+))", " ")
                        .replaceAll("\\s+"," ")
                        .trim.split(" ")
                    }
                    ngramsSlice(replaced)
                }.diff(direct_ngram)
                val quotes = sentence_list.flatMap{
                  sentence =>
                    "(\"[^\"]+\"|\'[^\']+\')".r.findAllIn(sentence)
                      .map(s => s.slice(1,s.length-1).trim)
                      .toSeq.filter(_.length<=max_str_length)
                }
                val abbrs = sentence_list.flatMap{
                  sentence =>
                    "(?<=\\s+)\\p{Lu}[^\\sa-z]+".r.findAllIn(sentence).toSeq
                }
                val nested = sentence_list.flatMap{
                  sentence =>
                    "(?<=\\()[^)]+(?=\\))".r.findAllIn(sentence).toSeq
                }
                val tmpResult = direct_ngram ++ replaced_ngram ++ quotes ++ abbrs ++ nested
                tmpResult
                  .filter(str =>  "\\p{L}".r.findAllIn(str).nonEmpty)
                  .filter(str => !str.split(" ").forall(x => stopwords_bd.value.contains(x.toLowerCase)))
                  .map(_.trim/*.toLowerCase*/)
                  .filter(str => str.length >= min_word_lenth && str.length <= max_str_length)
              }
              // 临时处理 's 的情况,不能直接替换掉,因为有电影名是带's的,构造一个不带's的新词
              val noun_InContent = text_NGram_.map{
                w =>
                  val nounOption = "(?<=\\s+)\\p{Lu}+(?=[‘`']s)".r.findAllIn(w)
                  if(nounOption.nonEmpty) nounOption.mkString("") else null
              }.filter(_!=null)
              val nounLongerOne_InContent = getLonger(noun_InContent)
              val text_NGram = nounLongerOne_InContent++text_NGram_

              (article_id,text,text_NGram,title_Ngram,text_NGram++title_Ngram)
          }
      }.toDF("article_id","article","content_ngram","title_ngram","n_gram").filter("size(n_gram) > 0")
    }
  }
}
