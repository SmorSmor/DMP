package com.Rpt

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object OperatorRpt {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc: SparkContext = new SparkContext(conf)

    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val df: DataFrame = spark.read.parquet("D://out-0820-01")
    import spark.implicits._

    val load: Config = ConfigFactory.load()
    // 获取数据库链接信息
    val prop = new Properties()
    prop.put("user", load.getString("jdbc.user"))
    prop.put("password", load.getString("jdbc.password"))
    val url = load.getString("jdbc.url")

    df.map(row => {
      // 把需要的字段全部取出
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      // key 值是地域省份
      val ispname: String = row.getAs[String]("ispname")

      // 创建三个方法处理三个指标
      val request = com.utils.RptUtils2.request(requestmode, processnode)
      val click = com.utils.RptUtils2.click(requestmode, iseffective)
      val ad = com.utils.RptUtils2.AD(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      // 返回值4个元组
      (
        ispname,
        request ++ click ++ ad
      )
    }).rdd //转为rdd
      .reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(res => {
      (res._1, res._2(0), res._2(1), res._2(2), res._2(3), res._2(4), res._2(5), res._2(6), res._2(7), res._2(8))
    })
      .toDF("运营商", "总请求", "有效请求", "广告请求", "参与竞价数", "竞价成功数", "展示数", "点击数", "广告成本", "广告消费")
      .write.mode("append").jdbc(url, "OperatorRpt", prop)
    //      .sortBy(_._2(0), false)
    //      .collect()
    //      .toList
    //      .foreach(println)
  }

}
