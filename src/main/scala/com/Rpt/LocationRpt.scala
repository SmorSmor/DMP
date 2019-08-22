package com.Rpt

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.utils.RptUtils

object LocationRpt {
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

    df.createOrReplaceTempView("logs")

    val load: Config = ConfigFactory.load()
    // 获取数据库链接信息
    val prop = new Properties()
    prop.put("user", load.getString("jdbc.user"))
    prop.put("password", load.getString("jdbc.password"))
    val url = load.getString("jdbc.url")

    spark.sql("select tt.provincename,tt.cityname,sum(request1),sum(request2),sum(request3),sum(ad1),sum(ad2),sum(click1),sum(click2),sum(ad3),sum(ad4) " +
      "from " +
      "(select " +
      "provincename," +
      "cityname," +
      "(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) request1," +
      "(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) request2," +
      "(case when requestmode = 1 and processnode = 3 then 1 else 0 end) request3," +
      "(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) ad1," +
      "(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid <> 0 then 1 else 0 end) ad2," +
      "(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) click1," +
      "(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) click2," +
      "(case when requestmode = 1 and isbilling >= 1 and iswin = 1 then winprice else 0 end) ad3," +
      "(case when requestmode = 1 and isbilling >= 1 and iswin =1 then adpayment else 0 end) ad4 " +
      "from logs ) tt " +
      "group by tt.provincename ,tt.cityname "

    ).write.mode("append").jdbc(url, "LocationRPT", prop)


//    df.select(
//      "requestmode",
//      "processnode",
//      "iseffective",
//      "isbilling",
//      "isbid",
//      "iswin",
//      "adorderid",
//      "winprice",
//      "adpayment",
//      "provincename",
//      "cityname"
//    ).show()

    import spark.implicits._

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
      val provincename: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")

      // 创建三个方法处理三个指标
      val request = com.utils.RptUtils.request(requestmode, processnode)
      val click = com.utils.RptUtils.click(requestmode, iseffective)
      val ad = com.utils.RptUtils.AD(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      // 返回值4个元组
      (
        (provincename, cityname),
        request, click, ad
      )
    }).rdd //转为rdd
      .map(tups =>
      ( // 转为KV对
        (tups._1._1, tups._1._2),
        (tups._2._1, tups._2._2, tups._2._3, tups._4._1, tups._4._2, tups._3._1, tups._3._2, tups._4._3, tups._4._4)
      )
    ).reduceByKey((a, b) => { // VALUE累加
      (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9)
    }).sortBy(_._2._1, false)
      .collect()
      .toList
      .foreach(println)



    sc.stop()
    spark.stop()
  }

}
