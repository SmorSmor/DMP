package com.Rpt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ClientRpt {
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
      val client: Int = row.getAs[Int]("client")

      // 创建三个方法处理三个指标
      val request = com.utils.RptUtils2.request(requestmode, processnode)
      val click = com.utils.RptUtils2.click(requestmode, iseffective)
      val ad = com.utils.RptUtils2.AD(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      // 返回值4个元组
      (
        client,
        request++click++ad
      )
    }).rdd //转为rdd
      .reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).sortBy(_._2(0),false)
      .collect()
      .toList
      .foreach(println)
  }

}
