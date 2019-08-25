package com.exam01

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/** *
  * 1、按照pois，分类businessarea，并统计每个businessarea的总数。
  * 2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
  */
object Test01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高

    // 创建执行入口
    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val line: RDD[String] = sc.textFile("C:\\Users\\孙洪斌\\Documents\\Tencent Files\\1223882390\\FileRecv\\json.txt")


    line.map(json => {
      val jsonParse = JSON.parseObject(json)
      val pios = JsonUtils.getPois(jsonParse)

      val business = JsonUtils.getBusiness(jsonParse)

      (pios, business)

    })
      .collect().toList.foreach(println)
    println()
    println("------------------------------------------------------------------------------------------------------------------------------------------------------------")
    println()
    line.map(json => {
      val jsonParse = JSON.parseObject(json)
      val pios = JsonUtils.getPois(jsonParse)

      val types = JsonUtils.getType(jsonParse)

      (pios, types)

    })
      .collect().toList.foreach(println)

  }


}
