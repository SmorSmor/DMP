package com.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试类
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    val sc = new SparkContext(conf)
    val list = List("112.717828,36.726411")
    val rdd = sc.makeRDD(list)
    val bs = rdd.map(t => {
      val arr = t.split(",")
      AmapUtil.getBusinessFromAmap(arr(0).toDouble, arr(1).toDouble)
    })
    bs.foreach(println)


  }
}
