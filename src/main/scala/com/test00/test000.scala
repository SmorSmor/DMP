package com.test00

import org.apache.spark.{SparkConf, SparkContext}

object test000 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高

    // 创建执行入口
    val sc = new SparkContext(conf)

    //    sc.parallelize(list)
    sc.stop()

  }

}
