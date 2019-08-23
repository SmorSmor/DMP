package com.test00

import com.utils.RedisPoolUtils
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * 将字典集KEY、VALUE写入redis
  */
object save2redis {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc: SparkContext = new SparkContext(conf)

    // 切分字典集，并生成一个map
    sc.textFile("C:\\Users\\孙洪斌\\Desktop\\Spark用户画像分析\\app_dict.txt")
      .foreachPartition(rdd => {

        val jedis: Jedis = RedisPoolUtils.getRedis()
        val unit: Unit = rdd.foreach(str => {
          val arr: Array[String] = str.split("\t", -1)
          jedis.set(arr(4), arr(1))
        })
        jedis.close()

      })
    sc.stop()

  }

}
