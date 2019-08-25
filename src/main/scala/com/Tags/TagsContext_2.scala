package com.Tags

import com.utils.{RedisPoolUtils, TagUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object TagsContext_2 {

  def main(args: Array[String]): Unit = {
    // 判断路径是否正确
    if (args.length != 4) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }

    // 创建一个集合保存输入和输出目录
    val Array(inputPath, outputPath, dirPath, stopPath) = args

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高

    // 创建执行入口
    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // 切分字典集，并生成一个map
    val appMap: Map[String, String] = sc.textFile(dirPath)
      .map(_.split("\t", -1))
      .map(arr => (arr(4), arr(1)))
      .collect()
      .toMap

    // 将appMap作为广播变量
    val appInfo: Broadcast[Map[String, String]] = sc.broadcast(appMap)

    // 获取停用词库
    val stopMap: Array[String] = sc.textFile(stopPath)
      .flatMap(_.split(" "))
      .collect()

    val stopInfo: Broadcast[Array[String]] = sc.broadcast(stopMap)


    // 读取数据D:\out-0820-01-less 0 C:\Users\孙洪斌\Desktop\Spark用户画像分析\app_dict.txt C:\Users\孙洪斌\Desktop\Spark用户画像分析\stopwords.txt
    val df: DataFrame = spark.read.parquet(inputPath)

    // 使用redis存储的字典集来实现指标
    df.filter(TagUtils.OneUserId)
      .rdd
      .mapPartitions(rdd => {
        val jedis: Jedis = RedisPoolUtils.getRedis()

        try {
          rdd.map(row => {

            val userId = TagUtils.getOneUserId(row)
            (
              userId,
              TagsAD.makeTags(row) ++
                TagsApp_redis.makeTags(row, jedis) ++
                TagsChannel.makeTags(row) ++
                TagsDevice.makeTags(row) ++
                TagsKeywords.makeTags(row, stopInfo.value) ++
                TagsProAndCity.makeTags(row)
            )

          })
        } finally {
          jedis.close()
        }
      }).reduceByKey((list1, list2) => {
      list1 ::: list2
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_ + _._2))
        .toList
    }).collect()
      .foreach(println)


    sc.stop()
    spark.stop()

  }
}
