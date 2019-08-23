package com.Tags

import com.utils.{RedisPoolUtils, TagUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object TagsContext {

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

    import spark.implicits._

    // 读取数据
    val df: DataFrame = spark.read.parquet(inputPath)

    // 使用redis存储的字典集来实现指标
    //    df.foreachPartition(df=>{
    //      val jedis: Jedis = RedisPoolUtils.getRedis()
    //
    //      df.map(row=>{
    //        val keywordsList2: List[(String, Int)] = TagsApp_redis.makeTags(row, jedis)
    //        keywordsList2
    //      }).foreach(println)
    //
    //      jedis.close()
    //
    //    })

    df.filter(TagUtils.OneUserId)
      // 所有标签都在内部实现
      .map(row => {
      // 取出用户id
      val userId = TagUtils.getOneUserId(row)

      val adList: List[(String, Int)] = TagsAD.makeTags(row)

      val appList: List[(String, Int)] = TagsApp.makeTags(row, appInfo.value)

      val channelList: List[(String, Int)] = TagsChannel.makeTags(row)

      val deviceList: List[(String, Int)] = TagsDevice.makeTags(row)

      val keywordsList: List[(String, Int)] = TagsKeywords.makeTags(row, stopInfo.value)

      val proAndCityList: List[(String, Int)] = TagsProAndCity.makeTags(row)


      //通过row数据打上所有标签
      //      (userId, adList :+ appList :+ channelList :+ deviceList :+ keywordsList :+ proAndCityList)
      //      (userId, adList :: appList :: channelList :: deviceList :: keywordsList :: proAndCityList)
      (userId, adList, appList, channelList, deviceList, keywordsList, proAndCityList)

    }).rdd.collect().foreach(println)


    sc.stop()
    spark.stop()


  }
}
