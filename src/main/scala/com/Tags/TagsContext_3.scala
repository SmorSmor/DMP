package com.Tags

import com.utils.TagUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object TagsContext_3 {

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


    // 读取数据
    val df: DataFrame = spark.read.parquet(inputPath)

    // 处理标签：打标签
    val baseRDD = df.rdd
      .map(row => {
        // 取出用户id
        val userId = TagUtils.getAllUserId(row)

        (userId, row)
      })

    // 构建顶点集合
    val vertiesRDD = baseRDD.flatMap(tp => {
      val row = tp._2
      val adList: List[(String, Int)] = TagsAD.makeTags(row)
      val appList: List[(String, Int)] = TagsApp.makeTags(row, appInfo.value)
      val channelList: List[(String, Int)] = TagsChannel.makeTags(row)
      val deviceList: List[(String, Int)] = TagsDevice.makeTags(row)
      val keywordsList: List[(String, Int)] = TagsKeywords.makeTags(row, stopInfo.value)
      val proAndCityList: List[(String, Int)] = TagsProAndCity.makeTags(row)

      val allTags = adList ++ appList ++ channelList ++ deviceList ++ keywordsList ++ proAndCityList
      val VD = tp._1.map((_, 0)) ++ allTags

      // 只让一个uid携带标签，其余uid携带空链表
      tp._1.map(uId => {
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        }
        else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })
    //    vertiesRDD.foreach(println)

    // 构建边集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      tp._1.map(uid => {
        Edge(tp._1.head.hashCode.toLong, uid.hashCode, 0)
      })
    })

    //    edges.foreach(println)

    // 构建图
    val graph: Graph[List[(String, Int)], Int] = Graph(vertiesRDD, edges)

    // 取出顶点，使用连通图的算法
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    // 处理所有的标签和ID
    vertices.join(vertiesRDD).map {
      case (uid, (conId, tagsAll)) => {
        (conId, tagsAll)
      }
    }.reduceByKey((list1, list2) => {
      // 聚合所有的标签
      (list1 ++ list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
      .foreach(println)


    sc.stop()
    spark.stop()

  }
}
