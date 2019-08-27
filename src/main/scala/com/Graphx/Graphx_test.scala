package com.Graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Graphx_test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)

    val vertexRdd = sc.makeRDD(Seq(
      (1L, ("詹姆斯", 35)),
      (2L, ("霍德华", 34)),
      (6L, ("杜兰特", 31)),
      (9L, ("库里", 30)),
      (133L, ("哈登", 30)),
      (138L, ("席尔瓦", 36)),
      (16L, ("法尔考", 35)),
      (44L, ("内马尔", 27)),
      (21L, ("J罗", 28)),
      (5L, ("高斯林", 60)),
      (7L, ("奥德斯基", 55)),
      (158L, ("马云", 55))
    ))

    val edge = sc.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)

    ))
    val graph = Graph(vertexRdd, edge)

    val vertices = graph.connectedComponents().vertices
    val value: RDD[(VertexId, (VertexId, (String, Int)))] = vertices.join(vertexRdd)
    value
      .map {
      case (userId, (conId, (name, age))) => {
        (conId, List(name, age))
      }
    }.reduceByKey(_ ++ _)
      .foreach(println)


  }

}
