package com.Rpt

import java.sql.{Connection, Statement}

import com.utils.DBConnectionPool
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AppRpt {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc: SparkContext = new SparkContext(conf)

    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // 加载数据库连接的信息
    //    val load: Config = ConfigFactory.load()
    // 获取数据库链接信息
    //    val prop = new Properties()
    //    prop.put("user", load.getString("jdbc.user"))
    //    prop.put("password", load.getString("jdbc.password"))
    //    val url = load.getString("jdbc.url")

    // 切分字典集，并生成一个map
    val appMap: Map[String, String] = sc.textFile("C:\\Users\\孙洪斌\\Desktop\\Spark用户画像分析\\app_dict.txt")
      .map(_.split("\t", -1))
      .map(arr => (arr(4), arr(1)))
      .collect().toMap

    // 将appMap作为广播变量
    val appInfo: Broadcast[Map[String, String]] = sc.broadcast(appMap)

    // 读取需要统计的文件内容
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
      val appid: String = row.getAs[String]("appid")
      val appname: String = row.getAs[String]("appname")

      // 创建三个方法处理三个指标
      val request = com.utils.RptUtils2.request(requestmode, processnode)
      val click = com.utils.RptUtils2.click(requestmode, iseffective)
      val ad = com.utils.RptUtils2.AD(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      // 返回值
      (
        appid, appname, request ++ click ++ ad
      )
    })
      // 转为rdd
      .rdd
      // 使用AppId从字典集获取APPName，如果没有则使用原始APPName
      .map(res => {
      (appInfo.value.getOrElse(res._1, res._2), res._3)
    })
      // 使用拉链将value求和
      .reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })
      // 转为一个大元组方便使用
      .map(res => {
      (res._1, res._2(0), res._2(1), res._2(2), res._2(3), res._2(4), res._2(5), res._2(6), res._2(7), res._2(8))
    })
      // 使用数据库连接池写入到MySQL
      // 使用此方式需要先在数据库中建立对应的表
      .foreachPartition(pr => {
      // 获取数据库连接池中的连接
      val connection: Connection = DBConnectionPool.getConn()
      val statement: Statement = connection.createStatement()
      // 对每一个rdd进行插入数据库的操作
      pr.foreach(x => {
        statement.execute(s"insert into AppRpt values('${x._1.toString}','${x._2.toInt}','${x._3.toInt}','${x._4.toInt}','${x._5.toInt}','${x._6.toInt}','${x._7.toInt}','${x._8.toString}','${x._9.toDouble}','${x._10.toDouble}') ")
      })
      // 释放连接，将链接还给连接池
      DBConnectionPool.releaseCon(connection)
    })

    // 通过DF反射的方式，写入到数据库
    //      .toDF("媒体类型", "总请求", "有效请求", "广告请求", "参与竞价数", "竞价成功数", "展示数", "点击数", "广告成本", "广告消费")
    //      .write.mode("append").jdbc(url, "AppRpt", prop)

  }

}
