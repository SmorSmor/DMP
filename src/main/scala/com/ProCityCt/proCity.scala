package com.ProCityCt

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object proCity {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc: SparkContext = new SparkContext(conf)

    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val df: DataFrame = spark.read.parquet("D:\\out-0820-01")

    // 转为rdd操作
    val res: RDD[((String, String), Int)] = df
      .select("provincename", "cityname")
      .rdd
      .map(x => (x.getString(0), x.getString(1)))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    res.collect().toList.foreach(println)

    val schema: StructType = StructType(
      List(
        StructField("provincename", StringType),
        StructField("cityname", StringType),
        StructField("count", IntegerType)
      )
    )

    // 生成df
    val resDF: DataFrame = spark.createDataFrame(res.map(x => Row(x._1._1, x._1._2, x._2)), schema)

    // spark sql操作
    df
      .select("provincename", "cityname")
      .createOrReplaceTempView("procity")

    spark
      .sql("select *,count(cityname) ct from procity group by provincename,cityname")
      .show()

    val load: Config = ConfigFactory.load()
    // 获取数据库链接信息
    val prop = new Properties()
    prop.put("user", load.getString("jdbc.user"))
    prop.put("password", load.getString("jdbc.password"))
    val url = load.getString("jdbc.url")

    // 存储到数据库

    //    resDF.write.mode("append").jdbc(url, load.getString("jdbc.tablename"), prop)

    // 分区存储到本地
        resDF.coalesce(1).write.partitionBy("provincename").format("json").save("D:\\out-0820-02")
    //    resDF.write.partitionBy("provincename").format("json").save("hdfs://hadoop02:9000/out-0820")


    sc.stop()
    spark.stop()
  }

}
