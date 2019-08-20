package com.test00

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 要求一：
  * 将统计的结果输出成 json 格式，并输出到磁盘目录。
  * {"ct":943,"provincename":"内蒙古自治区","cityname":"阿拉善盟"}
  * {"ct":578,"provincename":"内蒙古自治区","cityname":"未知"}
  * {"ct":262632,"provincename":"北京市","cityname":"北京市"}
  * {"ct":1583,"provincename":"台湾省","cityname":"未知"}
  * {"ct":53786,"provincename":"吉林省","cityname":"长春市"}
  * {"ct":41311,"provincename":"吉林省","cityname":"吉林市"}
  * {"ct":15158,"provincename":"吉林省","cityname":"延边朝鲜族自治州"}
  *
  * 要求二：
  * 将结果写到到 mysql 数据库。
  *
  * 要求三：
  * 用 spark 算子的方式实现上述的统计，存储到磁盘。
  */
object Test01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

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


    val resDF: DataFrame = spark.createDataFrame(res.map(x => Row(x._1._1, x._1._2, x._2)), schema)

    // 获取数据库链接信息
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    val url = "jdbc:mysql://localhost:3306/mydb1"
    // 存储到数据库
    //    resDF.write.jdbc(url, "count0820", prop)

    // 分区存储到本地
//    resDF.write.partitionBy("provincename").format("json").save("D:\\out-0820-02")
    resDF.write.partitionBy("provincename").format("json").save("hdfs://hadoop02:9000/out-0820")


    sc.stop()
    spark.stop()
  }

}
