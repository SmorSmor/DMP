package com.test

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test01 {
  def main(args: Array[String]): Unit= {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val jsonstr =sc.textFile("C:\\Users\\孙洪斌\\Documents\\Tencent Files\\1223882390\\FileRecv\\json.txt")
    val jsonparse: RDD[String] = jsonstr.map(t => {
      // 创建集合 保存数据
      val buffer = collection.mutable.ListBuffer[String]()

      val jsonparse = JSON.parseObject(t)
      // 判断状态是否成功
      val status = jsonparse.getIntValue("status")
      if (status == 1) {
        // 接下来解析内部json串，判断每个key的value都不能为空
        val regeocodeJson = jsonparse.getJSONObject("regeocode")
        if (regeocodeJson != null && !regeocodeJson.keySet().isEmpty) {
          //获取pois数组
          val poisArray = regeocodeJson.getJSONArray("pois")
          if (poisArray != null && !poisArray.isEmpty) {
            // 循环输出
            for (item <- poisArray.toArray) {
              if (item.isInstanceOf[JSONObject]) {
                val json = item.asInstanceOf[JSONObject]
                buffer.append(json.getString("businessarea"))
              }
            }
          }
        }
      }
      buffer.mkString(",")
    })


    jsonparse.flatMap(line=>{
      line.split(",").map((_,1))
    }).reduceByKey(_+_).foreach(println)



  }
}
