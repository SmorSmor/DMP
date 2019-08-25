package com.exam01

import com.alibaba.fastjson.JSONObject

/** *
  * 1、按照pois，分类businessarea，并统计每个businessarea的总数。
  * 2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
  */
object JsonUtils {
  /**
    * 获取 pois ID
    *
    * @param json
    * @return
    */
  def getPois(json: JSONObject): List[String] = {
    //判断状态是否成功
    val status = json.getIntValue("status")
    if (status == 0) return null

    //接下来解析内部json串,判断每个key的value都不能为空
    val regeocodesJson = json.getJSONObject("regeocode")
    if (regeocodesJson == null || regeocodesJson.keySet().isEmpty) return null

    val poisArray = regeocodesJson.getJSONArray("pois")
    if (poisArray == null || poisArray.isEmpty) return null

    //创建集合保存数据
    var list = List[String]()

    //循环输出
    for (item <- poisArray.toArray) {
      if (item.isInstanceOf[JSONObject]) {
        val json = item.asInstanceOf[JSONObject]
        list :+= json.getString("id")
      }
    }

    list

  }

  /**
    * 获取 businessarea
    *
    * @param json
    * @return
    */
  def getBusiness(json: JSONObject): List[(String, Int)] = {
    //判断状态是否成功
    val status = json.getIntValue("status")
    if (status == 0) return null

    //接下来解析内部json串,判断每个key的value都不能为空
    val regeocodesJson = json.getJSONObject("regeocode")
    if (regeocodesJson == null || regeocodesJson.keySet().isEmpty) return null

    val addressComponentJson = regeocodesJson.getJSONObject("addressComponent")
    val busnissAreasArray = addressComponentJson.getJSONArray("businessAreas")
    if (busnissAreasArray == null || busnissAreasArray.isEmpty) return null

    //创建集合保存数据
    var list = List[(String, Int)]()

    //循环输出
    for (item <- busnissAreasArray.toArray) {
      if (item.isInstanceOf[JSONObject]) {
        val json = item.asInstanceOf[JSONObject]
        val t: Array[String] = json.getString("name").split(";")
        for (tt <- t) {
          list :+= (tt + "", 1)
        }
      }
    }

    list

  }

  /**
    * 获取 type
    *
    * @param json
    * @return
    */
  def getType(json: JSONObject): List[(String, Int)] = {
    //判断状态是否成功
    val status = json.getIntValue("status")
    if (status == 0) return null

    //接下来解析内部json串,判断每个key的value都不能为空
    val regeocodesJson = json.getJSONObject("regeocode")
    if (regeocodesJson == null || regeocodesJson.keySet().isEmpty) return null

    val poisArray = regeocodesJson.getJSONArray("pois")
    if (poisArray == null || poisArray.isEmpty) return null

    //创建集合保存数据
    var list = List[(String, Int)]()

    //循环输出
    for (item <- poisArray.toArray) {
      if (item.isInstanceOf[JSONObject]) {
        val json = item.asInstanceOf[JSONObject]
        val t: Array[String] = json.getString("type").split(";")
        for (tt <- t) {
          list :+= (tt + "", 1)
        }
      }
    }

    list

  }

}
