package com.utils

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 商圈解析
  */
object AmapUtil {
  // 高德地图
  def getBusinessFromAmap(long: Double, lat: Double): String = {
    // https://lbs.amap.com/api/webservice/guide/api/georegeo/
    // https://restapi.amap.com/v3/geocode/regeo?output=xml&location=116.310003,39.991957&key=<用户的key>&radius=1000&extensions=all

    val key = "609a143c51318b6ca80058d2c47c61ce"
    val location = long + "," + lat
    val urlStr = "https://restapi.amap.com/v3/geocode/regeo?&location=" + location + "&key=" + key + "&radius=1000&extensions=all"

    val buffer = collection.mutable.ListBuffer[String]()
    // 调用请求
    val jsonStr = MyHttpUtils.get(urlStr)
    //解析json字符串
    val jsonparse = JSON.parseObject(jsonStr)
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

  }

}
