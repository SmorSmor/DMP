package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object TagsApp_redis extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]

    val appid = row.getAs[String]("appid")
    val appname = row.getAs[String]("appname")
    if (StringUtils.isNotBlank(appname)){
      list :+= ("APP" + appname, 1)
    }
    else{
      list :+= ("APP" + jedis.get(appid), 1)
    }


      list
  }
}
