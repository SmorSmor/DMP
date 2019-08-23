package com.Tags

import com.utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row


/**
  * APP标签
  */
object TagsApp extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    val bor = args(1).asInstanceOf[Map[String, String]]
    // 获取APPName
    val appid = row.getAs[String]("appid")
    val appname = row.getAs[String]("appname")

    list :+= ("APP" + bor.getOrElse(appid, appname), 1)

    list
  }


}
