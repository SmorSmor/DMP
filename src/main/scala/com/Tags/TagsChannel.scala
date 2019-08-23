package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagsChannel extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]

    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")

    list :+= ("CN" + adplatformproviderid, 1)

    list

  }
}
