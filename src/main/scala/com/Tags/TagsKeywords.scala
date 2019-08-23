package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagsKeywords extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]
    val stopWord = args(1).asInstanceOf[Array[String]]


    val keywords = row.getAs[String]("keywords")


    keywords.split("\\|")
      .filter(str => str.length <= 8 && str.length >= 3 && !stopWord.contains(str))
      .foreach(word => {
        list :+= ("K" + word, 1)
      })

    list
  }
}
