package com.Tags

import com.utils.{AmapUtil, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsBusiness extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取经纬度
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    //    && !long.equals("0") && !lat.equals("0")
    if (StringUtils.isNotBlank(long) && StringUtils.isNotBlank(lat)) {
      val business: String = AmapUtil.getBusinessFromAmap(long.toDouble, lat.toDouble)

      if (business.contains(",")) {
        val bussinessArr: Array[String] = business.split(",")

        for (b <- bussinessArr)
          list :+= (b, 1)
      } else {
        list :+= ("BUSINESS:" + business, 1)
      }

    }

    list
  }


}
