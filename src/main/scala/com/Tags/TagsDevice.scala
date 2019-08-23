package com.Tags

  import com.utils.Tag
  import org.apache.spark.sql.Row

  object TagsDevice extends Tag {
    /**
      * 打标签的统一接口
      */
    override def makeTags(args: Any*): List[(String, Int)] = {
      var list = List[(String, Int)]()

      // 解析参数
      val row = args(0).asInstanceOf[Row]

      val client: Int = row.getAs[Int]("client")
      val networkmannerid: Int = row.getAs[Int]("networkmannerid")
      val ispid: Int = row.getAs[Int]("ispid")

      list :+= ("D0001000" + client, 1)
      list :+= ("D0002000" + networkmannerid, 1)
      list :+= ("D0003000" + ispid, 1)


      list

    }
}
