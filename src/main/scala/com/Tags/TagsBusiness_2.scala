package com.Tags

import ch.hsr.geohash.GeoHash
import com.utils.{AmapUtil, RedisPoolUtils, Tag, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsBusiness_2 extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    // 获取经纬度，过滤经纬度
    if (Utils2Type.toDouble(long) >= 73.0 &&
      Utils2Type.toDouble(long) <= 135.0 &&
      Utils2Type.toDouble(lat) >= 3.0 &&
      Utils2Type.toDouble(lat) <= 54.0) {
      // 先去数据库获取商圈
      val business = getBusiness(long.toDouble, lat.toDouble)
      // 判断缓存中是否有此商圈
      if (StringUtils.isNotBlank(business)) {
        val lines = business.split(",")
        lines.foreach(f => list :+= (f, 1))
      }
      //      list:+=(business,1)
    }
    list
  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long: Double, lat: Double): String = {
    // 转换GeoHash字符串
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 8)
    // 去数据库查询
    var business = redis_queryBusiness(geohash)
    // 判断商圈是否为空
    if (business == null || business.length == 0) {
      // 通过经纬度获取商圈
      business = AmapUtil.getBusinessFromAmap(long.toDouble, lat.toDouble)
      // 如果调用高德地图解析商圈，那么需要将此次商圈存入redis
      redis_insertBusiness(geohash, business)
    }
    business
  }

  /**
    * 获取商圈信息
    */
  def redis_queryBusiness(geohash: String): String = {
    val jedis = RedisPoolUtils.getRedis()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  /**
    * 存储商圈到redis
    */
  def redis_insertBusiness(geoHash: String, business: String): Unit = {
    val jedis = RedisPoolUtils.getRedis()
    jedis.set(geoHash, business)
    jedis.close()
  }
}