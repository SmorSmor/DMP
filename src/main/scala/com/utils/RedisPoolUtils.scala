package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisPoolUtils {
  // 初始化JedisPool环境
  private val conf: JedisPoolConfig = new JedisPoolConfig

  // 设置JedisPoolConfig环境
  conf.setMaxTotal(30)
  conf.setMaxIdle(10)

  // 初始化一个JedisPool
  private val pool: JedisPool = new JedisPool(conf, "hadoop01", 6379)

  // 获取一个Jedis连接
  def getRedis(): Jedis = {
    pool.getResource
  }
}
