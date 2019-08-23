package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisPoolUtils {

  private val conf: JedisPoolConfig = new JedisPoolConfig
  conf.setMaxTotal(30)
  conf.setMaxIdle(10)
  private  val pool: JedisPool = new JedisPool(conf,"hadoop01", 6379)
  def getRedis() : Jedis={
    pool.getResource
  }
}
