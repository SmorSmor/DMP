package com.utils

import java.sql.{Connection, DriverManager}
import java.util.LinkedList

import com.typesafe.config.{Config, ConfigFactory}

object DBConnectionPool {
  private val config = ConfigFactory.load("jdbcPool.properties")
  private val max_connection = config.getString("jdbc.max_connection") //连接池总数
  private val connection_num = config.getString("jdbc.connection_num") //产生连接数
  private var current_num = 0 //当前连接池已产生的连接数
  private val pools = new LinkedList[Connection]() //连接池
  private val driver = config.getString("jdbc.driver")
  private val url = config.getString("jdbc.url")
  private val username = config.getString("jdbc.username")
  private val password = config.getString("jdbc.password")
  /**
    * 加载驱动
    */
  private def before() {
    if (current_num > max_connection.toInt && pools.isEmpty()) {
      print("busyness")
      Thread.sleep(2000)
      before()
    } else {
      Class.forName(driver)
    }
  }
  /**
    * 获得连接
    */
  private def initConn(): Connection = {
    DriverManager.getConnection(url,username,password)
  }
  /**
    * 初始化连接池
    */
  private def initConnectionPool(): LinkedList[Connection] = {
    AnyRef.synchronized({
      if (pools.isEmpty()) {
        before()
        for (i <- 1 to connection_num.toInt) {
          pools.push(initConn())
          current_num += 1
        }
      }
      pools
    })
  }
  /**
    * 获得连接
    */
  def getConn():Connection={
    initConnectionPool()
    pools.poll()
  }
  /**
    * 释放连接
    */
  def releaseCon(con:Connection){
    pools.push(con)
  }
}