package com.utils

object Utils2Type {
  //String 转 Double
  def toDouble(str: String): Double = {
    try {
      str.toDouble
    } catch {
      case _: Exception => 0.0
    }
  }

  // String 转 Int
  def toInt(str: String): Int = {
    try (
      str.toInt
      ) catch {
      case _: Exception => 0
    }
  }


}
