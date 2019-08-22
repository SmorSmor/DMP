package com.utils

/**
  * 指标方法
  */
object RptUtils {
  // 此方法处理请求数
  def request(requestmode: Int, processnode: Int)= {
    var res1 = 0
    var res2 = 0
    var res3 = 0

    if (requestmode == 1 && processnode >= 1) {
      res1 = 1
    }
    if (requestmode == 1 && processnode >= 2) {
      res2 = 1
    }
    if (requestmode == 1 && processnode == 3) {
      res3 = 1
    }

    (res1, res2, res3)

  }

  // 此方法处理展示点击数
  def click(requestmode: Int, iseffective: Int) = {
    var res1 = 0
    var res2 = 0

    if (requestmode == 2 && iseffective == 1) {
      res1 = 1
    }
    if (requestmode == 3 && iseffective == 1) {
      res2 = 1
    }

    (res1, res2)

  }

  // 此方法处理竞价操作
  def AD(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int,
         adorderid: Int, winprice: Double, adpayment: Double)  = {
    var res1 = 0
    var res2 = 0
    var res3 = 0.0
    var res4 = 0.0

    if (iseffective == 1 && isbilling == 1 && isbid == 1) {
      res1 = 1
    }
    if (iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0) {
      res2 = 1
    }
    if (iseffective == 1 && isbilling == 1 && iswin == 1) {
      res3 = winprice
    }
    if (iseffective == 1 && isbilling == 1 && iswin == 1) {
      res4 = adpayment
    }

    (res1, res2, res3, res4)

  }

}
