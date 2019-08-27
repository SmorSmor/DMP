//package com.utils
//
//import org.apache.spark.{Accumulator, SparkContext}
//import org.apache.spark.util.LongAccumulator
//
//object UuidUtils {
//  def getuuid(sc: SparkContext) = {
//
//    var uuid = 1L
//        // 首先需要进行注册，注册并初始化一个累加器
//        def longAcc(name: String): LongAccumulator = {
//          val acc = new LongAccumulator
//          sc.register(acc, name)
//          acc
//        }
//
//        // 累加器的返回值
//        val acc1: LongAccumulator = longAcc("LongAccumulator")
//        acc1.add(uuid)
//
//
//
//
//  }
//
//}
