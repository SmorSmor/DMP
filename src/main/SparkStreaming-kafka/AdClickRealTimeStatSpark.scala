import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

/**
  * 广告点击流量实时统计
  * 1、使用updateStateByKey操作，实时的计算每天各省各城市各广告的点击量，并更新到数据库
  * 2、使用transform结合SparkSQL统计每天各省份top3热门广告（开窗函数）
  * 3、使用窗口操作，对最近1小时的窗口内的数据，计算出各广告每分钟的点击量，实时更新到数据库
  */
object AdClickRealTimeStatSpark extends Serializable {
  def main(args: Array[String]): Unit = { // 模板代码
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("d://out-kafka")

    // 指定请求kafka的配置信息
    val kafkaParam = Map[String, Object](
      "bootstrap.servers" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092",
      // 指定key的反序列化方式
      "key.deserializer" -> classOf[StringDeserializer],
      // 指定value的反序列化方式
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // 指定消费位置
      "auto.offset.reset" -> "latest",
      // 如果value合法，自动提交offset
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    // 指定topic
    val topics = Array("AdRealTimeLog")

    // 消费数据
    val massage: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topics, kafkaParam)
      )
    // 实时的计算每天各省各城市各广告的点击量，并更新到数据库


    // 业务一：计算每天各省各城市各广告的点击流量实时统计
    // 生成的数据格式为：<yyyyMMdd_province_city_adId, clickCount>
    val adRealTimeStatDStream = calcuateRealTimeStat(massage)

    // 业务二：统计每天各省top3热门广告
    calculateProvinceTop3Ad(adRealTimeStatDStream)

    // 业务三：统计各广告最近1小时内的点击量趋势
    //    calculateClickCountByWindow(massage)


    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 1、使用updateStateByKey操作，实时的计算每天各省各城市各广告的点击量，并更新到数据库
    *
    * @param msg
    */
  def calcuateRealTimeStat(msg: DStream[ConsumerRecord[String, String]]) = {
    // 对原始数据进行map，把结果映射为：<date_province_city_adId, 1L>
    val mappedDStream = msg.map(x => {
      // 把原始数据进行切分并获取各字段
      val logSplited = x.value().split(" ")
      val timestamp = logSplited(0)
      val date = new Date(timestamp.toLong)
      val dateKey = DateUtils.formatDateKey(date)

      // yyyyMMdd
      val province = logSplited(1)
      val city = logSplited(2)
      val adId = logSplited(4)
      val key = dateKey + "_" + province + "_" + city + "_" + adId

      (key, 1L)
    })

    // 聚合，按批次累加
    // 在这个DStream中，相当于每天各省各城市各广告的点击次数
    val upstateFun = (values: Seq[Long], state: Option[Long]) => {
      // state存储的是历史批次结果
      // 首先根据state来判断，之前的这个key值是否有对应的值
      var clickCount = 0L

      // 如果之前存在值，就以之前的状态作为起点，进行值的累加
      if (state.nonEmpty) clickCount = state.getOrElse(0L)

      // values代表当前batch RDD中每个key对应的所有的值
      // 比如当前batch RDD中点击量为4，values=(1,1,1,1)
      for (value <- values) {
        clickCount += value
      }

      Option(clickCount)
    }

    val aggrDStream = mappedDStream.updateStateByKey(upstateFun)

    // 将结果输出
    aggrDStream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        while (it.hasNext) {
          val tuple = it.next
          val keySplited = tuple._1.split("_")

          val date = keySplited(0)
          val province = keySplited(1)
          val city = keySplited(2)
          val adId = keySplited(3).toLong
          val clickCount = tuple._2.toLong

          //          println(s"日期：$date, 省份：$province, 城市：$city, 广告id：$adId, 点击量：$clickCount")
        }
      })
    })
    aggrDStream
  }

  /**
    * 2、使用transform结合SparkSQL统计每天各省份top3热门广告（开窗函数）
    *
    * @param adRealTimeStatDStream
    */
  def calculateProvinceTop3Ad(adRealTimeStatDStream: DStream[(String, Long)]) = {
    // 把adRealTimeStatDStream数据封装到Row
    // 封装后的数据是没有city字段的
    val rowDStream = adRealTimeStatDStream.transform(rdd => {
      // 把rdd数据格式整合为：<yyyyMMdd_province_adId, clickCount>
      val mappedRDD = rdd.map(tup => {
        val keySplited = tup._1.split("_")
        val date = keySplited(0)
        val province = keySplited(1)
        val adId = keySplited(3).toLong

        val clickCount = tup._2

        val key = date + "_" + province + "_" + adId

        (key, clickCount)
      })

      // 将mappedRDD的clickCount以省份进行聚合, 得到省份对应的点击广告数
      val dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(_ + _)

      // 将dailyAdClickCountByProvinceRDD转换为DataFrame
      // 注册为一张临时表
      // 通过SQL的方式（开窗函数）获取某天各省的top3热门广告
      val rowsRDD = dailyAdClickCountByProvinceRDD.map(tup => {
        val keySplited = tup._1.split("_")
        val date = keySplited(0)
        val province = keySplited(1)
        val adId = keySplited(2).toLong
        val clickCount = tup._2

        Row(date, province, adId, clickCount)
      })

      // 指定schema
      val schema = StructType(Array(
        StructField("date", DataTypes.StringType, true),
        StructField("province", DataTypes.StringType, true),
        StructField("ad_id", DataTypes.LongType, true),
        StructField("click_count", DataTypes.LongType, true)
      ))

      val spark = SparkSession.builder().getOrCreate()

      // 映射生成DataFrame
      val dailyAdClickCountByProvinceDF = spark.createDataFrame(rowsRDD, schema)

      // 生成临时表
      dailyAdClickCountByProvinceDF.createOrReplaceTempView("tmp_daily_ad_click_count_prov");

      // 使用HiveContext配合开窗函数，给province打一个行标，统计出各省的top3热门商品
      val sql =
        "select " +
          "date, " +
          "province, " +
          "ad_id, " +
          "click_count " +
          "from (" +
          "select date,province,ad_id,click_count," +
          "row_number() over (partition by province order by click_count desc) rank " +
          "from tmp_daily_ad_click_count_prov" +
          ") t " +
          "where rank <= 3"

      val provinceTop3DF = spark.sql(sql)

      provinceTop3DF.rdd
    })

    // 将结果输出
    rowDStream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        while (it.hasNext) {
          val row = it.next();
          val date = row.getString(0)
          val province = row.getString(1)
          val adId = row.getLong(2)
          val clickCount = row.getLong(3)

          println(s"日期：$date, 省份：$province, 广告id：$adId, 点击量：$clickCount")
        }
      })
    })
  }

  /**
    * 3、使用窗口操作，对最近1小时的窗口内的数据，计算出各广告每分钟的点击量，实时更新到数据库
    *
    * @param msg
    */
  def calculateClickCountByWindow(msg: DStream[ConsumerRecord[String, String]]): Unit = {
    // 首先把数据整合成：<yyyyMMddHHmm_adId, 1L>
    val tupDStream = msg.map(tup => {
      // 获取原始数据并切分
      val logSplited = tup.value().split(" ")

      // 把时间戳调整为yyyyMMddHHmm
      val timeMinute = DateUtils.formatTimeMinute(new Date(logSplited(0).toLong))

      val adId = logSplited(4).toLong

      (timeMinute + "_" + adId, 1L)
    })

    // 每一次出来的新的batch，都要获取最近一小时的所有的batch
    // 然后根据key进行reduceByKey，统计出一小时内的各分钟各广告的点击量
    val aggDtream = tupDStream.reduceByKeyAndWindow(
      (x: Long, y: Long) => x + y, Durations.minutes(60), Durations.seconds(10))

    // 结果数据的输出
    aggDtream.print()

  }

  /**
    * 通过时间戳来获取yyyyMMdd格式的日期
    *
    * @param timestamp
    * @return
    */
  def formatDate(timestamp: Long) = new SimpleDateFormat("yyyyMMdd").format(new Date(timestamp))

}