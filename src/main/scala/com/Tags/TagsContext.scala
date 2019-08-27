package com.Tags

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.{RedisPoolUtils, TagUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object TagsContext {

  def main(args: Array[String]): Unit = {
    // 判断路径是否正确
    if (args.length != 5) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }

    // 创建一个集合保存输入和输出目录
    val Array(inputPath, outputPath, dirPath, stopPath, day) = args

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高

    // 创建执行入口
    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // 切分字典集，并生成一个map
    val appMap: Map[String, String] = sc.textFile(dirPath)
      .map(_.split("\t", -1))
      .map(arr => (arr(4), arr(1)))
      .collect()
      .toMap

    // 将appMap作为广播变量
    val appInfo: Broadcast[Map[String, String]] = sc.broadcast(appMap)

    // 获取停用词库
    val stopMap: Array[String] = sc.textFile(stopPath)
      .flatMap(_.split(" "))
      .collect()

    val stopInfo: Broadcast[Array[String]] = sc.broadcast(stopMap)

    import spark.implicits._

    // 读取数据
    val df: DataFrame = spark.read.parquet(inputPath)

    // 使用redis存储的字典集来实现指标
    //    df.foreachPartition(df=>{
    //      val jedis: Jedis = RedisPoolUtils.getRedis()
    //
    //      df.map(row=>{
    //        val keywordsList2: List[(String, Int)] = TagsApp_redis.makeTags(row, jedis)
    //        keywordsList2
    //      }).foreach(println)
    //
    //      jedis.close()
    //
    //    })

    //  调用hbase API
    val load: Config = ConfigFactory.load()
    val hbaseConf: Configuration = HBaseConfiguration.create()

    hbaseConf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")

    val connection: Connection = ConnectionFactory.createConnection(hbaseConf)

    val admin: Admin = connection.getAdmin

    // 创建namespace
    //    val namespace: NamespaceDescriptor = NamespaceDescriptor.create("dmp").build()
    //    admin.createNamespace(namespace)
    val hbaseTableName: String = "dmp:tags"

    val tableName: TableName = TableName.valueOf(hbaseTableName)

    if (admin.tableExists(tableName)) {
      // 删除表之前先禁用
      admin.disableTable(tableName)

      // 删除表
      admin.deleteTable(tableName)
    }
    // 创建hbase表的描述类
    val tableDescriptor: HTableDescriptor = new HTableDescriptor(tableName)

    // 创建一个列簇
    val columnDescriptor = new HColumnDescriptor("tags")
    // 将列簇放入到表中
    tableDescriptor.addFamily(columnDescriptor)
    // 创建表
    admin.createTable(tableDescriptor)
    admin.close()
    connection.close()
    // 创建job对象
    val jobConf: JobConf = new JobConf(hbaseConf)
    // 指定输出类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定输出表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

    // 处理标签：打标签
    val tagsRDD: RDD[(String, List[(String, Int)])] = df.filter(TagUtils.OneUserId)
      // 所有标签都在内部实现
      .map(row => {
      // 取出用户id
      val userId = TagUtils.getOneUserId(row)
      val adList: List[(String, Int)] = TagsAD.makeTags(row)
      val appList: List[(String, Int)] = TagsApp.makeTags(row, appInfo.value)
      val channelList: List[(String, Int)] = TagsChannel.makeTags(row)
      val deviceList: List[(String, Int)] = TagsDevice.makeTags(row)
      val keywordsList: List[(String, Int)] = TagsKeywords.makeTags(row, stopInfo.value)
      val proAndCityList: List[(String, Int)] = TagsProAndCity.makeTags(row)

      //      val businessList: List[(String, Int)] = TagsBusiness.makeTags(row)

      //通过row数据打上所有标签
      (userId, adList ++ appList ++ channelList ++ deviceList ++ keywordsList ++ proAndCityList) // ++ businessList )
      //      (userId, adList, appList, channelList, deviceList, keywordsList, proAndCityList)

    }).rdd

    tagsRDD.reduceByKey((list1, list2) => {
      (list1 ::: list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_ + _._2))
        .toList
    })
      .collect()
      .foreach(println)

    tagsRDD.map {
      case (userId, userTags) => {
        val put = new Put(Bytes.toBytes(userId))
        val tags = userTags.map(t => t._1 + ":" + t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(s"${day}"), Bytes.toBytes(tags))

        (new ImmutableBytesWritable(), put)
      }

    }.saveAsHadoopDataset(jobConf)

    sc.stop()
    spark.stop()

  }
}
