package com.test00

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object test000 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高

    // 创建执行入口
    val sc = new SparkContext(conf)

    val hbaseConf: Configuration = HBaseConfiguration.create()

    hbaseConf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
//    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

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

    val tableDescriptor: HTableDescriptor = new HTableDescriptor(tableName)

    // 创建一个列簇
    val columnDescriptor = new HColumnDescriptor("tags")
    // 将列簇放入到表中
    tableDescriptor.addFamily(columnDescriptor)

    admin.createTable(tableDescriptor)
    admin.close()
    connection.close()

//    // 创建job对象
//    val jobConf: JobConf = new JobConf(hbaseConf)
//
//    // 指定输出类型
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//
//    //指定输出表
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)
    //    sc.parallelize(list)
    sc.stop()

  }

}
