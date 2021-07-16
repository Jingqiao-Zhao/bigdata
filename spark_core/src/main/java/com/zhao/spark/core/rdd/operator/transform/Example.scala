package com.zhao.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Example {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc: SparkContext = new SparkContext(conf)

    // 获取数据：时间戳，省份，城市，用户，广告ID
    val dataRDD: RDD[String] = sc.textFile("datas/agent.log")
    // 数据转化：（（省份，广告），1）
    val mapRDD: RDD[((String, String), Int)] = dataRDD.map(
      line =>{
        val datas: Array[String] = line.split(" ")
        ((datas(1),datas(4)),1)
      }
    )
    // 转换后的数据分组聚合：（（省份，广告），sum）
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    // 聚合的结果进行结构转换：（省份，（广告，sum））
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((prv, ad), sum) =>
        (prv, (ad, sum))
    }
    // 相同的省份进行分组：（省份，（（广告，sum），（广告，sum），（广告，sum）））
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()
    // 根据数量组内降序排序，取前三名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    // 打印
    resultRDD.collect().foreach(println)

    sc.stop()
  }

}
