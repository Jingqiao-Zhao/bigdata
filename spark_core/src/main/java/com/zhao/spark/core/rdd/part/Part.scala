package com.zhao.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Part {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "xxxsxxx"),
      ("cba", "xxxssxxx"),
      ("cba", "xssxxxxx"),
      ("wnba", "xxxxxx"),
    ))

    val value: RDD[(String, String)] = rdd.partitionBy(new Mypartitioner)

    value.saveAsTextFile("output")


    sc.stop()
  }

  class Mypartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3
    // 根据数据的key值返回数据的分区索引，从0开始
    override def getPartition(key: Any): Int = {
      key match{
        case "nba" => 0
        case "cba" => 1
        case "wnba" =>2
      }
    }


  }

}

