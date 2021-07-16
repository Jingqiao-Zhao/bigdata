package com.zhao.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Persist {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(conf)
    // 保存目录一般在分布式系统中
    sc.setCheckpointDir("./sc")
    val fileRDD: RDD[String] = sc.textFile("datas/*.txt")

    val flatRDD: RDD[String] = fileRDD.flatMap(
      str => str.split(" ")
    )

    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))

    // 持久化操作
    // 内存持久化
    mapRDD.cache()
    // 文件存储,设置存储级别,默认内存级别，在行动算子执行时完成的
    // 不一定为了重用，数据比较重要的场合也可以持久化操作，但是仍然属于临时文件，任务结束后删除
    mapRDD.persist(StorageLevel.DISK_ONLY)

    // 落盘操作，结束后依然存在
    mapRDD.checkpoint()
    /*
    *  缓存和检查点区别
        1） Cache 缓存只是将数据保存起来，不切断血缘依赖。 Checkpoint 检查点切断血缘依赖。
        2） Cache 缓存的数据通常存储在磁盘、内存等地方，可靠性低。
           Checkpoint 的数据通常存储在 HDFS 等容错、高可用的文件系统，可靠性高。
        3）建议对 checkpoint()的 RDD 使用 Cache 缓存，这样 checkpoint 的 job 只需从 Cache 缓存
        中读取数据即可，否则需要再从头计算一次 RDD。
     * */

    // 对象看起来重用，但是RDD本身并不存储数据，实际上是又读取了一遍
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    reduceRDD.collect().foreach(println)
    groupRDD.collect().foreach(println)



    sc.stop()
  }

}
