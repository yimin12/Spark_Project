package persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 20:43
*   @Description : 
*
*/
/**
 * persist :持久化算子，可以手动指定持久化的级别。懒执行算子，需要action算子进行触发。
 *   persist() 默认将数据持久化到内存中,cache() = persist() = persist(StorageLevel.MEMORY_ONLY)
 *   常用的持久化级别：
 *     MEMORY_ONLY :数据放在内存中
 *     MEMORY_ONLY_SER:数据序列化之后存放在内存中，序列化之后的数据节省空间。时间换取空间。
 *     MEMORY_AND_DISK: 数据首先存放内存，内存存不下放磁盘
 *     MEMORY_AND_DISK_SER: 数据序列化之后，首先放内存，存不下放磁盘。
 *   注意：
 *     尽量不使用“_2”级别。
 *     尽量避免使用“DISK_ONLY”级别。
 *
 *  cache() 与 persist() 注意问题：
 *  1.cache() 与persist都是懒执行算子，需要action算子触发执行。持久化数据的单位是partition
 *  2.可以对一个RDD使用cache或者persist之后赋值给一个变量，在其他job中使用这个变量就是使用持久化的数据。建议不赋值。
 *  3.如果采用第二种方式对RDD进行持久化，对RDD进行持久化之后不能紧跟action算子。
 */
object PersistTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("cache test")
    val sc = new SparkContext(conf)
    var lines: RDD[String] = sc.textFile("./data/persistData.txt")

    lines = lines.persist()
    //    lines.cache()
    lines.persist(StorageLevel.MEMORY_ONLY)

    val count = lines.count()
    println(s"count = $count")

  }
}
