package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object SortByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("./data/words")
    val words = lines.flatMap(line => {line.split(" ")})
    val pairWords = words.map(word => {new Tuple2(word, 1)}) // mapper
    val reduce: RDD[(String, Int)] = pairWords.reduceByKey((v1, v2) => {v1 + v2})

    // sort by the frequence of the key
    val transRDD1: RDD[(Int, String)] = reduce.map(tp=>tp.swap)
    val sortedRDD = transRDD1.sortByKey(false)
    val result = sortedRDD.map(tp => tp.swap)
    result.foreach(println)

  }
}
