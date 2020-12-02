package streaming

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Durations, StreamingContext}


/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 23:50
*   @Description : 
*
*/
object ForeachRDDTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val ssc = new StreamingContext(conf, Durations.seconds(5)) // batch interval 5 secs
    val lines = ssc.socketTextStream("mynode5", 9999)
    val result: DStream[(String, Int)] = lines.flatMap(line => {
      line.split(" ")
    }).map(word => {
      new Tuple2(word, 1)
    }).reduceByKey((v1, v2) =>{
      v1 + v2
    })
    /**
     * foreachRDD :
     *    1).foreachRDD 可以获取DStream中的RDD，可以对RDD使用RDD的transformation类算子进行转换，
     *       但是一定要对RDD进行Actioin算子触发，不然DStream的逻辑也不会执行。
     *    2).foreachRDD算子内的代码，获取的RDD算子外的代码是在Driver端执行的，每隔batchInterval执行一次。可以利用这个特点动态的改变广播变量
     */
    result.foreachRDD(rdd => {
      println("**** Driver ****")
      val bc: Broadcast[String] = rdd.sparkContext.broadcast("Reading data form database")
      val map: RDD[String] = rdd.map(tp => {
          println("=========== Executor-map ==============" + tp)
        val value: String = bc.value
        tp._1 + "_" + tp._2
      })
      val end = map.filter(s => {
        true
      })
      end.foreach(println)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
