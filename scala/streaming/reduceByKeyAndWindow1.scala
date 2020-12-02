package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 0:22
*   @Description : 
*
*/
/**
 *  需求：
 *    每隔一段时长，统计最近一段时间数据。
 *  reduceByKeyAndWindow： 每隔一段时长统计最近一段时间的内的数据。
 *    注意：
 *      窗口长度 - window length - wl
 *      滑动间隔 - slding interval - si
 *      窗口长度和滑动间隔必须是batchInterval的整数倍
 *
 */
object reduceByKeyAndWindow1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("mynode5",9999)
    val words: DStream[String] = lines.flatMap(line => {
      line.split(" ")
    })
    val pairWords: DStream[(String, Int)] = words.map(word => (word, 1))
    /**
     *  每隔 滑动间隔  统计最近 窗口长度内的数据，按照指定逻辑计算。
     */
    val result:DStream[(String, Int)] = pairWords.reduceByKeyAndWindow((v1:Int, v2:Int) => {v1 + v2}, Durations.seconds(15), Durations.seconds(5))
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
