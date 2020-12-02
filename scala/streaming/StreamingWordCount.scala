package streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 0:46
*   @Description : 
*
*/
/**
 *  SparkStreaming 读取Socket中的数据进行实时wordcount统计。
 *     注意:
 *       1).nc -lk 9999 在linux中启动一个socke服务器
 *       2).本地运行SparkStreaming程序需要设置 local[2] ,如果集群中运行，需要至少两个core
 *       3).写SparkStreaming程序需要创建StreamingContext对象，创建StreamingContext对象有两种方式：
 *           ①.val ssc = new StreamingContext(sc,Durations.Seconds(xxx))
 *           ②.val ssc = new StreamingContext(conf,Durations.Seconds(xxx))
 *       4).Durations.seconds(5) 代表batchInterval ,我们指定的可以接受的数据延迟度。可以设置：seconds,minutes,milliseconds
 *         如何设置batchInterval 需要参照webui 调节。
 *
 *       5).SparkStreaming 启动之后，不能添加新的代码逻辑。
 *       6).SparkStreaming.stop(stopSparkContext = true),在关闭StreamingContext时，需要不需要关闭parkContext对象
 *       7).SparkStreaming.stop 之后不能再次调用SparkStreaming.start()重新将程序启动。
 *
 *
 */
object StreamingWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("streamingwc")
    //    val sc = new SparkContext(conf)
    //    sc.setLogLevel("Error")
    val ssc: StreamingContext = new StreamingContext(conf, Durations.seconds(5))
    val sc: SparkContext = ssc.sparkContext
    sc.setLogLevel("Error")
    /**
     * hello spark
     * hello java
     * hello streaming
     */
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("mynode5",9999)
    val words: DStream[String] = lines.flatMap(line=>{line.split(" ")})
    val pairWords: DStream[(String, Int)] = words.map(word => {new Tuple2(word, 1)})
    val result = pairWords.reduceByKey((v1, v2) => {v1 + v2})
    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false)

  }
}
