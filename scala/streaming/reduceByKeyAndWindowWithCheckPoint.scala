package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 0:29
*   @Description : 
*
*/
object reduceByKeyAndWindowWithCheckPoint {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")
    ssc.checkpoint("./data/ck")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("mynode5",9999)
    val words: DStream[String] = lines.flatMap(line=>{line.split(" ")})
    val pairWords: DStream[(String, Int)] = words.map(word=>{(word,1)})
    val result: DStream[(String, Int)] =
      pairWords.reduceByKeyAndWindow((v1:Int, v2:Int)=>{v1+v2},(v1:Int, v2:Int)=>{v1-v2},Durations.seconds(15),Durations.seconds(5))
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
