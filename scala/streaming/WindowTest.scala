package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 0:58
*   @Description : 
*
*/
/**
 * window 窗口函数 ，可以每隔一段时间，将最近的窗口长度内的数据组成一个DStream处理。
 */
object WindowTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("test")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")
    val lines = ssc.socketTextStream("mynode5",9999)
    val blackList = List[String]("zhangsan","lisi","wangwu")
    // window function
    val windowDs: DStream[String] = lines.window(Durations.seconds(15), Durations.seconds(5))
    // input data
    val result = windowDs.filter(one => {
      !blackList.contains(one.split(" ")(1))
    })
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
