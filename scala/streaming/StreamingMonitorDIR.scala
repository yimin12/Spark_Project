package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 0:41
*   @Description : 
*
*/
/**
 *  textFileStream :
 *    可以监控目录下的增量文件数据，增加的数据格式必须是text格式，并且目录下已经存在的数据不能监控到，已经存在的数据追加数据也不能被监控到。
 *    只能监控原子产生在目录下的数据。
 *
 *  saveAsTextFile : 将数据结果保存到目录中，可以指定前缀后缀，多级目录可以指定在前缀中。
 *
 */
object StreamingMonitorDIR {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    val lines: DStream[String] = ssc.textFileStream("./data/streamingCopyFile")
    lines.flatMap(line=>{
      line.split(" ")
    }).map(word => {(word, 1)}).reduceByKey((v1, v2) => (v1 + v2)).saveAsTextFiles("./data/streamingresult/aaa","bbb")
    ssc.start()
    ssc.awaitTermination()
  }
}
