package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/1 23:32
*   @Description : 
*
*/
/**
 * Driver  HA 实现：
 *   Driver HA 实现有两个层面：
 *      1.在提交Spark 任务时 指定参数 --supervise ,当Driver挂掉会自动重启
 *      2.代码中实现Driver HA 回复对应的 配置、执行逻辑、数据处理位置信息。
 *
 *     利用checkpoint实现,checkpoint 中存储数据：
 *        1.SparkStreaming的配置信息
 *        2.SparkStreaming执行逻辑
 *        3.SparkStreaming处理数据批次
 *    StreamingContext.getOrCreate(ckdir,createStreamingContext) :
 *       首先从checkpoint目录中获取对应的数据信息回复StreamingContext,如果不能回复可以根据  createStreamingContext 来创建StreamingContext对象。
 *
 *   我们可以利用SparkStreaming Driver HA 恢复数据处理的位置，但是这种方式有问题：
 *     1).将旧的执行逻辑同时恢复过来，如果代码逻辑改变，新的代码逻辑不能执行，这种方式有局限性。
 *     2).这种利用checkpoint方式来恢复数据处理位置，会将最近处理过的批次重复读取，会造成重复数据处理。这种方式有局限性。
 *
 */
object DriverHaTest {

  // check point address
  var ckdir = "./data/ck"
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getOrCreate(ckdir,createStreamingContext)
    ssc.start()
    ssc.awaitTermination()
  }

  def createStreamingContext():StreamingContext = {
    println("*********  create new streamingContext *********")
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")
    ssc.checkpoint(ckdir)
    val lines: DStream[String] = ssc.textFileStream("./data/streamingCopyFile")
    val words = lines.flatMap(line => {line.split(" ")})
    val pairWords = words.map(word => {(word, 1)})
    val result = pairWords.reduceByKey((v1,v2) => {v1 + v2})
    result.map(tp => {
      println("******************" + tp)
      tp._1 + "_" + tp._2
    }).print()
    ssc
  }
}
