package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 0:55
*   @Description : 
*
*/
/**
 *  updataeStateByKey :
 *   1).可以更新key的状态,统计自从SparkStreaming 启动以来所有key的状态值
 *   2).需要设置checkpoint 来保存之前所有key对应的value状态值
 *        默认状态是在内存中的，必须设置checkpoint保存状态，多久将内存中的装填向checkpoint持久化一次？
 *          如果batchInterval 小于10s ,那就10s保存一次，如果batchInterval 大于10s 就batchInterval 保存一次，为了避免频繁的磁盘IO。
 *
 */
object UpdateStateByKeyTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("test")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")
    ssc.checkpoint("./data/ck")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("mynode5",9999)
    val words : DStream[String]= lines.flatMap(line=>{line.split(" ")})
    val pairWords : DStream[(String, Int)] = words.map(word=>{(word,1)})
    /**
     *  updateStateByKey : 按照key 分组，针对每个分组内的数据进行处理
     *   seq： 当前组内的value组成的集合
     *   option : 当前批次之前某个key对应的值
     */
    val result: DStream[(String, Int)] = pairWords.updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
      var value = option.getOrElse(0)
      for (currentValue <- seq) {
        value += currentValue
      }
      Option(value)
    })
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
