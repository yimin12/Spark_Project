package streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 0:52
*   @Description : 
*
*/
object TransformTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("test")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    val lines = ssc.socketTextStream("mynode5",9999)
    /**
     * transform :
     *   1).DStream的Transformation算子，可以获取DStream中的RDD，对RDD进行RDD的Transformation类算子转换，也可以使用Action算子。
     *   但是一定最后需要返回一个RDD，将返回的RDD会包装到一个DStream中。
     *   2).与foreachRDD类似，算子内的代码获取的RDD算子外的代码是在Driver端执行的，可以通过这个算子来动态的改变广播变量的值
     */
    val result: DStream[String] = lines.transform(rdd => {
      val value: RDD[String] = rdd.filter(one => {
        !one.contains("zhangsan")
      })
      value
    })
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
