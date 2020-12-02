package example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object FindNormalMonitorInfo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    //0006	68871
    val baseInfos = sc.textFile("./data/monitor_camera_info")
    //2020-07-14	0005	27430	鲁A17158	2020-07-14 14:39:48	194	2	05
    val monitorInfos = sc.textFile("./data/monitor_flow_action")
    //Calculate the number of camera in each block
    val baseMonitorCountInfos: RDD[(String, Int)] = baseInfos.map(line => {
      (line.split("\t")(0),1)
    }).reduceByKey((v1, v2) => {v1 + v2})
    val factMonitorCountInfos: RDD[(String, Int)] = monitorInfos.map(line => {
      line.split("\t")(1) + "_" + line.split("\t")(2)
    }).distinct().map(one =>{
      (one.split("_")(0),1)
    }).reduceByKey((v1, v2) => {v1 + v2})

    val joinInfo: RDD[(String, (Int, Int))] = baseMonitorCountInfos.join(factMonitorCountInfos)
    val normalInfos: RDD[(String, (Int, Int))] = joinInfo.filter(tp =>{
      tp._2._1 == tp._2._2
    })
    normalInfos.foreach(tp => {
      println(s"Information of block ：${tp._1}")
    })
  }
}
