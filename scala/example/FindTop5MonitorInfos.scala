package example

import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object FindTop5MonitorInfos {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    //2020-07-14	0004	36223	鲁A17158	2020-07-14 14:32:41	42	44	05
    val monitorInfos = sc.textFile("./data/monitor_flow_action")
    //找出当天通过车辆数最高的top5卡扣信息
    val top5MonitorInfos : Array[String] = monitorInfos.map(line => {(line.split("\t")(1), 1)}).reduceByKey((v1, v2) => {v1 + v2})
      .sortBy(tp => {tp._2}, false).map(tp => {tp._1}).take(5)
//    top5MonitorInfos.foreach(println)
    /**
     * 0001
     * 0007
     * 0005
     * 0006
     * 0002
     */

    //根据通过车辆数最高top5卡扣信息，找出这些卡扣下通过的所有车辆信息
    monitorInfos.filter(line=>{
      top5MonitorInfos.contains(line.split("\t")(1))
    }).foreach(println)
  }
}
