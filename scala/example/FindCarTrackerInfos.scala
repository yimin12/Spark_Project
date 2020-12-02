package example

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object FindCarTrackerInfos {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    //2020-07-14	0005	99311	鲁A17158	2020-07-14 14:04:42	127	14	02
    val monitorInfos = sc.textFile("./data/monitor_flow_action")
    //找出卡扣0001下通过的所有车辆信息
    val allCars: Array[String] = monitorInfos.filter(line => {"0001".equals(line.split("\t")(1))}).map(line => {line.split("\t")(3)}).distinct().collect()

    // Analyze the track of vehicle
    monitorInfos.filter(line => {
      val car = line.split("\t")(3)
      allCars.contains(car)
    }).map(line => {
      val car = line.split("\t")(3)
      (car, line)
    }).groupByKey().map(tp=>{
      val car = tp._1
      val list: List[String] = tp._2.toList
      val sortedList: List[String] = list.sortWith((s1, s2) =>{
        val actionTime1 = s1.split("\t")(4)
        val actionTime2 = s2.split("\t")(4)
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val time1 = dateFormat.parse(actionTime1)
        val time2 = dateFormat.parse(actionTime2)
        time1.before(time2)
      })
      var carTracker = ""
      for(line <- sortedList){
        carTracker += line.split("\t")(1) + "_" + line.split("\t")(4) + "->"
      }
      (car, carTracker.substring(0, carTracker.length-2))
    }).foreach(tp => {
      println(s"car = ${tp._1} ,carTracker = ${tp._2}")
    })
  }
}
