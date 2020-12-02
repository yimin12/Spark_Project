package example

import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 20:58
*   @Description : 
*
*/
object FindCarInfo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    //2020-07-14	0004	36223	鲁A17158	2020-07-14 14:32:41	42	44	05
    val monitorInfos = sc.textFile("./data/monitor_flow_action")
    // 05(7) -> 鲁A17158(3)
    val area01Infos = monitorInfos.filter(line=>{"01".equals(line.split("\t")(7))}).map(line=>{line.split("\t")(3)}).distinct()
    val area05Infos = monitorInfos.filter(line=>{"05".equals(line.split("\t")(7))}).map(line=>{line.split("\t")(3)}).distinct()
    area01Infos.intersection(area05Infos).foreach(car => {
      println(s"区域 01 与区域 05 同时出现的车辆有：$car")
    })

  }
}
