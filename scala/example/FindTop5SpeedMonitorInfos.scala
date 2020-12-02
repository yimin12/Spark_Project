package example

import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 22:25
*   @Description : 
*
*/
/**
 * 找出通过车辆速度高的top5卡扣信息
 */
//first : 低速  second:正常   third:中速    fourth:高速
case class SpeedSortKey(first:Int, second:Int, third:Int, fourth:Int) extends Ordered[SpeedSortKey]{
  override def compare(that: SpeedSortKey): Int = {
    if(this.fourth != that.fourth){
      this.fourth - that.fourth
    } else if(this.third != that.third){
      this.third - that.third
    } else if(this.second != that.second){
      this.second - that.second
    } else {
      this.first - that.first
    }
  }
}
object FindTop5SpeedMonitorInfos {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    //2020-07-14	0004	36223	鲁A17158	2020-07-14 14:32:41	42	44	05
    val monitorInfos = sc.textFile("./data/monitor_flow_action")
    monitorInfos.map(line => {
      (line.split("\t")(1), line.split("\t")(5))
    }).groupByKey().map(tp => {
      val monitorId = tp._1
      val iter = tp._2.iterator
      var firstCount = 0
      var secondCount = 0
      var thirdCount = 0
      var fourthCount = 0
      while(iter.hasNext){
        val currentSpeed = iter.next().toInt
        if(currentSpeed <= 60){
          firstCount += 1
        } else if(currentSpeed <= 90){
          secondCount += 1
        } else if(currentSpeed <= 120){
          thirdCount += 1
        } else {
          fourthCount += 1
        }
      }
      (SpeedSortKey(firstCount, secondCount, thirdCount, fourthCount), monitorId)
    }).sortByKey(false).map(tp =>{tp._2}).take(5).foreach(println)

  }
}
