package example

import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 23:15
*   @Description : 
*
*/
case class SecondSortKey(first:Int,second:Int) extends Ordered[SecondSortKey] {
  override def compare(that: SecondSortKey): Int = {
    if(this.first == that.first){
      this.second - that.second
    } else {
      this.first - that.first
    }
  }
}

object SecondSortTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./data/SecondSort.txt")
    val pairInfos = lines.map(line =>{
      val first = line.split("\t")(0).toInt
      var second = line.split("\t")(1).toInt
      (SecondSortKey(first, second), line)
    })
    pairInfos.sortByKey(false).foreach(tp=>{
      println(tp._2)
    })
  }
}
