package example

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 13:45
*   @Description : 
*
*/
object GroupTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val lines = sc.textFile("./data/groupData.txt")
    val pairInfos = lines.map(line =>{
      val cls = line.split("\t")(0)
      val score = line.split("\t")(1).toInt
      (cls, score)
    })

    pairInfos.groupByKey().foreach(tp =>{
      val cls = tp._1
      val iter = tp._2.iterator
      val top3 = new Array[Int](3)
      val break = new Breaks
      while(iter.hasNext){
        val next = iter.next()
        break.breakable{
          for(i <- 0 until top3.length){
            if(top3(i) == 0){
              top3(i) = next
              break.break()
            } else if(next > top3(i)){
              for(j <- 2 until (i, -1)){
                top3(j) = top3(j-1)
              }
              top3(i) = next
              break.break()
            }
          }
        }
      }
      println(s"class = $cls,socres = ${top3.toBuffer}")


      //原生集合排序
      //      val key = tp._1
      //      val list: List[Int] = tp._2.toList
      //      val sortedList: List[Int] = list.sortWith((v1, v2)=>{v1>v2})
      //      if(sortedList.length > 3){
      //        for(i <- 0 until 3){
      //          println(s"class = $key , score = ${sortedList(i)}")
      //        }
      //      }else{
      //        for(score <- sortedList)
      //          println(s"class = $key ,score = $score")
      //      }

    })
  }
}
