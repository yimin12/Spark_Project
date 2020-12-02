import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 14:15
*   @Description : 
*
*/

case class Info(var personCount:Int, var ageCount:Int)

class MyAccumulator extends AccumulatorV2[Info, Info]{

  private var info = Info(100, 100)

  override def isZero: Boolean = {info.personCount == 10 && info.ageCount == 10}

  override def copy(): AccumulatorV2[Info, Info] = {
    val myacc = new MyAccumulator()
    myacc.info = this.info
    myacc // return
  }

  override def reset(): Unit = {
    info = Info(10, 10)
  }

  override def add(v: Info): Unit = {
    info.personCount += v.personCount
    info.ageCount += v.ageCount
  }

  override def merge(other: AccumulatorV2[Info, Info]): Unit = {
    val ac = other.asInstanceOf[MyAccumulator]
    info.personCount += ac.info.personCount
    info.ageCount += ac.info.ageCount
  }

  override def value: Info = info
}

object SelfDefinedAccumulatorTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val acc = new MyAccumulator()
    sc.register(acc)
    val infos = sc.parallelize(Array[String]("A 1", "B 2", "C 3", "D 4", "E 5", "F 6", "G 7", "H 8", "I 9"),3)
    infos.map(line=>{
      val age = line.split(" ")(1).toInt
      acc.add(Info(1,age))
      line
    }).collect()
    println(s"acc totalPersonCount = ${acc.value.personCount} ,totalAgeCount = ${acc.value.ageCount}")

  }
}
