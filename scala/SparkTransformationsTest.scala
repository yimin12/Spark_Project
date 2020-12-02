import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/11/30 14:39
*   @Description : 
*
*/
object SparkTransformationsTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("transformations test")
    val sc = new SparkContext(conf)
    val lines :RDD[String] = sc.textFile("./data/words")
    val words: RDD[String] = lines.flatMap(line => {
      println("crrent line is " + line)
      line.split(" ")
    })
    val pairWords: RDD[(String, Int)] = words.map(word =>{
      println("current word is " + word)
      new Tuple2(word, 1)
    })
    pairWords.foreach(println)
  }
}
