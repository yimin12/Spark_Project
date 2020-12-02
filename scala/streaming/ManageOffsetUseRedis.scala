package streaming

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

import scala.collection.mutable
/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 0:02
*   @Description : 
*
*/
/**
 * 利用redis 来维护消费者偏移量
 */
object ManageOffsetUseRedis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("manageoffsetuseredis")
    //设置每个分区每秒读取多少条数据
    conf.set("spark.streaming.kafka.maxRatePerPartition","10")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    //设置日志级别
    ssc.sparkContext.setLogLevel("Error")

    val topic = "mytopic"
    /**
     * 从Redis 中获取消费者offset
     */
    val dbIndex = 3
    //从Redis中获取存储的消费者offset
    val currentTopicOffset: mutable.Map[String, String] = getOffSetFromRedis(dbIndex,topic)
    //初始读取到的topic offset:
    currentTopicOffset.foreach(tp=>{println(s" 初始读取到的offset: $tp")})
    val fromOffsets: Predef.Map[TopicPartition, Long] = currentTopicOffset.map { resultSet =>
      new TopicPartition(topic, resultSet._1.toInt) -> resultSet._2.toLong
    }.toMap

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "mynode1:9092,mynode2:9092,mynode3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "MyGroupId11",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)//默认是true
    )

    /**
     * 将获取到的消费者offset 传递给SparkStreaming
     */
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
    )
    stream.foreachRDD { (rdd:RDD[ConsumerRecord[String, String]]) =>

      println("**** 业务处理完成  ****")

      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"topic:${o.topic}  partition:${o.partition}  fromOffset:${o.fromOffset}  untilOffset: ${o.untilOffset}")
      }

      //将当前批次最后的所有分区offsets 保存到 Redis中
      saveOffsetToRedis(dbIndex,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

  /**
   * Save the offset to Redis
   */
  def saveOffsetToRedis(db:Int, offsetRanges:Array[OffsetRange]) = {
    val jedis = RedisClient.pool.getResource
    jedis.select(db)
    offsetRanges.foreach(offsetRanges => {
      jedis.hset(offsetRanges.topic, offsetRanges.partition.toString, offsetRanges.untilOffset.toString)
    })
    println("Saved")
    RedisClient.pool.returnResource(jedis)
  }

  /**
   * Get the offset info from redis
   * @param db
   * @param topic
   * @return
   */
  def getOffSetFromRedis(db:Int,topic:String)  ={
    val jedis = RedisClient.pool.getResource
    jedis.select(db)
    val result: util.Map[String, String] = jedis.hgetAll(topic)
    RedisClient.pool.returnResource(jedis)
    if(result.size()==0){
      result.put("0","0")
      result.put("1","0")
      result.put("2","0")
    }
    import scala.collection.JavaConversions.mapAsScalaMap
    val offsetMap: scala.collection.mutable.Map[String, String] = result
    offsetMap
  }
}
