package streaming


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 0:31
*   @Description : 
*
*/
object SparkStreamingReadKafka {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SparkStreamingReadKafka")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    //    ssc.sparkContext.setLogLevel("Error")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "mynode1:9092, mynode2:9092,mynode3:9092", // kafka cluster
      "key.deserializer" -> classOf[StringDeserializer], // read data from kafka and set the serializer
      "value.deserializer" -> classOf[StringDeserializer], // same for the value
      "group.id" -> "firstgroup", // 指定消费者组，利用kafka管理消费者offset时，需要以组为单位存储。
      /**
       * latest :连接kafka之后,读取向kafka中生产的数据
       * earliest : 如果kafka中有当前消费者组存储的消费者offset,就接着这个位置读取数据继续消费。如果kafka中没有当前消费者组对应的消费offset,
       *       就从最早的位置消费数据。
       */
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)//是否开启自动向Kafka 提交消费者offset,周期5s
    )

    val topics = Array[String]("streamingtopic")
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent, //接收Kafka数据的策略，这种策略是均匀将Kafka中的数据接收到Executor中处理。
      Subscribe[String, String](topics, kafkaParams)
    )
    val lines: DStream[String] = ds.map(cr => {
      println(s"message key = ${cr.key()}")
      println(s"message value = ${cr.value()}")
      cr.value()
    })
    val words : DStream[String]= lines.flatMap(line=>{line.split("\t")})
    val pairWords: DStream[(String,Int)] = words.map(word=>{(word,1)})
    val result = pairWords.reduceByKey((v1,v2)=>{v1+v2})
    result.print()
    //保证业务逻辑处理完成的情况下，异步将当前批次offset提交给kafka,异步提交DStream需要使用源头的DStream
    ds.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // some time later, after outputs have completed
      ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges) // 异步向Kafka中提交消费者offset
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
