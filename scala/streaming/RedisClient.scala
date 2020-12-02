package streaming

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/*
*   @Author : Yimin Huang
*   @Contact : hymlaucs@gmail.com
*   @Date : 2020/12/2 0:05
*   @Description : 
*
*/
object RedisClient {

  val redisHost = "mynode4"
  val redisPort = 6379
  val redisTimeout = 30000

  /**
   * JedisPool是一个连接池，既可以保证线程安全，又可以保证了较高的效率。
   */
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)
}
