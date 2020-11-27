package com.atguigu.app

import java.util.Properties
import com.atguigu.handler.BlackListHandler
import com.atguigu.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {
  def main(args: Array[String]): Unit = {
    //初始化配置
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //3.读取数据
    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic: String = properties.getProperty("kafka.topic")
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,ssc)
    //4.将从kafka读出的数据转换为样例类对象
    val adsLogDStream: DStream[Ads_log] = kafkaDStream.map(
      record => {
        val value: String = record.value()
        val arr: Array[String] = value.split(" ")
        Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
      }
    )
    //5.需求一：根据mysql中黑名单过滤当前数据集
    val filterAdsLogDStream: DStream[Ads_log] = BlackListHandler.filterByBlackList(adsLogDStream)
    //6.需求一：将满足要求的用户写入黑名单
    BlackListHandler.addBlackList(filterAdsLogDStream)

    //测试打印
    filterAdsLogDStream.cache()
    filterAdsLogDStream.count().print()

    //启动阻塞
    ssc.start()
    ssc.awaitTermination()
  }

}

// 时间 地区 城市 用户id 广告id
case class Ads_log(timestamp: Long, area: String, city: String, userid: String, adid: String)
