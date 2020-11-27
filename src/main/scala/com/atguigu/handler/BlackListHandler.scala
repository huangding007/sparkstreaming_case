package com.atguigu.handler

import java.sql.{Connection, Date}
import java.text.SimpleDateFormat

import com.atguigu.app.Ads_log
import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

object BlackListHandler {

  //时间格式化对象
  private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def addBlackList(filterAdsLogDSteam: DStream[Ads_log]) = {
    //统计当前批次中单日每个用户点击每个广告的总次数
    //1.转换和累加:ads_log=>((date,user,adid),1)=>((date,user,adid),count)
    val dateUserAdToCount: DStream[((String, String, String), Long)] = filterAdsLogDSteam.map(
      adsLog => {
        //a.将时间戳转换为日期字符串
        val date: String = sdf.format(new Date(adsLog.timestamp))
        //b.返回值
        ((date, adsLog.userid, adsLog.adid), 1L)
      }
    ).reduceByKey(_ + _)
    //2.写出
    dateUserAdToCount.foreachRDD(
      rdd => {
        //每个分区数据写一次
        rdd.foreachPartition(
          iter => {
            //获取连接
            val connection: Connection = JDBCUtil.getConnection

            iter.foreach {
              case ((dt, user, ad), count) =>
                //向mysql中user_ad_count表，更新累加点击次数
                JDBCUtil.executeUpdate(
                  connection,
                  """
                    |insert into user_ad_count (dt,userid,adid,count)
                    |values(?,?,?,?)
                    |on duplicate key
                    |update count=count+?
                    |""".stripMargin, Array(dt, user, ad, count, count)
                )
                //查询user_ad_count表，读取mysql中点击次数
                val ct: Long = JDBCUtil.getDataFromMysql(
                  connection,
                  """
                    |select count from user_ad_count where dt=? and userid=? and adid=?
                    |""".stripMargin, Array(dt, user, ad)
                )
                if (ct >= 30) {
                  JDBCUtil.executeUpdate(connection,
                    """
                      |insert into black_list (userid) values (?) on duplicate key update userid=?
                      |""".stripMargin, Array(user, user))
                }
            }
            connection.close()
          }
        )
      }
    )
  }

  //判断用户是否在黑名单中
  def filterByBlackList(adsLogDStream: DStream[Ads_log]): DStream[Ads_log] = {
    adsLogDStream.filter(
      adsLog => {
        //获取连接
        val connection: Connection = JDBCUtil.getConnection
        //判断黑名单中是否存在该用户
        val bool: Boolean = JDBCUtil.isExist(connection,
          """
            |select * from black_list where userid=?
            |""".stripMargin, Array(adsLog.userid))
        connection.close()
        //返回是否存在标记
        !bool
      }

    )
  }
}
