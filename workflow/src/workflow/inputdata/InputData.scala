package workflow.inputdata

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * 数据读取接口
 **/
trait InputData {
  
  //获取数据接口
  def load():RDD[String]

}


/**
 * 从hdfs中读数据
 * @params hdfs 数据hdfs连接
 * @params sc   SparkContext
 * */
class FromHdfs(sc: SparkContext, hdfs: String) extends InputData{
  
  def load(): RDD[String] = {
      sc.textFile(hdfs)
  }
}


/**
 * 从关系型数据库中读数据
 * @params sc        SparkContext
 * @params driver    数据库驱动路径
 * @params url       数据库连接
 * @params usrName   用户名
 * @params usrPasswd 密码
 * @params sqlConf   sql语句
 * */
class FromRDBS(
      sc        :SparkContext,
      driver    :String,
      url       :String,
      usrName   :String,
      usrPasswd :String,
      sqlConf   :String
      ) extends InputData {

  def load() :RDD[String] = {
      //RunSparkTool.getQueryResult(sc, driver, url, usrName, usrPasswd, sqlConf)
    null
  }
}
