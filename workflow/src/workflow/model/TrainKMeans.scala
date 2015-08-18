package workflow.model

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import scala.collection.mutable.HashMap
import workflow.dataset.DataSet
import workflow.dataset.Column
import workflow.mathanalysis.util

class TrainKMeans(modelParams: HashMap[String,String]) extends DataLearning {

  
  private var KMmodel :KMeansModel            = null
  val params          :HashMap[String,String] = new HashMap[String,String]()
  
  def runModel(data:DataSet){
    

    val vectorRdd = data.toVectorData.filterColumnWithCondition(_.isInput)
  
    
    
    val k = modelParams.getOrElse("kmeans.k", "5").toDouble.toInt
    val maxIterations = modelParams.getOrElse("kmeans.max.iteration", "10").toDouble.toInt
    val runs = modelParams.getOrElse("kmeans.runs", "1").toDouble.toInt
    val initializationMode = modelParams.getOrElse("kmeans.initialization.mode", "k-means||")    
    KMmodel = TrainKMeans.train(vectorRdd.data, k, maxIterations, runs, initializationMode)  
    saveModel
  }
  
  def rebulitModel(modelPath: String):this.type = {
    readParams(modelPath)
    if(!params.isEmpty){
      KMmodel = createModel
      return this
    } else throw new Exception("模型重建失败，原因是该模型参数为空！")
  } 
  
  
  def createModel():KMeansModel = {
    val k       = params.get("k").get.toInt
    var centers = Array[Vector]()
    for(i <- 0 until k) {
      val center = params.get("centers_" + i).get.split(",").map(_.toDouble)
      centers(i) = Vectors.dense(center)
    }    
    new KMeansModel(centers)
  }
  
  /**
   * 保存模型参数到params，数组保存为String类型，默认分隔符为","
   * */
  def saveModel(){
    if(KMmodel != null) {
      val centers = KMmodel.clusterCenters.map(_.toArray)
      val k       = KMmodel.k
      params.clear
      params("k") = k.toString
      for(i <- 0 until k) {
    	  params("centers_" + i) = centers(i).mkString(",")
      }
    }
  }

  
  def predict(data:DataSet):DataSet = {
    if(KMmodel != null) {
      val predictData = data.toVectorData
                            .filterColumnWithCondition(col => col.isInput)
                            .getData
                            
      val preData  = KMmodel.predict(predictData)
      
      val origData = data.getData
      
      val result = predictData.zip(preData).map{
        case(old,newdata) => {
          old.toArray.map(_.toString) ++ Array(newdata.toString)
        }
      }
      data.setData(result)
      val newColumn = data.getColumnSet
      val tmpCol = new Column(newColumn.length ,"DecisionTreeResult")
      newColumn.addColumn(tmpCol)
      data.setColumnSet(newColumn)
      } else {
        throw new Exception("没有训练模型,不能得到运行结果")
    }     
    data
  }

  
}



object TrainKMeans {
  
  /**
   * Trains a k-means model using the given set of parameters.
   *
   * @param data training points stored as `RDD[Array[Double]]`
   * @param k number of clusters
   * @param maxIterations max number of iterations
   * @param runs number of parallel runs, defaults to 1. The best model is returned.
   * @param initializationMode initialization model, either "random" or "k-means||" (default).
   */

  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String): KMeansModel = {
      KMeans.train(data,k,maxIterations,runs,initializationMode)
  }

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int): KMeansModel = {
      KMeans.train(data,k,maxIterations)
  }

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int): KMeansModel = {
      KMeans.train(data,k,maxIterations,runs)
  }

}
