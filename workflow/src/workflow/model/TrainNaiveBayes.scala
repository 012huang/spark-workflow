package workflow.model

import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import scala.math._
import scala.collection.mutable.HashMap
import workflow.dataset.DataSet
import workflow.dataset.Column
import workflow.mathanalysis.util

class TrainNaiveBayes(modelParams: HashMap[String,String]) extends DataLearning {

  
  private var NBmodel    :NaiveBayesModel         = null
  val params             :HashMap[String,String]  = new HashMap[String,String]() 
  
  /**get naivebayes model*/
  def runModel(data:DataSet){
   //tempDatadeal
    

    val LabelRdd = data.toLabeledData

  
    val lambda = modelParams.getOrElse("naive.lambda", "1").toDouble.toInt
    NBmodel = TrainNaiveBayes.train(LabelRdd.data,lambda) 
    saveModel
  }  
  
  def rebulitModel(modelPath: String):this.type = {
    readParams(modelPath)
    if(!params.isEmpty){
      NBmodel = createModel
      return this
    } else throw new Exception("模型重建失败，原因是该模型参数为空！")
  } 
  
  def createModel(): NaiveBayesModel = {
    val labels = params.get("labels").get.split(",").map(_.toDouble)
    val pi     = params.get("pi").get.split(",").map(_.toDouble)
    val theta  = params.get("theta").get.split("##").map(_.split(",").map(_.toDouble))
    null
  }
  
  def saveModel(){
    val labels = NBmodel.labels.mkString(",")
    val pi     = NBmodel.pi.mkString(",")
    val theta  = NBmodel.theta.map(_.mkString(",")).mkString("##")
    params.clear
    params("labels") = labels
    params("pi")     = pi
    params("theta")  = theta    
  }
  

  def predict(data:DataSet):DataSet = {
    if(NBmodel != null) {
      val predictData = data.toVectorData
                            .filterColumnWithCondition(col => col.isInput)
                            .getData
                            
      val preData  = NBmodel.predict(predictData)
      
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
  

  def modelEvaluate{
    println("the evaluate is not ready ok")
  }
  
}


object TrainNaiveBayes {

  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * This is the Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all kinds of
   * discrete data.  For example, by converting documents into TF-IDF vectors, it can be used for
   * document classification.  By making every vector a 0-1 vector, it can also be used as
   * Bernoulli NB ([[http://tinyurl.com/p7c96j6]]).
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   * @param lambda The smoothing parameter
   */
  def train(input: RDD[LabeledPoint], lambda: Double): NaiveBayesModel = {
    NaiveBayes.train(input,lambda)
  }
}
