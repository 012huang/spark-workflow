package workflow.model

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.classification.{SVMWithSGD,SVMModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import workflow.mathanalysis.ModelEvaluate
import scala.collection.mutable.HashMap
import workflow.dataset.DataSet
import workflow.dataset.Column
import workflow.mathanalysis.util

class TrainSVM(modelParams: HashMap[String,String]) extends DataLearning {

  
  private var SVMmodel    :SVMModel                = null
  
  var clearThreshold      :Boolean                 = false
  val params              :HashMap[String,String]  = new HashMap[String,String]() 

  /**get SVM model*/
  def runModel (data:DataSet){
   //tempDatadeal
    

     val LabelRdd = data.toLabeledData

  
    
    
    val maxIterations       = modelParams.getOrElse("svm.max.iteration", "10").toDouble.toInt
    val step                = modelParams.getOrElse("svm.step.size", "1").toDouble.toInt
    val regParam            = modelParams.getOrElse("svm.reg.param", "1").toDouble
    val miniBatchFraction   = modelParams.getOrElse("svm.mini.batch.fraction", "1").toDouble
    val weights             = modelParams.getOrElse("svm.initial.weights", "not set")
    SVMmodel = if(weights == "not set" || weights.equals("") ) TrainSVM.train(LabelRdd.data, maxIterations, step, regParam, miniBatchFraction) 
    else {
      val weg = weights.split(",").map(_.toDouble)
      TrainSVM.train(LabelRdd.data, maxIterations, step, regParam, miniBatchFraction,Vectors.dense(weg))
    }
    saveModel
  }
  
  
  def rebulitModel(modelPath: String):this.type = {
    readParams(modelPath)
    if(!params.isEmpty){
      SVMmodel = createModel
      return this
    } else throw new Exception("模型重建失败，原因是该模型参数为空！")
  } 
  
  
  /**
   * 从模型参数文档重建模型
   * */
  def createModel(): SVMModel = {
    val intercept = params.get("intercept").get.toDouble
    val weigths   = params.get("weights").get.split(",").map(_.toDouble)
    new SVMModel(Vectors.dense(weigths), intercept)
  }
  
  def saveModel() {
    if(SVMmodel != null){
      val intercept = SVMmodel.intercept
      val weights    = SVMmodel.weights.toArray.mkString(",")
      params.clear
      params("intercept") = intercept.toString
      params("weights")   = weights
    }    
  }
  
  
  /**
   * 模型预测
   * */
  def predict(data:DataSet):DataSet = {
    if(SVMmodel != null) {
      val predictData = data.toVectorData
                            .filterColumnWithCondition(col => col.isInput)
                            .getData
                            
      val preData  = SVMmodel.predict(predictData)
      
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


object TrainSVM {

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient. The weights used in
   * gradient descent are initialized using the initial weights provided.
   *
   * NOTE: Labels used in SVM should be {0, 1}.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   * @param initialWeights Initial set of weights to be used. Array should be equal in size to
   *        the number of features in the data.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector): SVMModel = {
      SVMWithSGD.train(input, numIterations, stepSize, regParam, miniBatchFraction,initialWeights)      
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient.
   * NOTE: Labels used in SVM should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double): SVMModel = {
      SVMWithSGD.train(input, numIterations, stepSize, regParam, miniBatchFraction)      
  }


}
