package workflow.model

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import workflow.mathanalysis.ModelEvaluate
import scala.collection.mutable.HashMap
import workflow.dataset.DataSet
import workflow.dataset.Column
import workflow.mathanalysis.util

class TrainLogisticRegression(modelParams: HashMap[String,String]) extends DataLearning {


  
  private var LRmodel         :LogisticRegressionModel      = null
  val params        :HashMap[String,String]       = new HashMap[String,String]() 

  /**run LogisticRegression model*/
  def runModel(data:DataSet){
   //tempDatadeal
    

    val LabelRdd = data.toLabeledData 
    
    val maxIterations     = modelParams.getOrElse("logreg.max.iteration", "10").toDouble.toInt
    val step              = modelParams.getOrElse("logreg.step.size", "1").toDouble.toInt
    val miniBatchFraction = modelParams.getOrElse("logreg.mini.batch.fraction", "1").toDouble
    val weights           = modelParams.getOrElse("logreg.initial.weights", "")
    
    LRmodel = if(weights == "not set" || weights.equals("")) TrainLogisticRegression.train(LabelRdd.data, maxIterations, step, miniBatchFraction) 
    else {
      val weg = weights.split(",").map(_.toDouble)
      TrainLogisticRegression.train(LabelRdd.data, maxIterations, step, miniBatchFraction,Vectors.dense(weg))
    }
    saveModel
  }  
  
  def rebulitModel(modelPath: String):this.type = {
    readParams(modelPath)
    if(!params.isEmpty){
      LRmodel = createModel
      return this
    } else throw new Exception("模型重建失败，原因是该模型参数为空！")
  } 
  
  def createModel(): LogisticRegressionModel = {
    val intercept = params.get("intercept").get.toDouble
    val weigths   = params.get("weights").get.split(",").map(_.toDouble)
    new LogisticRegressionModel(Vectors.dense(weigths), intercept)
  }
  
  def saveModel() {
    if(LRmodel != null){
      val intercept = LRmodel.intercept
      val weights    = LRmodel.weights.toArray.mkString(",")
      params.clear
      params("intercept") = intercept.toString
      params("weights")   = weights
    }    
  }
  
  
  
  def predict(data:DataSet):DataSet = {
    if(LRmodel != null) {
      val predictData = data.toVectorData
                            .filterColumnWithCondition(col => col.isInput)
                            .getData
                            
      val preData  = LRmodel.predict(predictData)
      
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




object TrainLogisticRegression {
  
  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient. The weights used in
   * gradient descent are initialized using the initial weights provided.
   * NOTE: Labels used in Logistic Regression should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   * @param initialWeights Initial set of weights to be used. Array should be equal in size to
   *        the number of features in the data.
   */
  
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double,
      initialWeights: Vector): LogisticRegressionModel = {
      LogisticRegressionWithSGD.train(input,numIterations,stepSize,miniBatchFraction,initialWeights)
  }

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient.
   * NOTE: Labels used in Logistic Regression should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.

   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double): LogisticRegressionModel = {
     LogisticRegressionWithSGD.train(input,numIterations,stepSize,miniBatchFraction)
  }
}
