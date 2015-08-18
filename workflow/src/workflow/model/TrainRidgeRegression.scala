package workflow.model

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.RidgeRegressionModel
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
import scala.collection.mutable.HashMap
import workflow.dataset.DataSet
import workflow.dataset.Column
import workflow.mathanalysis.util

class TrainRidgeRegression(modelParams: HashMap[String,String]) extends DataLearning {

  private var RRmodel :RidgeRegressionModel     = null
  
  val params          :HashMap[String,String]   = new HashMap[String,String]() 
  
  /**get RidgeRegression model*/
  def runModel(data:DataSet){
   //tempDatadeal
    

     val LabelRdd = data.toLabeledData
//   lazy val VectorRdd = data.filterColumnWithCondition(col => !col.isVoid).toVectorData

  


    val maxIterations      = modelParams.getOrElse("ridreg.max.iteration", "10").toDouble.toInt
    val step               = modelParams.getOrElse("ridreg.step.size", "1").toDouble.toInt
    val regParam           = modelParams.getOrElse("ridreg.reg.param", "1").toDouble
    val miniBatchFraction  = modelParams.getOrElse("ridreg.mini.batch.fraction", "1").toDouble
    val weights            = modelParams.getOrElse("ridreg.initial.weights", "not set")
    
    RRmodel = if(weights == "not set" || weights.equals("")) TrainRidgeRegression.train(LabelRdd.data, maxIterations, step, regParam, miniBatchFraction) 
    else {
      val weg = weights.split(",").map(_.toDouble)
      TrainRidgeRegression.train(LabelRdd.data, maxIterations, step, regParam, miniBatchFraction,Vectors.dense(weg))
    } 
    saveModel
  } 
  
  
  def rebulitModel(modelPath: String):this.type = {
    readParams(modelPath)
    if(!params.isEmpty){
      RRmodel = createModel
      return this
    } else throw new Exception("模型重建失败，原因是该模型参数为空！")
  } 
  
  def createModel(): RidgeRegressionModel = {
    val intercept = params.get("intercept").get.toDouble
    val weigths   = params.get("weights").get.split(",").map(_.toDouble)
    new RidgeRegressionModel(Vectors.dense(weigths), intercept)
  }
  
  def saveModel() {
    if(RRmodel != null){
      val intercept = RRmodel.intercept
      val weights    = RRmodel.weights.toArray.mkString(",")
      params.clear
      params("intercept") = intercept.toString
      params("weights")   = weights
    }    
  }
  

  def predict(data:DataSet):DataSet = {
    if(RRmodel != null) {
      val predictData = data.toVectorData
                            .filterColumnWithCondition(col => col.isInput)
                            .getData
                            
      val preData  = RRmodel.predict(predictData)
      
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

object TrainRidgeRegression {

  /**
   * Train a RidgeRegression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient. The weights used
   * in gradient descent are initialized using the initial weights provided.
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
      initialWeights: Vector): RidgeRegressionModel = {
      RidgeRegressionWithSGD.train(input,numIterations,stepSize, regParam, miniBatchFraction,initialWeights)
  }

  /**
   * Train a RidgeRegression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient.
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
      miniBatchFraction: Double): RidgeRegressionModel = {
      RidgeRegressionWithSGD.train(input,numIterations,stepSize, regParam, miniBatchFraction)
  }

  /**
   * Train a RidgeRegression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. We use the entire data set to
   * compute the true gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param regParam Regularization parameter.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a RidgeRegressionModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double): RidgeRegressionModel = {
      RidgeRegressionWithSGD.train(input,numIterations,stepSize, regParam)
  }

  /**
   * Train a RidgeRegression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using a step size of 1.0. We use the entire data set to
   * compute the true gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a RidgeRegressionModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int): RidgeRegressionModel = {
      RidgeRegressionWithSGD.train(input,numIterations)
  }
}
