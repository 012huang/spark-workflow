package workflow.model

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LassoWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.regression.LassoModel
import scala.collection.mutable.HashMap
import workflow.dataset.DataSet
import workflow.dataset.Column
import workflow.mathanalysis.util

class TrainLasso(modelParams: HashMap[String,String]) extends DataLearning {

  
  private var Lmodel    :LassoModel             = null  
  val params            :HashMap[String,String] = new HashMap[String,String]() 
  
  /**get Lasso model*/
  
  def runModel(data:DataSet){
   //tempDatadeal
    

    val LabelRdd = data.toLabeledData

  
    
    
    
    val maxIterations = modelParams.getOrElse("lasso.max.iteration", "10").toDouble.toInt
    val step = modelParams.getOrElse("lasso.step.size", "1").toDouble.toInt
    val regParam = modelParams.getOrElse("lasso.reg.param", "1").toDouble
    val miniBatchFraction = modelParams.getOrElse("lasso.mini.batch.fraction", "1").toDouble
    val weights = modelParams.getOrElse("lasso.initial.weights", "")
    
    Lmodel = if(weights == "not set" || weights.equals("")) TrainLasso.train(LabelRdd.data, maxIterations, step, regParam, miniBatchFraction) 
    else {
      val weg = weights.split(",").map(_.toDouble)
      TrainLasso.train(LabelRdd.data, maxIterations, step, regParam, miniBatchFraction,Vectors.dense(weg))
    }
    saveModel
  }
  
  def rebulitModel(modelPath: String):this.type = {
    readParams(modelPath)
    if(!params.isEmpty){
      Lmodel = createModel
      return this
    } else throw new Exception("模型重建失败，原因是该模型参数为空！")
  } 
  
  def createModel(): LassoModel = {
    val intercept = params.get("intercept").get.toDouble
    val weigths   = params.get("weights").get.split(",").map(_.toDouble)
    new LassoModel(Vectors.dense(weigths), intercept)
  }
  
  def saveModel() {
    if(Lmodel != null){
      val intercept = Lmodel.intercept
      val weights    = Lmodel.weights.toArray.mkString(",")
      params.clear
      params("intercept") = intercept.toString
      params("weights")   = weights
    }    
  }
  

  def predict(data:DataSet):DataSet = {
    if(Lmodel != null) {
      val predictData = data.toVectorData
                            .filterColumnWithCondition(col => col.isInput)
                            .getData
                            
      val preData  = Lmodel.predict(predictData)
      
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



object TrainLasso {
  /**
   * Train a Lasso model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient. The weights used
   * in gradient descent are initialized using the initial weights provided.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size scaling to be used for the iterations of gradient descent.
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
      initialWeights: Vector): LassoModel = {
      LassoWithSGD.train(input, numIterations,stepSize,regParam, miniBatchFraction,initialWeights)
      
  }

  /**
   * Train a Lasso model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
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
      miniBatchFraction: Double): LassoModel = {
      LassoWithSGD.train(input, numIterations,stepSize,regParam, miniBatchFraction)
  }

  /**
   * Train a Lasso model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. We use the entire data set to
   * update the true gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param regParam Regularization parameter.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a LassoModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double): LassoModel = {
      LassoWithSGD.train(input, numIterations,stepSize,regParam)
  }

  /**
   * Train a Lasso model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using a step size of 1.0. We use the entire data set to
   * compute the true gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a LassoModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int): LassoModel = {
      LassoWithSGD.train(input, numIterations)
  }
}
