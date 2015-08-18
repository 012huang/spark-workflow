package workflow.model

import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.loss.LogLoss
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.tree.model.Node
import java.io.PrintWriter
import java.io.OutputStreamWriter
import java.io.FileOutputStream
import java.io.IOException
import scala.io.Source
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.tree.model.Predict
import org.apache.spark.mllib.tree.model.Split
import org.apache.spark.mllib.tree.configuration.FeatureType
import org.apache.spark.mllib.tree.model.InformationGainStats
import workflow.mathanalysis.DataManage
import workflow.dataset.DataSet
import workflow.dataset.Column
import workflow.mathanalysis.util

class TrainGradientBoostedTrees(modelParams: HashMap[String,String]) extends TrainDecisionTree(modelParams){

  
  private var GBTmodel  :GradientBoostedTreesModel = null
  
  override def runModel(data:DataSet){
    
    val dataTramform = new DataManage(data.getColumnSet)
    val dataValueSet = dataTramform.CreateCategoricalValue(data.getData)
    data.setCategoricalFeaturesInfo(dataValueSet)
    val dataTemp = data.getData.map(dataTramform.CategoricalValueToClassTranform(_))
    data.setData(dataTemp)

    val LabelRdd = data.toLabeledData

  


    val algo :Algo = modelParams.getOrElse("decitree.algo", "Classification") match{
      case "Classification" =>Classification
      case "Regression" =>Regression
      case _ => throw new Exception("算法输入错误 ，应输入Classification或Regression")
    }
    val impurity  = modelParams.getOrElse("decitree.impurity", modelParams.toArray) match {
      case "Gini"      => Gini
      case "Entropy"   => Entropy
      case "Variance"  => Variance
      case _ => throw new Exception("信息量输入错误，应输入Gini，Entropy或Variance")
    }   
    val maxDepth     = modelParams.getOrElse("decitree.max.depth", "10").toDouble.toInt
    val maxBins      = modelParams.getOrElse("decitree.max.bins", "100").toDouble.toInt
    val maxIteration = modelParams.getOrElse("gradientboostedtrees.maxiteration", "20").toInt
    val quantileCalculationStrategy = modelParams.getOrElse("decitree.quantile.calculation.strategy", "Sort") match{
      case "Sort"       =>Sort
      case "MinMax"     =>MinMax
      case "ApproxHist" =>ApproxHist
    }
    val numClassesForClassification = modelParams.getOrElse("decitree.num.classes.for.classification", "2").toDouble.toInt

    val classNum = LabelRdd.getColumnSet.getCategoricalIdAndClassesNumWithoutTarget
    LabelRdd.AnalysisContinuousColumn()
    
    
    val filtedId = classNum.filter{case(key,value) => value > maxBins || value <2}
    val filterFeature = filtedId.map(_._1).toArray
    val columnName = filtedId.map{case(key,value) =>(LabelRdd.getColumnSet.idToName(key),value)}
    LabelRdd.FilterOutlierValueInCategorical()
//    columnName.foreach{case(key,value) => logger.paragraph("被过滤的字段名: "+ key +"  类别数: " + value)}
    LabelRdd.filterFeature(filterFeature)
    val classNumFinally = LabelRdd.getColumnSet.getCategoricalIdAndClassesNumWithoutTarget   
//    logger.paragraph("训练模型数据一共: "+ trainingData.count+" 条")
    val strategy = new Strategy(algo, impurity, maxDepth, numClassesForClassification, maxBins, quantileCalculationStrategy,classNumFinally)
    val BoostedStrategy = new BoostingStrategy(strategy, LogLoss, maxIteration)
//    logger.titlePrint("开始训练模型:",2)
    GBTmodel = TrainGradientBoostedTrees.train(LabelRdd.data ,BoostedStrategy)
//    logger.titlePrint("模型训练完毕:",2)
//    logger.paragraph(toDebugString)
    saveGradientBoostedTreesModel
   
  }

  
  override def predict(data:DataSet):DataSet = {
    if(GBTmodel != null) {
      val predictData = data.toVectorData
                            .filterColumnWithCondition(col => col.isInput)
                            .getData
                            
      val preData  = GBTmodel.predict(predictData)
      
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

  
  override def rebulitModel(modelPath: String):this.type = {
    readParams(modelPath)
    if(!params.isEmpty){
      GBTmodel = createGradientBoostedTreesModel
      return this
    } else {
      throw new Exception("模型重建失败，原因是该模型参数为空！")
    }
    
  }
  
  
  
  def saveGradientBoostedTreesModel{
    if(GBTmodel != null) {
      val trees = GBTmodel.trees
      val weight = GBTmodel.treeWeights
      val algo = GBTmodel.algo
      val numTrees = trees.length
      val treesName = for(i<- (1 to numTrees).toArray) yield s"tree_${i}_Node"
      params.clear
      params("treeNames") = treesName.mkString(",")
      params("algo") = algo.toString()
      params("weight") = weight.mkString(",")
      for (i <- 0 until numTrees){ 
    	  params(treesName(i)) = saveTree(trees(i).topNode)
      }      
    }    
  }
  
  
  

  def createGradientBoostedTreesModel:GradientBoostedTreesModel = {
    val algo = params.get("algo").get match{
      case "Classification" =>Classification
      case "Regression" =>Regression
    }
    val weight = params.get("weight").get.split(",").map(_.toDouble)
    val treesName = params.get("treeNames").get.split(",")
    val decisionTreeModel = for(name <- treesName) yield createDecisionTreeModel(algo,name)
    new GradientBoostedTreesModel(algo, decisionTreeModel, weight)
  }
  
  def createDecisionTreeModel(algo:Algo,treeName:String):DecisionTreeModel = {
    val treeNodes = params.get(treeName).get
    val newNodes = rebulitTreeNodes(treeNodes)
    new DecisionTreeModel(newNodes(0),algo)  
  }
  
  
  
  override def saveParams(savePath:String) {
    if(!params.isEmpty && savePath!= null){
      try{
        val out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath),"UTF-8"))
        val treeNames = params.get("treeNames").get
        val algo = params.get("algo").get
        val weight = params.get("weight").get
        
        out.println("treeNames" + "=" + treeNames)
        out.println("algo" + "=" + algo)
        out.println("weight" + "=" + weight)
//        logger.paragraph(s"正在保存模型参数到${savePath}:")
        for(treeName <- treeNames.split(",")){
          val nodes = params.get(treeName).get
          val nodeStr = nodes.split("[*]")
          nodeStr.foreach(node => out.println(treeName + "=" + node))
        }        
        out.close()
      }catch{
        case ex:IOException => throw new Exception("the save params path is not right!")
      }      
    }
  }
  
  override def readParams(savePath:String) {
    if(savePath != null){
      params.clear
      try{
        val readIn = Source.fromFile(savePath,"UTF-8").getLines.toArray
        var algo:String = "" 
        var weight:String = ""
        val treeName = readIn(0).split("=")
        params("treeNames")  = treeName(1)
        val treeNames = treeName(1).split(",")
        val nodes = new Array[ArrayBuffer[String]](treeNames.length)
        for(elem <- readIn) {
          val (key,value) = {
            val temp = elem.split("=")
            (temp(0),temp(1))
          }
          
          key match {
            case "algo" => algo = value
            case "weight" => weight = value
            case _ => val index = treeNames.indexOf(key)
                      if(index != -1)
                         if(nodes(index) == null) nodes(index) = new ArrayBuffer[String]()
                         else nodes(index) += value
          } 
        }
        if(!algo.equals("") && !weight.equals("")){
          params("algo") = algo
          params("weight") = weight
        }
        if(!nodes.isEmpty){
          for(i<- 0 until treeNames.length) {
            if(!nodes.isEmpty) {
              params(treeNames(i)) = nodes(i).mkString("*")
            }
          }
        }
      }catch{
        case ex:IOException => throw new Exception("模型路径不正确")
        
      }
    }
  }



}


object TrainGradientBoostedTrees{
  
  /**
   * Method to train a gradient boosting model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param boostingStrategy Configuration options for the boosting algorithm.
   * @return a gradient boosted trees model that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): GradientBoostedTreesModel = {
      GradientBoostedTrees.train(input, boostingStrategy)
  }
  
  
}  
  
  
