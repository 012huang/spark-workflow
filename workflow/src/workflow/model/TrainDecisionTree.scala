package workflow.model

import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.tree.model.InformationGainStats
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Impurity
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.tree.model.Node
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.{PrintWriter,OutputStreamWriter,FileOutputStream,IOException}
import org.apache.spark.mllib.tree.configuration.FeatureType
import org.apache.spark.mllib.tree.model.{Split,InformationGainStats}
import org.apache.spark.mllib.tree.model.Predict
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import workflow.dataset.DataSet
import workflow.dataset.Column
import workflow.mathanalysis.DataManage
import workflow.mathanalysis.util


class TrainDecisionTree(modelParams: HashMap[String,String]) extends DataLearning {
  
  /**
   * 决策树模型
   * */
  private var DTModel  :DecisionTreeModel         = null  
  val params           :HashMap[String,String]    = new HashMap[String,String]() 

  def modelIsReady():Boolean = {
    if(DTModel == null) false else true
  }
  
  
  /**
   *模型训练入口
   **/  
  def runModel(data:DataSet){
   //tempDatadeal
    
    val dataTramform = new DataManage(data.getColumnSet)
    val dataValueSet = dataTramform.CreateCategoricalValue(data.getData)
    data.setCategoricalFeaturesInfo(dataValueSet)
    val dataTemp = data.getData.map(dataTramform.CategoricalValueToClassTranform(_))
    data.setData(dataTemp)

    val LabelRdd = data.toLabeledData
//   lazy val VectorRdd = data.filterColumnWithCondition(col => !col.isVoid).toVectorData
   
  
    val algo :Algo= modelParams.getOrElse("decitree.algo", "Classification") match{
      case "Classification" =>Classification
      case "Regression" =>Regression
      case _ => throw new Exception("算法输入错误 ，应输入Classification或Regression")
    }
    val impurity = modelParams.getOrElse("decitree.impurity", "Gini") match {
      case "Gini" => Gini
      case "Entropy" => Entropy
      case "Variance" => Variance
      case _ => throw new Exception("信息量输入错误，应输入Gini，Entropy或Variance")
    }   
    val maxDepth = modelParams.getOrElse("decitree.max.depth", "10").toDouble.toInt
    val maxBins = modelParams.getOrElse("decitree.max.bins", "100").toDouble.toInt
    val quantileCalculationStrategy = modelParams.getOrElse("decitree.quantile.calculation.strategy", "Sort") match{
      case "Sort" =>Sort
      case "MinMax" =>MinMax
      case "ApproxHist" =>ApproxHist
    }
    val numClassesForClassification= modelParams.getOrElse("decitree.num.classes.for.classification","2").toDouble.toInt

    val classNum = LabelRdd.getColumnSet.getCategoricalIdAndClassesNumWithoutTarget
    
    
    val filtedId = classNum.filter{case(key,value) => value > maxBins || value <2}
    val filterFeature = filtedId.map(_._1).toArray
    val columnName = filtedId.map{case(key,value) =>(LabelRdd.getColumnSet.idToName(key),value)}
    LabelRdd.filterFeature(filterFeature)
    
    val classNumFinally = LabelRdd.getColumnSet.getCategoricalIdAndClassesNumWithoutTarget   
//    logger.paragraph("训练模型数据一共: "+ trainingData.count+" 条")
    
    val strategy = new Strategy(algo, impurity, maxDepth, numClassesForClassification, maxBins, quantileCalculationStrategy,classNumFinally)
//    logger.titlePrint("开始训练模型:",2)
    
    DTModel = TrainDecisionTree.train(LabelRdd.data ,strategy)
//    logger.titlePrint("模型训练完毕:",2)
//    logger.paragraph(toDebugString)
    saveModel
  }
  
  

  
  def predict(data:DataSet):DataSet = {
    if(DTModel != null) {
      val predictData = data.toVectorData
                            .filterColumnWithCondition(col => col.isInput)
                            .getData
                            
      val preData  = DTModel.predict(predictData)
      
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
  
  
  def rebulitModel(modelPath:String):this.type = {
    readParams(modelPath)
    if(!params.isEmpty){
      DTModel = createModel
      return this
    } else throw new Exception("模型重建失败，原因是该模型参数为空！")
  }  
  

    

  def createModel():DecisionTreeModel = {
    val algo = params.get("algo").get match{
      case "Classification" =>Classification
      case "Regression" =>Regression
    }
    val treeNodes = params.get("nodes").get
    val newNodes = rebulitTreeNodes(treeNodes)
    val model = new DecisionTreeModel(newNodes(0),algo) 
    model
    
  }
  
  /**
   * 决策树模型节点字符串转化为 Node结构集合
   * @params treeNodes nodes 字符串表达式
   * return  决策树节点集合 
   * */
  protected def rebulitTreeNodes(treeNodes:String):Array[Node] = {
    val nodes = treeNodes.split("[*]").map{
      line=>
      val treeNode = line.split(' ')
      val treeId = treeNode(0).toInt
      val predict = new Predict(treeNode(1).toDouble,treeNode(2).toDouble)
      val impurity = treeNode(3).toDouble
      val parentsChildId = Array(treeNode(7).toInt,treeNode(5).toInt,treeNode(6).toInt)
      val isleaf = if (treeNode(4)=="true") true else false 
      val sp = treeNode(8).split("[|]")
      val split:Option[Split] = if(sp(0) != "None"){
          val categories = if(sp.length < 4) List[Double]() else sp(3).split(',').map(_.toDouble).toList
          val featureType = sp(2) match{
            case "Continuous" => FeatureType.Continuous
            case "Categorical" => FeatureType.Categorical
          }
          Some(Split(sp(0).toInt,sp(1).toDouble,featureType,categories))
      } else None
      val statsTemp = treeNode(9).split("[|]")
      val informationGainStats:Option[InformationGainStats] = if(statsTemp(0) == "None") None else{
          val stats = statsTemp.map(_.toDouble)
          Some(new InformationGainStats(stats(0),stats(1),stats(2),stats(3),new Predict(stats(4),stats(5)),new Predict(stats(6),stats(7))))
      }
      val node = new Node(treeId,predict,impurity,isleaf, split, None, None, informationGainStats)
      (node,parentsChildId)
    }   
    createTree(nodes)    
  }
  
  /**
   *得到模型参数，用于构造模型
   *模型保存为String类型，split，stats之间用“|”分隔，array元素之间用“ ”分隔
   **/
  def saveModel(){
    if(DTModel != null){
	    val algo = DTModel.algo.toString()
	    val output = saveTree(DTModel.topNode)
	    params.clear
	    params("algo") = algo
	    params("nodes") = output  
    }
  }
  
  /**
   * 把树所有节点保存成成一个字符串
   * return treeString
   * */
  protected def saveTree(topNode :Node):String = {
    val (temp,parantsId,nodesNum) = nodesWithParentsId(topNode,-1)
    val nodes = (parantsId.toArray zip temp)

    nodes.map{
      case(parentsId, node)=>
      def left = if (node.leftNode != None) node.leftNode.get.id else -1
      def right = if (node.rightNode != None) node.rightNode.get.id else -1
      val split = if(node.split == None) {
        "None"
        }else {
          Array(node.split.get.feature,node.split.get.threshold,node.split.get.featureType,node.split.get.categories.toArray.mkString(",")).mkString("|")
        }
      
      val stats = if(node.stats == None) {
        "None"
        }else {
         Array(node.stats.get.gain,node.stats.get.impurity,node.stats.get.leftImpurity,node.stats.get.rightImpurity,node.stats.get.leftPredict.predict,node.stats.get.leftPredict.prob,node.stats.get.rightPredict.predict,node.stats.get.rightPredict.prob).mkString("|")
        }
      val str =Array(node.id,node.predict.predict,node.predict.prob,node.impurity,node.isLeaf,left,right,parentsId,split,stats).mkString(" ")
      str
    }.mkString("*")    
  }
  
  
/**
 * 根据nodes重新构建决策树*/
  protected def createTree(nodes:Array[(Node,Array[Int])]):Array[Node]={
     val (tempNodes,parentsChildId) = (nodes.map(_._1),nodes.map(_._2))
     def localNum(id:Int,tempNode:Array[Node]):Int = {
       var local:Int = 0
       for(i<- 0 until nodes.length)
         if (tempNodes(i).id == id) local = i
       local
     }     
     def builtTree(tempNodes:Array[Node],parentsChildId:Array[Array[Int]],id:Int){
       val i = localNum(id,tempNodes)
       val node = tempNodes(i)
       val children = Array(parentsChildId(i)(1),parentsChildId(i)(2))       
       def left(i:Int) = if(children(0)== -1) None else Some(tempNodes(localNum(children(0),tempNodes)))       
       def right(i:Int) = if(children(1)== -1) None else Some(tempNodes(localNum(children(1),tempNodes)))       
       node.leftNode = left(i)
       node.rightNode = right(i)
       if(node.leftNode!= None) builtTree(tempNodes,parentsChildId,node.leftNode.get.id)
       if(node.rightNode!=None) builtTree(tempNodes,parentsChildId,node.rightNode.get.id) 
     } 
    builtTree(tempNodes,parentsChildId,0)
    tempNodes
  }

  protected def nodesWithParentsId(topNode:Node,id:Int):(ArrayBuffer[Node],ArrayBuffer[Int],Int)={
    val Nodes = new ArrayBuffer[Node]
    val nodesId = new ArrayBuffer[Int]
    var k = 0
    if(topNode!=null){      
      Nodes.insert(k, topNode)
      nodesId.insert(k,id)
      k=k+1
      if(topNode.leftNode!= None) {
        val (addNew,addId,t)= nodesWithParentsId(topNode.leftNode.get,topNode.id)
        Nodes.insertAll(k,addNew)
        nodesId.insertAll(k,addId)
        k=k+t
      }
      if(topNode.rightNode!= None) {
        val (addNew,addId,t) = nodesWithParentsId(topNode.rightNode.get,topNode.id)
        Nodes.insertAll(k,addNew)
        nodesId.insertAll(k,addId)
        k=k+t
      }
    }
    (Nodes,nodesId,k)
  }

  override def saveParams(savePath:String) {
    if(!params.isEmpty && savePath!= null){
      try{
        val out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath),"UTF-8"))
        val algo = params.get("algo").get
        val nodes = params.get("nodes").get
        val nodeStr = nodes.split("[*]")
        out.println("algo" + "=" + algo)

        nodeStr.foreach(node => out.println("node"+"="+node))
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
        val readIn = Source.fromFile(savePath,"UTF-8").getLines
        var algo:String =""
        var nodes = ArrayBuffer[String]()

        for(elem <- readIn) {
          val (key,value) = {
            val temp = elem.split("=")
            (temp(0),temp(1))
          }
          if(key.equals("algo")) algo = value
          if(key.equals("node")) nodes += value
        }
        if(!algo.equals("") && !nodes.equals("")){
          params("algo") = algo
          params("nodes") = nodes.mkString("*")
        }
      }catch{
        case ex:IOException => throw new Exception("the save params path is not right!")
        
      }
    }
  }
  
  def printRule{
    
    
  }
  
  
  

  
}




object TrainDecisionTree {
  /**
   * Method to train a decision tree model.
   * The method supports binary and multiclass classification and regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   *       and [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   *       is recommended to clearly separate classification and regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param strategy The Configuration parameters for the tree algorithm which specify the type
   *                 of algorithm (classification, regression, etc.), feature type (continuous,
   *                 categorical), depth of the tree, quantile calculation strategy, etc.
   * @return DecisionTreeModel that can be used for prediction
  */
  def train(input: RDD[LabeledPoint], strategy: Strategy): DecisionTreeModel = {
    DecisionTree.train(input,strategy)
  }

  /**
   * Method to train a decision tree model.
   * The method supports binary and multiclass classification and regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   *       and [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   *       is recommended to clearly separate classification and regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param algo algorithm, classification or regression
   * @param impurity impurity criterion used for information gain calculation
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @return DecisionTreeModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int): DecisionTreeModel = {
      DecisionTree.train(input,algo,impurity,maxDepth)
  }

  /**
   * Method to train a decision tree model.
   * The method supports binary and multiclass classification and regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   *       and [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   *       is recommended to clearly separate classification and regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param algo algorithm, classification or regression
   * @param impurity impurity criterion used for information gain calculation
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param numClassesForClassification number of classes for classification. Default value of 2.
   * @return DecisionTreeModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClassesForClassification: Int): DecisionTreeModel = {
      DecisionTree.train(input,algo, impurity, maxDepth, numClassesForClassification)
  }

  /**
   * Method to train a decision tree model.
   * The method supports binary and multiclass classification and regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   *       and [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   *       is recommended to clearly separate classification and regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param algo classification or regression
   * @param impurity criterion used for information gain calculation
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param numClassesForClassification number of classes for classification. Default value of 2.
   * @param maxBins maximum number of bins used for splitting features
   * @param quantileCalculationStrategy  algorithm for calculating quantiles
   * @param categoricalFeaturesInfo Map storing arity of categorical features.
   *                                E.g., an entry (n -> k) indicates that feature n is categorical
   *                                with k categories indexed from 0: {0, 1, ..., k-1}.
   * @return DecisionTreeModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClassesForClassification: Int,
      maxBins: Int,
      quantileCalculationStrategy: QuantileStrategy,
      categoricalFeaturesInfo: Map[Int,Int]): DecisionTreeModel = {
      DecisionTree.train(input,algo, impurity, maxDepth, numClassesForClassification, maxBins,
      quantileCalculationStrategy, categoricalFeaturesInfo)
  }
  
    
  

  /**
   * Method to train a decision tree model for binary or multiclass classification.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels should take values {0, 1, ..., numClasses-1}.
   * @param numClassesForClassification number of classes for classification.
   * @param categoricalFeaturesInfo Map storing arity of categorical features.
   *                                E.g., an entry (n -> k) indicates that feature n is categorical
   *                                with k categories indexed from 0: {0, 1, ..., k-1}.
   * @param impurity Criterion used for information gain calculation.
   *                 Supported values: "gini" (recommended) or "entropy".
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   *                  (suggested value: 4)
   * @param maxBins maximum number of bins used for splitting features
   *                 (suggested value: 100)
   * @return DecisionTreeModel that can be used for prediction
   */
  def trainClassifier(
      input: RDD[LabeledPoint],
      numClassesForClassification: Int,
      categoricalFeaturesInfo: Map[Int, Int],
      impurity: String,
      maxDepth: Int,
      maxBins: Int): DecisionTreeModel = {
      DecisionTree.trainClassifier(input, numClassesForClassification, categoricalFeaturesInfo,impurity, maxDepth,maxBins)
  }


  /**
   * Method to train a decision tree model for regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels are real numbers.
   * @param categoricalFeaturesInfo Map storing arity of categorical features.
   *                                E.g., an entry (n -> k) indicates that feature n is categorical
   *                                with k categories indexed from 0: {0, 1, ..., k-1}.
   * @param impurity Criterion used for information gain calculation.
   *                 Supported values: "variance".
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   *                  (suggested value: 4)
   * @param maxBins maximum number of bins used for splitting features
   *                 (suggested value: 100)
   * @return DecisionTreeModel that can be used for prediction
   */
  def trainRegressor(
      input: RDD[LabeledPoint],
      categoricalFeaturesInfo: Map[Int, Int],
      impurity: String,
      maxDepth: Int,
      maxBins: Int): DecisionTreeModel = {
      DecisionTree.trainRegressor(input,categoricalFeaturesInfo,impurity, maxDepth,maxBins)
  }

}
