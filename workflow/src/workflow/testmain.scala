package workflow

import workflow.built.WorkflowFromXML

object testmain {
  
  def main(args:Array[String]){
    val  workflow = new WorkflowFromXML(args(0)).builter
    workflow.run
    println("END")
  }

}
