package workflow.built

import workflow.Workflow
import java.io.File

trait WorkflowBuilter {
  
  /**
   * 生成工作流
   * */
  
  def builter():Workflow

}
