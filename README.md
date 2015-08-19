# spark-workflow

基于spark的workflow模式分布式计算引擎

通过解析xml文件获取计算任务，执行计算并返回结果

工作流实现是用尾递归方法实现， 从计算终节点往前递归实现。

workflow 工作流框架

单个节点交互过程

![actor-desgin](http://7xl71l.com1.z0.glb.clouddn.com/workflow_defign_超脑工作流.jpg)

计算流程图:

![computer-desgin](http://7xl71l.com1.z0.glb.clouddn.com/workflow_defign_工作流训练预测.jpg)

datapreprocessing 数据预处理框架
