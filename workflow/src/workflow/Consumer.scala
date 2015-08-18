package workflow

import workflow.records.Record

trait Consumer {
	/**
	 * 消息传递接口
	 * @param record 从provider传递过来的数据
	 */
	def handleRecord(record : Record)
}
