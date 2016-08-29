package pl.touk.esp.engine.api

import scala.util.Try

trait ProcessListener {

  def nodeEntered(nodeId: String, context: Context, processMetaData: MetaData, mode: InterpreterMode): Unit

  def expressionEvaluated(expression: String, context: Context, processMetaData: MetaData, result: Any): Unit

  def serviceInvoked(id: String, context: Context, processMetaData: MetaData, result: Try[Any]): Unit

}