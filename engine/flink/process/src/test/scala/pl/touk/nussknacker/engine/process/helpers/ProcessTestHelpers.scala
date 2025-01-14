package pl.touk.nussknacker.engine.process.helpers

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.scala._
import org.scalatest.Suite
import pl.touk.nussknacker.engine.api._
import pl.touk.nussknacker.engine.api.deployment.DeploymentData
import pl.touk.nussknacker.engine.api.dict.DictInstance
import pl.touk.nussknacker.engine.api.dict.embedded.EmbeddedDictDefinition
import pl.touk.nussknacker.engine.api.exception.ExceptionHandlerFactory
import pl.touk.nussknacker.engine.api.process.{ProcessObjectDependencies, _}
import pl.touk.nussknacker.engine.api.signal.ProcessSignalSender
import pl.touk.nussknacker.engine.api.typed.TypedMap
import pl.touk.nussknacker.engine.flink.api.process._
import pl.touk.nussknacker.engine.flink.test.FlinkSpec
import pl.touk.nussknacker.engine.graph.EspProcess
import pl.touk.nussknacker.engine.process.compiler.FlinkProcessCompiler
import pl.touk.nussknacker.engine.process.helpers.SampleNodes._
import pl.touk.nussknacker.engine.process.registrar.FlinkProcessRegistrar
import pl.touk.nussknacker.engine.process.{ExecutionConfigPreparer, SimpleJavaEnum, registrar}
import pl.touk.nussknacker.engine.testing.LocalModelData

trait ProcessTestHelpers extends FlinkSpec { self: Suite =>

  object processInvoker {

    def invokeWithSampleData(process: EspProcess,
                             data: List[SimpleRecord],
                             processVersion: ProcessVersion = ProcessVersion.empty,
                             parallelism: Int = 1): Unit = {
      val config = ConfigFactory.load()
      val creator: ProcessConfigCreator = ProcessTestHelpers.prepareCreator(data, config)

      val env = flinkMiniCluster.createExecutionEnvironment()
      val modelData = LocalModelData(config, creator)
      FlinkProcessRegistrar(new FlinkProcessCompiler(modelData), ExecutionConfigPreparer.unOptimizedChain(modelData))
        .register(new StreamExecutionEnvironment(env), process, processVersion, DeploymentData.empty)

      MockService.clear()
      SinkForStrings.clear()
      SinkForInts.clear()
      env.executeAndWaitForFinished(process.id)()
    }

    def invoke(process: EspProcess,
               creator: ProcessConfigCreator,
               config: Config,
               processVersion: ProcessVersion,
               parallelism: Int, actionToInvokeWithJobRunning: => Unit): Unit = {
      val env = flinkMiniCluster.createExecutionEnvironment()
      val modelData = LocalModelData(config, creator)
      registrar.FlinkProcessRegistrar(new FlinkProcessCompiler(modelData), ExecutionConfigPreparer.unOptimizedChain(modelData))
        .register(new StreamExecutionEnvironment(env), process, processVersion, DeploymentData.empty)

      MockService.clear()
      env.withJobRunning(process.id)(actionToInvokeWithJobRunning)
    }
  }

}

object ProcessTestHelpers {

  def prepareCreator(data: List[SimpleRecord], config: Config): ProcessConfigCreator = new ProcessConfigCreator {

    override def services(processObjectDependencies: ProcessObjectDependencies): Map[String, WithCategories[Service]] = Map(
      "logService" -> WithCategories(new MockService),
      "lifecycleService" -> WithCategories(LifecycleService),
      "eagerLifecycleService" -> WithCategories(EagerLifecycleService),
      "enricherWithOpenService" -> WithCategories(new EnricherWithOpenService),
      "serviceAcceptingOptionalValue" -> WithCategories(ServiceAcceptingScalaOption)
    )

    override def sourceFactories(processObjectDependencies: ProcessObjectDependencies): Map[String, WithCategories[FlinkSourceFactory[_]]] = Map(
      "input" -> WithCategories(SampleNodes.simpleRecordSource(data)),
      "intInputWithParam" -> WithCategories(new IntParamSourceFactory(new ExecutionConfig)),
      "genericParametersSource" -> WithCategories(GenericParametersSource),
      "genericSourceWithCustomVariables" -> WithCategories(GenericSourceWithCustomVariables)
    )

    override def sinkFactories(processObjectDependencies: ProcessObjectDependencies): Map[String, WithCategories[SinkFactory]] = Map(
      "monitor" -> WithCategories(SinkFactory.noParam(MonitorEmptySink)),
      "sinkForInts" -> WithCategories(SinkFactory.noParam(SinkForInts)),
      "sinkForStrings" -> WithCategories(SinkFactory.noParam(SinkForStrings)),
      "lazyParameterSink"-> WithCategories(LazyParameterSinkFactory),
      "eagerOptionalParameterSink"-> WithCategories(EagerOptionalParameterSinkFactory),
      "genericParametersSink" -> WithCategories(GenericParametersSink)
    )

    override def customStreamTransformers(processObjectDependencies: ProcessObjectDependencies): Map[String, WithCategories[CustomStreamTransformer]] = Map(
      "stateCustom" -> WithCategories(StateCustomNode),
      "customFilter" -> WithCategories(CustomFilter),
      "customFilterContextTransformation" -> WithCategories(CustomFilterContextTransformation),
      "customContextClear" -> WithCategories(CustomContextClear),
      "sampleJoin" -> WithCategories(CustomJoin),
      "joinBranchExpression" -> WithCategories(CustomJoinUsingBranchExpressions),
      "transformWithNullable" -> WithCategories(TransformerWithNullableParam),
      "optionalEndingCustom" -> WithCategories(OptionalEndingCustom),
      "genericParametersNode" -> WithCategories(GenericParametersNode),
      "nodePassingStateToImplementation" -> WithCategories(NodePassingStateToImplementation)
    )

    override def listeners(processObjectDependencies: ProcessObjectDependencies) = List()

    override def exceptionHandlerFactory(processObjectDependencies: ProcessObjectDependencies): ExceptionHandlerFactory =
      ExceptionHandlerFactory.noParams(_ => RecordingExceptionHandler)


    override def expressionConfig(processObjectDependencies: ProcessObjectDependencies): ExpressionConfig = {
      val dictId = EmbeddedDictDefinition.enumDictId(classOf[SimpleJavaEnum])
      val dictDef = EmbeddedDictDefinition.forJavaEnum(classOf[SimpleJavaEnum])
      val globalProcessVariables = Map(
        "processHelper" -> WithCategories(ProcessHelper),
        "enum" -> WithCategories(DictInstance(dictId, dictDef)),
        "typedMap" -> WithCategories(TypedMap(Map("aField" -> "123"))))
      ExpressionConfig(globalProcessVariables, List.empty, dictionaries = Map(dictId -> WithCategories(dictDef)))
    }

    override def signals(processObjectDependencies: ProcessObjectDependencies): Map[String, WithCategories[ProcessSignalSender]] = Map.empty

    override def buildInfo(): Map[String, String] = Map.empty
  }

}


