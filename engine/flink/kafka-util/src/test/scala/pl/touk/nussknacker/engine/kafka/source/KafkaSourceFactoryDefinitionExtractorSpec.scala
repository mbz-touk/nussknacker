package pl.touk.nussknacker.engine.kafka.source

import pl.touk.nussknacker.engine.api.typed.typing.{Typed, TypedClass}
import pl.touk.nussknacker.engine.definition.TypeInfos.{ClazzDefinition, FieldInfo, MethodInfo}
import pl.touk.nussknacker.engine.kafka.KafkaSourceFactoryMixin.{SampleKey, SampleValue}

class KafkaSourceFactoryDefinitionExtractorSpec extends KafkaSourceFactoryProcessMixin {

  test("should extract valid type definitions from source based on GenericNodeTransformation with explicit type definitions") {
    val extractedTypes = extractTypes(processDefinition)

    // Here we are checking explicit type extraction for sources based on GenericNodeTransformation
    // with defined explicit type extraction.
    // It is important that SampleKey and SampleValue are used only by source of that kind,
    // and they must not be returned by other services.
    extractedTypes should contain allOf (
      ClazzDefinition(TypedClass(classOf[SampleKey],Nil),
        Map(
          "partOne" -> FieldInfo(Typed[String], None),
          "partTwo" -> FieldInfo(Typed[Long], None),
        ),
        Map("toString" -> List(MethodInfo(Nil, Typed[String], None, varArgs = false)))
      ),
      ClazzDefinition(TypedClass(classOf[SampleValue],Nil),
        Map(
          "id" -> FieldInfo(Typed[String], None),
          "field" -> FieldInfo(Typed[String], None),
        ),
        Map("toString" -> List(MethodInfo(Nil, Typed[String], None, varArgs = false)))
      )
    )
  }

}