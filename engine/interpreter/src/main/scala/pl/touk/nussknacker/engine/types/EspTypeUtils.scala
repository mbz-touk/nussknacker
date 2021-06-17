package pl.touk.nussknacker.engine.types

import java.lang.reflect._
import java.util.Optional
import cats.data.StateT
import cats.effect.IO
import org.apache.commons.lang3.{ClassUtils, StringUtils}
import pl.touk.nussknacker.engine.api.process.PropertyFromGetterExtractionStrategy.{AddPropertyNextToGetter, DoNothing, ReplaceGetterWithProperty}
import pl.touk.nussknacker.engine.api.process.{ClassExtractionSettings, VisibleMembersPredicate}
import pl.touk.nussknacker.engine.api.typed.typing.{Typed, TypingResult, Unknown}
import pl.touk.nussknacker.engine.api.{Documentation, ParamName}
import pl.touk.nussknacker.engine.definition.TypeInfos.{ClazzDefinition, FieldInfo, MethodInfo, Parameter}
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl

object EspTypeUtils {

  import pl.touk.nussknacker.engine.util.Implicits._

  def clazzDefinition(clazz: Class[_])
                     (implicit settings: ClassExtractionSettings): ClazzDefinition = {
    val membersPredicate = settings.visibleMembersPredicate(clazz)
    val methods = extractPublicMethods(clazz, membersPredicate)

    val fieldsFromMethods = extractDeclaredFieldsFromMethods(clazz, methods, membersPredicate)
    val clazzFields = extractPublicFields(clazz, membersPredicate)
    val fields = clazzFields ++ fieldsFromMethods

    val filteredMethods = methods.filter(m => !fields.contains(m._1))

    ClazzDefinition(Typed(clazz), fields, filteredMethods)
  }

  // We try to extract declared fields from methods
  private def extractDeclaredFieldsFromMethods(clazz: Class[_], methods: Map[String, List[MethodInfo]], membersPredicate: VisibleMembersPredicate): Map[String, FieldInfo] = {
    val declaredFields = getDeclaredFields(clazz, membersPredicate).map(_.getName)

    methods.filter(m => declaredFields.contains(m._1)).map{case (name, methods) => {
      val fieldMethod = methods.filter(_.parameters.isEmpty).head
      (name , FieldInfo(fieldMethod.refClazz, fieldMethod.description))
    }}
  }

  private def extractPublicMethods(clazz: Class[_], membersPredicate: VisibleMembersPredicate)
                                  (implicit settings: ClassExtractionSettings): Map[String, List[MethodInfo]] = {
    /* From getMethods javadoc: If this {@code Class} object represents an interface then the returned array
           does not contain any implicitly declared methods from {@code Object}.
           The same for primitives - we assume that languages like SpEL will be able to do boxing
           It could be significant only for toString, as we filter out other Object methods, but to be consistent...
         */
    val additionalMethods = if (clazz.isInterface) {
      classOf[Object].getMethods.toList
    } else if (clazz.isPrimitive) {
      ClassUtils.primitiveToWrapper(clazz).getMethods.toList
    } else {
      List.empty
    }
    val publicMethods = clazz.getMethods.toList ++ additionalMethods

    val filteredMethods = publicMethods.filter(membersPredicate.shouldBeVisible)

    val methodNameAndInfoList = filteredMethods.flatMap { method =>
      val extractedMethod = extractMethod(method)
      collectMethodNames(method).map(_ -> extractedMethod)
    }

    deduplicateMethodsWithGenericReturnType(methodNameAndInfoList)
  }

  /*
    This is tricky case. If we have generic class and concrete subclasses, e.g.
    - ChronoLocalDateTime<D extends ChronoLocalDate>
    - LocalDateTime extends ChronoLocalDateTime<LocalDate>
    and method with generic return type from superclass: D toLocalDate()
    getMethods return two toLocalDate methods:
      ChronoLocalDate toLocalDate()
      LocalDate toLocalDate()
    In our case the second one is correct
   */
  private def deduplicateMethodsWithGenericReturnType(methodNameAndInfoList: List[(String, MethodInfo)]) = {
    val groupedByNameAndParameters = methodNameAndInfoList.groupBy(mi => (mi._1, mi._2.parameters))
    groupedByNameAndParameters.toList.map {
      case (_, methodsForParams) =>
        /*
          we want to find "most specific" class, however suprisingly it's not always possible, because we treat e.g. isLeft and left methods
          as equal (for javabean-like access) and e.g. in scala Either this is perfectly possible. In case we cannot find most specific
          class we pick arbitrary one (we sort to avoid randomness)
         */

        methodsForParams.find { case (_, MethodInfo(_, ret, _, _)) =>
          methodsForParams.forall(mi => ret.canBeSubclassOf(mi._2.refClazz))
        }.getOrElse(methodsForParams.minBy(_._2.refClazz.display))
    }.toGroupedMap
  }

  // SpEL is able to access getters using property name so you can write `obj.foo` instead of `obj.getFoo`
  private def collectMethodNames(method: Method)
                                (implicit settings: ClassExtractionSettings): List[String] = {
    val isGetter = method.getName.matches("^(get|is).+") && method.getParameterCount == 0
    if (isGetter) {
      val propertyMethod = StringUtils.uncapitalize(method.getName.replaceAll("^get|^is", ""))
      settings.propertyExtractionStrategy match {
        case AddPropertyNextToGetter    => List(method.getName, propertyMethod)
        case ReplaceGetterWithProperty  => List(propertyMethod)
        case DoNothing                  => List(method.getName)
      }
    } else {
      List(method.getName)
    }
  }

  private def extractMethod(method: Method) =
    MethodInfo(extractParameters(method), extractMethodReturnType(method), extractNussknackerDocs(method), method.isVarArgs)

  private def extractPublicFields(clazz: Class[_], membersPredicate: VisibleMembersPredicate)
                                 (implicit settings: ClassExtractionSettings): Map[String, FieldInfo] = {
    val interestingFields = clazz.getFields.filter(membersPredicate.shouldBeVisible)
    interestingFields.map { field =>
      field.getName -> FieldInfo(extractFieldReturnType(field), extractNussknackerDocs(field))
    }.toMap
  }

  private def getDeclaredFields(clazz: Class[_], membersPredicate: VisibleMembersPredicate): List[Field] = {
    if (clazz == classOf[Object] || clazz == null)
      List.empty
    else {
      getDeclaredFields(clazz.getSuperclass, membersPredicate) ++ clazz.getDeclaredFields.filter(membersPredicate.shouldBeVisible).toList
    }
  }

  private def extractNussknackerDocs(accessibleObject: AccessibleObject) = {
    Option(accessibleObject.getAnnotation(classOf[Documentation])).map(_.description())
  }

  private def extractParameters(method: Method): List[Parameter] = {
    for {
      param <- method.getParameters.toList
      annotationOption = Option(param.getAnnotation(classOf[ParamName]))
      name = annotationOption.map(_.value).getOrElse(param.getName)
      paramType = extractParameterType(param)
    } yield Parameter(name, paramType)
  }

  def extractParameterType(javaParam: java.lang.reflect.Parameter): TypingResult = {
    extractClass(javaParam.getParameterizedType).getOrElse(Typed(javaParam.getType))
  }

  private def extractFieldReturnType(field: Field): TypingResult = {
    extractGenericReturnType(field.getGenericType).orElse(extractClass(field.getGenericType)).getOrElse(Typed(field.getType))
  }

  def extractMethodReturnType(method: Method): TypingResult = {
    extractGenericReturnType(method.getGenericReturnType).orElse(extractClass(method.getGenericReturnType)).getOrElse(Typed(method.getReturnType))
  }

  private def extractGenericReturnType(typ: Type): Option[TypingResult] = {
    typ match {
      case t: ParameterizedTypeImpl => extractGenericMonadReturnType(t)
      case t => None
    }
  }

  // This method should be used only for method's and field's return type - for method's parameters such unwrapping has no sense
  private def extractGenericMonadReturnType(genericReturnType: ParameterizedTypeImpl): Option[TypingResult] = {
    val rawType = genericReturnType.getRawType

    // see ScalaLazyPropertyAccessor
    if (classOf[StateT[IO, _, _]].isAssignableFrom(rawType)) {
      val returnType = genericReturnType.getActualTypeArguments.apply(3) // it's IndexedStateT[IO, ContextWithLazyValuesProvider, ContextWithLazyValuesProvider, A]
      extractClass(returnType)
    }
    // see ScalaOptionOrNullPropertyAccessor
    else if (classOf[Option[_]].isAssignableFrom(rawType)) {
      val optionGenericType = genericReturnType.getActualTypeArguments.apply(0)
      extractClass(optionGenericType)
    }
    // see JavaOptionalOrNullPropertyAccessor
    else if (classOf[Optional[_]].isAssignableFrom(rawType)) {
      val optionalGenericType = genericReturnType.getActualTypeArguments.apply(0)
      extractClass(optionalGenericType)
    }
    else None
  }

  //TODO this is not correct for primitives and complicated hierarchies, but should work in most cases
  //http://docs.oracle.com/javase/8/docs/api/java/lang/reflect/ParameterizedType.html#getActualTypeArguments--
  private def extractClass(typ: Type): Option[TypingResult] = {
    typ match {
      case t: Class[_] => Some(Typed(t))
      case t: ParameterizedTypeImpl => Some(extractGenericParams(t))
      case t => None
    }
  }

  private def extractGenericParams(paramsType: ParameterizedTypeImpl): TypingResult = {
    val rawType = paramsType.getRawType
    Typed.genericTypeClass(rawType, paramsType.getActualTypeArguments.toList.map(p => extractClass(p).getOrElse(Unknown)))
  }

  def companionObject[T](klazz: Class[T]): T = {
    klazz.getField("MODULE$").get(null).asInstanceOf[T]
  }

}
