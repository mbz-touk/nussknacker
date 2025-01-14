package pl.touk.nussknacker.ui.api

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model._
import akka.http.scaladsl.server._
import akka.stream.Materializer
import cats.data.Validated.{Invalid, Valid}
import cats.instances.future._
import cats.data.Validated
import cats.syntax.either._
import pl.touk.nussknacker.engine.api.deployment.{ProcessManager, ProcessState}
import pl.touk.nussknacker.ui.api.ProcessesResources.{UnmarshallError, WrongProcessId}
import pl.touk.nussknacker.restmodel.displayedgraph.{DisplayableProcess, ProcessStatus, ValidatedDisplayableProcess}
import pl.touk.nussknacker.ui.process.marshall.ProcessConverter
import pl.touk.nussknacker.ui.process.repository.FetchingProcessRepository
import pl.touk.nussknacker.ui.util._
import pl.touk.nussknacker.ui._
import EspErrorToHttp._
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import pl.touk.nussknacker.engine.ProcessingTypeData
import pl.touk.nussknacker.ui.validation.ProcessValidation
import pl.touk.nussknacker.engine.ProcessingTypeData.ProcessingType
import pl.touk.nussknacker.ui.process._
import pl.touk.nussknacker.engine.api.process.ProcessName
import pl.touk.nussknacker.engine.canonicalgraph.CanonicalProcess
import pl.touk.nussknacker.engine.marshall.ProcessMarshaller
import pl.touk.nussknacker.ui.listener.ProcessChangeEvent._
import pl.touk.nussknacker.restmodel.process.{ProcessId, ProcessIdWithName}
import pl.touk.nussknacker.restmodel.processdetails.{BaseProcessDetails, BasicProcess, ProcessShapeFetchStrategy, ValidatedProcessDetails}
import pl.touk.nussknacker.restmodel.validation.ValidationResults.ValidationResult
import pl.touk.nussknacker.ui.security.api.{LoggedUser, Permission}
import pl.touk.nussknacker.ui.uiresolving.UIProcessResolving
import pl.touk.nussknacker.restmodel.process._

import scala.concurrent.{ExecutionContext, Future}
import pl.touk.nussknacker.engine.util.Implicits._
import pl.touk.nussknacker.ui.EspError.XError
import pl.touk.nussknacker.ui.listener.{ProcessChangeEvent, ProcessChangeListener}
import pl.touk.nussknacker.ui.listener.ProcessChangeEvent.OnCategoryChanged
import pl.touk.nussknacker.ui.process.ProcessService.{CreateProcessCommand, UpdateProcessCommand}
import pl.touk.nussknacker.ui.process.processingtypedata.ProcessingTypeDataProvider
import pl.touk.nussknacker.ui.service.{ProcessToolbarService, ProcessToolbarSettings}

//TODO: Move remained business logic to processService
class ProcessesResources(
  val processRepository: FetchingProcessRepository[Future],
  processService: ProcessService,
  processToolbarService: ProcessToolbarService,
  processValidation: ProcessValidation,
  processResolving: UIProcessResolving,
  val processAuthorizer:AuthorizeProcess,
  processChangeListener: ProcessChangeListener,
  typeToConfig: ProcessingTypeDataProvider[ProcessingTypeData]
)(implicit val ec: ExecutionContext, mat: Materializer)
  extends Directives
    with FailFastCirceSupport
    with EspPathMatchers
    with RouteWithUser
    with LazyLogging
    with AuthorizeProcessDirectives
    with ProcessDirectives {

  import akka.http.scaladsl.unmarshalling.Unmarshaller._

  def securedRoute(implicit user: LoggedUser): Route = {
      encodeResponse {
        path("archive") {
          get {
            complete {
              processRepository.fetchArchivedProcesses[Unit]().toBasicProcess
            }
          }
        } ~ path("unarchive" / Segment) { processName =>
          (post & processId(processName)) { processId =>
            canWrite(processId) {
              complete {
                processService.unArchiveProcess(processId)
                  .map(toResponse(StatusCodes.OK))
                  .withSideEffect(_ => processChangeListener.handle(OnUnarchived(processId.id)))
              }
            }
          }
        } ~ path("archive" / Segment) { processName =>
          (post & processId(processName)) { processId =>
            canWrite(processId) {
              complete {
                processService.archiveProcess(processId)
                  .map(toResponse(StatusCodes.OK))
                  .withSideEffect(_ => processChangeListener.handle(OnArchived(processId.id)))
              }
            }
          }
        }  ~ path("processes") {
          get {
            parameters(
              'isSubprocess.as[Boolean].?,
              'isArchived.as[Boolean].?,
              'isDeployed.as[Boolean].?,
              'categories.as(CsvSeq[String]).?,
              'processingTypes.as(CsvSeq[String]).?
            ) { (isSubprocess, isArchived, isDeployed, categories, processingTypes) =>
              complete {
                processRepository.fetchProcesses[Unit](
                  isSubprocess,
                  isArchived.orElse(Option(false)), //Back compatibility
                  isDeployed,
                  categories,
                  processingTypes
                ).map(_.map(enrichDetailsWithProcessState[Unit])).toBasicProcess //TODO: Remove enrichProcess when we will support cache for state
              }
            }
          }
        } ~ path("customProcesses") {
          get {
            complete {
              processRepository.fetchCustomProcesses[Unit]().toBasicProcess
            }
          }
        } ~ path("processesDetails") {
          get {
            parameter('names.as(CsvSeq[String])) { namesToFetch =>
              complete {
                validateAndReverseResolveAll(processRepository.fetchProcessesDetails(namesToFetch.map(ProcessName(_)).toList))
              }
            } ~
            complete {
              validateAndReverseResolveAll(processRepository.fetchProcessesDetails())
            }
          }
        } ~ path("processesComponents" / Segment) { componentId =>
          get {
            complete {
              processRepository.fetchAllProcessesDetails[DisplayableProcess]().map { processList =>
                ProcessObjectsFinder.findComponents(processList, componentId)
              }
            }
          }
        } ~ path("subProcesses") {
          get {
            complete {
              processRepository.fetchSubProcessesDetails[Unit]().toBasicProcess
            }
          }
        } ~ path("subProcessesDetails") {
          get {
            complete {
              validateAndReverseResolveAll(processRepository.fetchSubProcessesDetails[CanonicalProcess]())
            }
          }
        } ~ path("processes" / "status") {
          get {
            complete {
              for {
                processes <- processRepository.fetchProcesses[Unit]()
                customProcesses <- processRepository.fetchCustomProcesses[Unit]()
                statuses <- fetchProcessStatesForProcesses(processes ++ customProcesses)
              } yield statuses
            }
          }
        } ~ path("processes" / "import" / Segment) { processName =>
          processId(processName) { processId =>
            (canWrite(processId) & post) {
              fileUpload("process") { case (_, byteSource) =>
                complete {
                  MultipartUtils.readFile(byteSource).map[ToResponseMarshallable] { json =>
                    validateJsonForImport(processId, json) match {
                      case Valid(process) => importProcess(processId, process)
                      case Invalid(error) => EspErrorToHttp.espErrorToHttp(error)
                    }
                  }
                }
              }
            }
          }
        } ~ path("processes" / Segment / "deployments") { processName =>
          processId(processName) { processId =>
            complete {
              //FIXME: We should provide Deployment definition and return there all deployments, not actions..
              processRepository.fetchProcessActions(processId.id)
            }
          }
        } ~ path("processes" / Segment) { processName =>
          processId(processName) { processId =>
            (delete & canWrite(processId)) {
              complete {
                processService
                  .deleteProcess(processId)
                  .map(toResponse(StatusCodes.OK))
                  .withSideEffect(_ => processChangeListener.handle(OnDeleted(processId.id)))
              }
            } ~ (put & canWrite(processId)) {
              entity(as[UpdateProcessCommand]) { updateCommand =>
                complete {
                  processService
                    .updateProcess(processId, updateCommand)
                    .withSideEffect(response => sideEffectAction(response.toOption.flatMap(_.processResponse)) { resp =>
                      OnSaved(resp.id, resp.versionId)
                    })
                    .map(_.map(_.validationResult))
                    .map(toResponseEither[ValidationResult])
                }
              }
            } ~ parameter('businessView ? false) { businessView =>
              get {
                complete {
                  processRepository.fetchLatestProcessDetailsForProcessId[CanonicalProcess](processId.id, businessView).map[ToResponseMarshallable] {
                    case Some(process) => validateAndReverseResolve(enrichDetailsWithProcessState(process), businessView) // todo: we should really clearly separate backend objects from ones returned to the front
                    case None => HttpResponse(status = StatusCodes.NotFound, entity = "Process not found")
                  }
                }
              }
            }
          }
        } ~ path("processes" / Segment / "rename" / Segment) { (processName, newName) =>
          (put & processId(processName)) { processId =>
            canWrite(processId) {
              complete {
                processService
                  .renameProcess(processId, newName)
                  .withSideEffect(response => sideEffectAction(response) { resp =>
                    OnRenamed(processId.id, resp.oldName, resp.newName)
                  })
                  .map(toResponseEither[UpdateProcessNameResponse])
              }
            }
          }
        } ~ path("processes" / Segment / LongNumber) { (processName, versionId) =>
          (get & processId(processName)) { processId =>
            parameter('businessView ? false) { businessView =>
              complete {
                processRepository.fetchProcessDetailsForId[CanonicalProcess](processId.id, versionId, businessView).map[ToResponseMarshallable] {
                  case Some(process) => validateAndReverseResolve(process, businessView) // todo: we should really clearly separate backend objects from ones returned to the front
                  case None => HttpResponse(status = StatusCodes.NotFound, entity = "Process not found")
                }
              }
            }
          }
        } ~ path("processes" / Segment / Segment) { (processName, category) =>
          authorize(user.can(category, Permission.Write)) {
            parameter('isSubprocess ? false) { isSubprocess =>
              post {
                complete {
                  processService
                    .createProcess(CreateProcessCommand(ProcessName(processName), category, isSubprocess))
                    .withSideEffect(response => sideEffectAction(response) { process =>
                      OnSaved(process.id, process.versionId)
                    })
                    .map(toResponseEither[ProcessResponse](_, StatusCodes.Created))
                }
              }
            }
          }
        } ~ path("processes" / Segment / "status") { processName =>
          (get & processId(processName)) { processId =>
            complete {
              processService.getProcessState(processId).map(ToResponseMarshallable(_))
            }
          }
        } ~ path("processes" / Segment / "toolbars") { processName =>
          (get & processId(processName)) { processId =>
            complete {
              processService
                .getProcess[Unit](processId)
                .map(resp => resp.map(processToolbarService.getProcessToolbarSettings))
                .map(toResponseEither[ProcessToolbarSettings])
            }
          }
        } ~ path("processes" / "category" / Segment / Segment) { (processName, category) =>
          (post & processId(processName)) { processId =>
            hasAdminPermission(user) {
              complete {
                processService
                  .updateCategory(processId, category)
                  .withSideEffect(response => sideEffectAction(response) { resp =>
                    OnCategoryChanged(processId.id, resp.oldCategory, resp.newCategory)
                  })
                  .map(toResponseEither[UpdateProcessCategoryResponse])
              }
            }
          }
        } ~ path("processes" / Segment / LongNumber / "compare" / LongNumber) { (processName, thisVersion, otherVersion) =>
          (get & processId(processName)) { processId =>
            parameter('businessView ? false) { businessView =>
              complete {
                withJson(processId.id, thisVersion, businessView) { thisDisplayable =>
                  withJson(processId.id, otherVersion, businessView) { otherDisplayable =>
                    ProcessComparator.compare(thisDisplayable, otherDisplayable)
                  }
                }
              }
            }
          }
        } ~ new NodesResources(processRepository, typeToConfig.mapValues(_.modelData)).securedRoute
      }
  }

  private def sideEffectAction[T](response: XError[T])(eventAction: T => ProcessChangeEvent)(implicit user: LoggedUser): Unit =
    sideEffectAction(response.toOption)(eventAction)

  private def sideEffectAction[T](response: Option[T])(eventAction: T => ProcessChangeEvent)(implicit user: LoggedUser): Unit =
    response.foreach(resp => processChangeListener.handle(eventAction(resp)))

  private def validateJsonForImport(processId: ProcessIdWithName, json: String): Validated[EspError, CanonicalProcess] = {
    ProcessMarshaller.fromJson(json) match {
      case Valid(process) if process.metaData.id != processId.name.value =>
    Invalid(WrongProcessId(processId.name.value, process.metaData.id))
      case Valid(process) => Valid(process)
      case Invalid(unmarshallError) => Invalid(UnmarshallError(unmarshallError.msg))
    }
  }

  private def importProcess(processId: ProcessIdWithName, process: CanonicalProcess)(implicit user: LoggedUser): Future[ToResponseMarshallable] = {
    processRepository.fetchLatestProcessDetailsForProcessIdEither[Unit](processId.id).map { detailsXor =>
      val validatedProcess = detailsXor
        .map(details => ProcessConverter.toDisplayable(process, details.processingType))
        .map(process => new ValidatedDisplayableProcess(process, processValidation.validate(process)))
      toResponseXor(validatedProcess)
    }
  }

  private def fetchProcessStatesForProcesses(processes: List[BaseProcessDetails[Unit]])(implicit user: LoggedUser): Future[Map[String, ProcessState]] = {
    import cats.instances.future._
    import cats.instances.list._
    import cats.syntax.traverse._
    processes.map(process => processService.getProcessState(process.idWithName).map(status => process.name -> status))
      .sequence[Future, (String, ProcessState)].map(_.toMap)
  }

  //This is temporary function to enriching process state data
  //TODO: Remove it when we will support cache for state
  private def enrichDetailsWithProcessState[PS: ProcessShapeFetchStrategy](process: BaseProcessDetails[PS]): BaseProcessDetails[PS] =
    process.copy(state = processManager(process.processingType).map(m => ProcessStatus.createState(
      m.processStateDefinitionManager.mapActionToStatus(process.lastAction.map(_.action)),
      m.processStateDefinitionManager
    )))

  private def processManager(processingType: ProcessingType): Option[ProcessManager] =
    typeToConfig.forType(processingType).map(_.processManager)

  private def withJson(processId: ProcessId, version: Long, businessView: Boolean)
                      (process: DisplayableProcess => ToResponseMarshallable)(implicit user: LoggedUser): ToResponseMarshallable
  = processRepository.fetchProcessDetailsForId[DisplayableProcess](processId, version, businessView).map { maybeProcess =>
      maybeProcess.flatMap(_.json) match {
        case Some(displayable) => process(displayable)
        case None => HttpResponse(status = StatusCodes.NotFound, entity = s"Process $processId in version $version not found"): ToResponseMarshallable
      }
  }

  private def validateAndReverseResolveAll(processDetails: Future[List[BaseProcessDetails[CanonicalProcess]]]) : Future[List[ValidatedProcessDetails]] = {
    processDetails.flatMap(all => Future.sequence(all.map(validateAndReverseResolve(_, businessView = false))))
  }

  private def validateAndReverseResolve(processDetails: BaseProcessDetails[CanonicalProcess], businessView: Boolean): Future[ValidatedProcessDetails] = {
    val validatedDetails = processDetails.mapProcess { canonical: CanonicalProcess =>
      val processingType = processDetails.processingType
      val validationResult = processResolving.validateBeforeUiReverseResolving(canonical, processingType)
      processResolving.reverseResolveExpressions(canonical, processingType, businessView, validationResult)
    }
    Future.successful(validatedDetails)
  }

  private implicit class ToBasicConverter(self: Future[List[BaseProcessDetails[_]]]) {
    def toBasicProcess: Future[List[BasicProcess]] = self.map(f => f.map(bpd => BasicProcess(bpd)))
  }
}

object ProcessesResources {
  case class UnmarshallError(message: String) extends Exception(message) with FatalError

  case class WrongProcessId(processId: String, givenId: String) extends Exception(s"Process has id $givenId instead of $processId") with BadRequestError

  case class ProcessNotInitializedError(id: String) extends Exception(s"Process $id is not initialized") with NotFoundError

  case class NodeNotFoundError(processId: String, nodeId: String) extends Exception(s"Node $nodeId not found inside process $processId") with NotFoundError
}
