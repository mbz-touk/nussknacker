package pl.touk.nussknacker.engine.management.periodic

import java.time.{Clock, LocalDateTime, ZoneOffset}
import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import pl.touk.nussknacker.engine.management.periodic.db.{DbInitializer, SlickPeriodicProcessesRepository}
import pl.touk.nussknacker.engine.ModelData
import pl.touk.nussknacker.engine.api.ProcessVersion
import pl.touk.nussknacker.engine.api.deployment._
import pl.touk.nussknacker.engine.api.deployment.simple.SimpleStateStatus
import pl.touk.nussknacker.engine.api.process.ProcessName
import pl.touk.nussknacker.engine.management.FlinkConfig
import pl.touk.nussknacker.engine.management.periodic.flink.FlinkJarManager
import pl.touk.nussknacker.engine.management.periodic.model.{PeriodicProcessDeployment, PeriodicProcessDeploymentStatus}
import pl.touk.nussknacker.engine.management.periodic.service.{AdditionalDeploymentDataProvider, PeriodicProcessListener}
import slick.jdbc
import slick.jdbc.JdbcProfile
import sttp.client.asynchttpclient.future.AsyncHttpClientFutureBackend
import sttp.client.{NothingT, SttpBackend}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object PeriodicProcessManager {
  def apply(delegate: ProcessManager,
            periodicPropertyExtractor: PeriodicPropertyExtractor,
            enrichDeploymentWithJarDataFactory: EnrichDeploymentWithJarDataFactory,
            periodicBatchConfig: PeriodicBatchConfig,
            flinkConfig: FlinkConfig,
            originalConfig: Config,
            modelData: ModelData,
            listener: PeriodicProcessListener,
            additionalDeploymentDataProvider: AdditionalDeploymentDataProvider): PeriodicProcessManager = {
    implicit val system: ActorSystem = ActorSystem("periodic-process-manager-provider")
    implicit val ec: ExecutionContext = ExecutionContext.global
    implicit val backend: SttpBackend[Future, Nothing, NothingT] = AsyncHttpClientFutureBackend.usingConfigBuilder { builder =>
      builder.setThreadPoolName("AsyncBatchPeriodicClient")
    }

    val clock = Clock.systemDefaultZone()

    val (db: jdbc.JdbcBackend.DatabaseDef, dbProfile: JdbcProfile) = DbInitializer.init(periodicBatchConfig.db)
    val scheduledProcessesRepository = new SlickPeriodicProcessesRepository(db, dbProfile, clock)
    val jarManager = FlinkJarManager(flinkConfig, periodicBatchConfig, modelData, enrichDeploymentWithJarDataFactory(originalConfig))
    val service = new PeriodicProcessService(delegate, jarManager, scheduledProcessesRepository, listener, additionalDeploymentDataProvider, clock)
    system.actorOf(DeploymentActor.props(service, periodicBatchConfig.deployInterval))
    system.actorOf(RescheduleFinishedActor.props(service, periodicBatchConfig.rescheduleCheckInterval))
    val toClose = () => {
      Await.ready(system.terminate(), 10 seconds)
      db.close()
      Await.ready(backend.close(), 10 seconds)
      ()
    }
    new PeriodicProcessManager(delegate, service, periodicPropertyExtractor, toClose)
  }
}

class PeriodicProcessManager(val delegate: ProcessManager,
                             service: PeriodicProcessService,
                             periodicPropertyExtractor: PeriodicPropertyExtractor,
                             toClose: () => Unit)
                            (implicit val ec: ExecutionContext) extends ProcessManager with LazyLogging {

  override def deploy(processVersion: ProcessVersion, deploymentData: DeploymentData, processDeploymentData: ProcessDeploymentData, savepointPath: Option[String]): Future[Option[ExternalDeploymentId]] = {
    (processDeploymentData, periodicPropertyExtractor(processDeploymentData)) match {
      case (GraphProcess(processJson), Right(periodicProperty)) =>
        logger.info(s"About to (re)schedule ${processVersion.processName} in version ${processVersion.versionId}")

        // PeriodicProcessStateDefinitionManager do not allow to redeploy (so doesn't GUI),
        // but NK API does, so we need to handle this situation.
        cancelIfAlreadyDeployedOrFailed(processVersion, deploymentData.user)
          .flatMap(_ => {
            logger.info(s"Scheduling ${processVersion.processName}, versionId: ${processVersion.versionId}")
            service.schedule(periodicProperty, processVersion, processJson)
          }.map(_ => None))
      case (_: GraphProcess, Left(error)) =>
        Future.failed(new PeriodicProcessException(error))
      case _ =>
        Future.failed(new PeriodicProcessException("Only periodic processes can be scheduled"))
    }
  }

  private def cancelIfAlreadyDeployedOrFailed(processVersion: ProcessVersion, user: User): Future[Unit] = {
    findJobStatus(processVersion.processName)
      .map {
        case Some(s) => s.isDeployed || s.status.isFailed //for periodic status isDeployed=true means deployed or scheduled
        case None => false
      }
      .flatMap(shouldStop => {
        if (shouldStop) {
          logger.info(s"Process ${processVersion.processName} is running or scheduled. Cancelling before reschedule")
          cancel(processVersion.processName, user).map(_ => ())
        }
        else Future.successful(())
      })
  }

  override def stop(name: ProcessName, savepointDir: Option[String], user: User): Future[SavepointResult] = deactivate(name) {
    delegate.stop(name, savepointDir, user)
  }

  override def cancel(name: ProcessName, user: User): Future[Unit] = deactivate(name) {
    delegate.cancel(name, user)
  }

  private def deactivate[T](name: ProcessName)(callback: => Future[T]): Future[T] = {
    def handleFailed(processState: Option[ProcessState], periodicDeploymentOpt: Option[PeriodicProcessDeployment]): Future[Unit] = {
      import PeriodicProcessDeploymentStatus._
      periodicDeploymentOpt.map { periodicDeployment =>
        (processState.map(_.status), periodicDeployment.state.status) match {
          case (Some(status), Failed | Deployed) if status.isFailed => service.markFailed(periodicDeployment, processState)
          case _ => Future.successful(())
        }
      }.getOrElse(Future.successful(()))
    }

    for {
      maybeJobStatus <- findJobStatus(name)
      maybePeriodicStatus <- service.getScheduledRunDetails(name)
      _ <- handleFailed(maybeJobStatus, maybePeriodicStatus)
      _ <- service.deactivate(name)
      res <- callback
    } yield res
  }

  override def test[T](name: ProcessName, json: String, testData: TestProcess.TestData, variableEncoder: Any => T): Future[TestProcess.TestResults[T]] =
    delegate.test(name, json, testData, variableEncoder)

  override def findJobStatus(name: ProcessName): Future[Option[ProcessState]] = {
    def handleFailed(original: Option[ProcessState]): Future[Option[ProcessState]] = {
      service.getScheduledRunDetails(name).map {
        // this method returns only active schedules, so 'None' means this process has been already canceled
        case None => original.map(_.copy(status = SimpleStateStatus.Canceled))
        case _ => original
      }
    }

    def handleScheduled(original: Option[ProcessState]): Future[Option[ProcessState]] = {
      service.getScheduledRunDetails(name).map { maybeProcessDeployment =>
        maybeProcessDeployment.map { processDeployment =>
          processDeployment.state.status match {
            case PeriodicProcessDeploymentStatus.Scheduled => Some(ProcessState(
              Some(ExternalDeploymentId("future")),
              status = ScheduledStatus(processDeployment.runAt),
              version = Option(processDeployment.periodicProcess.processVersion),
              definitionManager = processStateDefinitionManager,
              //TODO: this date should be passed/handled through attributes
              startTime = Option(processDeployment.runAt.toEpochSecond(ZoneOffset.UTC)),
              attributes = Option.empty,
              errors = List.empty
            ))
            case PeriodicProcessDeploymentStatus.Failed => Some(ProcessState(
              Some(ExternalDeploymentId("future")),
              status = SimpleStateStatus.Failed,
              version = Option(processDeployment.periodicProcess.processVersion),
              definitionManager = processStateDefinitionManager,
              startTime = Option.empty,
              attributes = Option.empty,
              errors = List.empty
            ))
            case PeriodicProcessDeploymentStatus.Deployed | PeriodicProcessDeploymentStatus.Finished =>
              original.map(o => o.copy(status = WaitingForScheduleStatus))
          }
        }.getOrElse(original)
      }
    }

    // Just to trigger periodic definition manager, e.g. override actions.
    def withPeriodicProcessState(state: ProcessState): ProcessState = ProcessState(
      deploymentId = state.deploymentId,
      status = state.status,
      version = state.version,
      definitionManager = processStateDefinitionManager,
      startTime = state.startTime,
      attributes = state.attributes,
      errors = state.errors
    )

    delegate
      .findJobStatus(name)
      .flatMap {
        // Scheduled again or waiting to be scheduled again.
        case state@Some(js) if js.status.isFinished => handleScheduled(state)
        case state@Some(js) if js.status.isFailed => handleFailed(state)
        // Job was previously canceled and it still exists on Flink but a new periodic job can be already scheduled.
        case state@Some(js) if js.status == SimpleStateStatus.Canceled => handleScheduled(state)
        // Scheduled or never started or latest run already disappeared in Flink.
        case state@None => handleScheduled(state)
        case Some(js) => Future.successful(Some(js))
      }.map(_.map(withPeriodicProcessState))
  }

  override def processStateDefinitionManager: ProcessStateDefinitionManager =
    new PeriodicProcessStateDefinitionManager(delegate.processStateDefinitionManager)

  override def savepoint(name: ProcessName, savepointDir: Option[String]): Future[SavepointResult] = {
    delegate.savepoint(name, savepointDir)
  }

  override def close(): Unit = {
    logger.info("Closing periodic process manager")
    toClose()
    delegate.close()
  }

  override def customActions: List[CustomAction] = List.empty

  override def invokeCustomAction(actionRequest: CustomActionRequest,
                                  processDeploymentData: ProcessDeploymentData): Future[Either[CustomActionError, CustomActionResult]] =
    Future.successful(Left(CustomActionNotImplemented(actionRequest)))
}

case class ScheduledStatus(nextRunAt: LocalDateTime) extends CustomStateStatus("SCHEDULED") {
  override def isRunning: Boolean = true
}

case object WaitingForScheduleStatus extends CustomStateStatus("WAITING_FOR_SCHEDULE") {
  override def isRunning: Boolean = true
}
