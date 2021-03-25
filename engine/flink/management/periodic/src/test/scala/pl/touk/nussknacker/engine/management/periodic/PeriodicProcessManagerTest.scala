package pl.touk.nussknacker.engine.management.periodic

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSuite, Inside, Matchers, OptionValues}
import pl.touk.nussknacker.engine.api.ProcessVersion
import pl.touk.nussknacker.engine.api.deployment.simple.SimpleStateStatus
import pl.touk.nussknacker.engine.api.deployment.{CustomProcess, DeploymentData, FailedStateStatus, GraphProcess, ProcessActionType, User}
import pl.touk.nussknacker.engine.api.process.ProcessName
import pl.touk.nussknacker.engine.management.FlinkStateStatus
import pl.touk.nussknacker.engine.management.periodic.model.PeriodicProcessDeploymentStatus
import pl.touk.nussknacker.engine.management.periodic.service.{DefaultAdditionalDeploymentDataProvider, EmptyListener}
import pl.touk.nussknacker.test.PatientScalaFutures

import java.time.Clock
import scala.concurrent.Await
import scala.concurrent.duration.{FiniteDuration, HOURS, SECONDS}

class PeriodicProcessManagerTest extends FunSuite
  with Matchers
  with ScalaFutures
  with OptionValues
  with Inside
  with TableDrivenPropertyChecks
  with PatientScalaFutures {

  import org.scalatest.LoneElement._

  import scala.concurrent.ExecutionContext.Implicits.global

  private val processName = ProcessName("test1")
  private val processVersion = ProcessVersion(versionId = 42L, processName = processName, user = "test user", modelVersion = None)

  class Fixture {
    val repository = new db.InMemPeriodicProcessesRepository
    val delegateProcessManagerStub = new ProcessManagerStub
    val jarManagerStub = new JarManagerStub
    val periodicProcessService = new PeriodicProcessService(
      delegateProcessManager = delegateProcessManagerStub,
      jarManager = jarManagerStub,
      scheduledProcessesRepository = repository,
      EmptyListener,
      DefaultAdditionalDeploymentDataProvider, Clock.systemDefaultZone()
    )
    val periodicProcessManager = new PeriodicProcessManager(
      delegate = delegateProcessManagerStub,
      service = periodicProcessService,
      periodicPropertyExtractor = CronPropertyExtractor(),
      toClose = () => ()
    )
  }

  test("findJobStatus - should return none for no job") {
    val f = new Fixture

    val state = f.periodicProcessManager.findJobStatus(processName).futureValue

    state shouldBe 'empty
  }

  test("findJobStatus - should be scheduled when process scheduled and no job on Flink") {
    val f = new Fixture
    f.repository.addActiveProcess(processName, PeriodicProcessDeploymentStatus.Scheduled)

    val state = f.periodicProcessManager.findJobStatus(processName).futureValue

    val status = state.value.status
    status shouldBe a[ScheduledStatus]
    status.isRunning shouldBe true
    state.value.allowedActions shouldBe List(ProcessActionType.Cancel, ProcessActionType.Deploy)
  }

  test("findJobStatus - should be scheduled when process scheduled and job finished on Flink") {
    val f = new Fixture
    f.repository.addActiveProcess(processName, PeriodicProcessDeploymentStatus.Scheduled)
    f.delegateProcessManagerStub.setStateStatus(FlinkStateStatus.Finished)

    val state = f.periodicProcessManager.findJobStatus(processName).futureValue

    val status = state.value.status
    status shouldBe a[ScheduledStatus]
    state.value.allowedActions shouldBe List(ProcessActionType.Cancel, ProcessActionType.Deploy)
  }

  test("findJobStatus - should be running when process deployed and job running on Flink") {
    val f = new Fixture
    f.repository.addActiveProcess(processName, PeriodicProcessDeploymentStatus.Deployed)
    f.delegateProcessManagerStub.setStateStatus(FlinkStateStatus.Running)

    val state = f.periodicProcessManager.findJobStatus(processName).futureValue

    val status = state.value.status
    status shouldBe FlinkStateStatus.Running
    state.value.allowedActions shouldBe List(ProcessActionType.Cancel)
  }

  test("findJobStatus - should be waiting for reschedule if job finished on Flink but process is still deployed") {
    val f = new Fixture
    f.repository.addActiveProcess(processName, PeriodicProcessDeploymentStatus.Deployed)
    f.delegateProcessManagerStub.setStateStatus(FlinkStateStatus.Finished)

    val state = f.periodicProcessManager.findJobStatus(processName).futureValue

    val status = state.value.status
    status shouldBe WaitingForScheduleStatus
    state.value.allowedActions shouldBe List(ProcessActionType.Cancel)
  }

  test("findJobStatus - should be failed after unsuccessful deployment") {
    val f = new Fixture
    f.repository.addActiveProcess(processName, PeriodicProcessDeploymentStatus.Failed)

    val state = f.periodicProcessManager.findJobStatus(processName).futureValue

    val status = state.value.status
    status shouldBe SimpleStateStatus.Failed
    state.value.allowedActions shouldBe List(ProcessActionType.Cancel)
  }

  test("deploy - should fail for custom process") {
    val f = new Fixture

    val deploymentResult = f.periodicProcessManager.deploy(processVersion, DeploymentData.empty, CustomProcess("test"), None)

    intercept[PeriodicProcessException](Await.result(deploymentResult, patienceConfig.timeout))
  }

  test("deploy - should fail for invalid periodic property") {
    val f = new Fixture

    val deploymentResult = f.periodicProcessManager.deploy(processVersion, DeploymentData.empty, GraphProcess("broken"), None)

    intercept[PeriodicProcessException](Await.result(deploymentResult, patienceConfig.timeout))
  }

  test("deploy - should schedule periodic process") {
    val f = new Fixture

    f.periodicProcessManager.deploy(processVersion, DeploymentData.empty, PeriodicProcessGen(), None).futureValue

    f.repository.processEntities.loneElement.active shouldBe true
    f.repository.deploymentEntities.loneElement.status shouldBe PeriodicProcessDeploymentStatus.Scheduled
  }

  test("deploy - should cancel existing process if already scheduled") {
    val f = new Fixture
    f.repository.addActiveProcess(processName, PeriodicProcessDeploymentStatus.Scheduled)

    f.periodicProcessManager.deploy(processVersion, DeploymentData.empty, PeriodicProcessGen(), None).futureValue

    f.repository.processEntities should have size 2
    f.repository.processEntities.map(_.active) shouldBe List(false, true)
  }

  test("should get status of failed job") {
    val f = new Fixture
    f.repository.addActiveProcess(processName, PeriodicProcessDeploymentStatus.Deployed)
    f.delegateProcessManagerStub.setStateStatus(FlinkStateStatus.Failed)

    val state = f.periodicProcessManager.findJobStatus(processName).futureValue

    val status = state.value.status
    status shouldBe SimpleStateStatus.Failed
    state.value.allowedActions shouldBe List(ProcessActionType.Cancel)
  }

  ignore("should cancel failed job after RescheduleActor") {
    val f = new Fixture
    f.repository.addActiveProcess(processName, PeriodicProcessDeploymentStatus.Deployed)
    f.delegateProcessManagerStub.setStateStatus(FlinkStateStatus.Failed)

    //this one is cyclically called by RescheduleActor
    f.periodicProcessService.handleFinished.futureValue

    f.periodicProcessManager.findJobStatus(processName).futureValue.get.status shouldBe SimpleStateStatus.Failed
    f.repository.deploymentEntities.loneElement.status shouldBe PeriodicProcessDeploymentStatus.Failed
    f.repository.processEntities.loneElement.active shouldBe true

    f.periodicProcessManager.cancel(processName, User("test", "Tester")).futureValue

    f.repository.processEntities.loneElement.active shouldBe false
    f.repository.deploymentEntities.loneElement.status shouldBe PeriodicProcessDeploymentStatus.Failed

    //this one fails
    f.periodicProcessManager.findJobStatus(processName).futureValue.get.status shouldBe SimpleStateStatus.Canceled
  }

  ignore("should cancel failed job before RescheduleActor") {
    val f = new Fixture
    f.repository.addActiveProcess(processName, PeriodicProcessDeploymentStatus.Deployed)
    f.delegateProcessManagerStub.setStateStatus(FlinkStateStatus.Failed)

    f.periodicProcessManager.cancel(processName, User("test", "Tester")).futureValue

    f.repository.processEntities.loneElement.active shouldBe false
    //this one fails
    f.repository.deploymentEntities.loneElement.status shouldBe PeriodicProcessDeploymentStatus.Failed
    //this also
    f.periodicProcessManager.findJobStatus(processName).futureValue.get.status shouldBe SimpleStateStatus.Canceled
  }

  test("should cancel failed after disappeared from Flink console") {
    val f = new Fixture
    f.repository.addActiveProcess(processName, PeriodicProcessDeploymentStatus.Deployed)
    f.delegateProcessManagerStub.setStateStatus(FlinkStateStatus.Failed)

    //this one is cyclically called by RescheduleActor
    f.periodicProcessService.handleFinished.futureValue

    //after some time Flink stops returning job status
    f.delegateProcessManagerStub.jobStatus = None

    f.periodicProcessManager.findJobStatus(processName).futureValue.get.status shouldBe SimpleStateStatus.Failed
    f.repository.deploymentEntities.loneElement.status shouldBe PeriodicProcessDeploymentStatus.Failed
    f.repository.processEntities.loneElement.active shouldBe true

    f.periodicProcessManager.cancel(processName, User("test", "Tester")).futureValue

    f.repository.processEntities.loneElement.active shouldBe false
    f.repository.deploymentEntities.loneElement.status shouldBe PeriodicProcessDeploymentStatus.Failed
    f.periodicProcessManager.findJobStatus(processName).futureValue shouldBe None
  }
}
