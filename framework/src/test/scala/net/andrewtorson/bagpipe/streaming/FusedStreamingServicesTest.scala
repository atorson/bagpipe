/*
 *
 * Author: Andrew Torson
 * Date: Feb 8, 2017
 */

package net.andrewtorson.bagpipe.streaming


import java.util.Date

import scala.concurrent.{Await, Future, Promise}
import scala.util.{Success, Try}
import akka.Done
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import net.andrewtorson.bagpipe._
import net.andrewtorson.bagpipe.entities._
import net.andrewtorson.bagpipe.utils._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.concurrent.duration._
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri}
import de.heikoseeberger.akkasse.ServerSentEvent

import scala.language.existentials
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._
import net.andrewtorson.bagpipe.eventbus.{EntityBusEvent, EventBusClassifiers, EventBusEntityID}
import net.andrewtorson.bagpipe.networking.BE

/**
 * Created by Andrew Torson on 2/8/17.
 */
class FusedStreamingServicesTest extends FunSuite with BeforeAndAfterAll{
  implicit val timeout = Timeout(5.seconds)
  val data: TestData = CoreTestData
  val triggerEvent: EntityBusEvent = EntityBusEvent(EntityOneOf(CoreTestData.statistic), EventBusClassifiers(REST_CREATED, entityPredicate()))
  val modules = new TestConfigurationModule with ActorModuleImpl

  implicit lazy val actorSystem: ActorSystem = modules.system
  implicit lazy val materializer = ActorMaterializer()
  implicit lazy val ec = actorSystem.dispatcher

  class FusedTestStreamingModule(override protected val dependency: ServiceBusModule with ActorModule) extends CoreStreamingModule{
    override protected def bootstrapFlows() = {
      EntityDefinition.all().foldLeft[Boolean](true)((b, d) => b && addCoreService(d.tag))
    }
    ApplicationService.register[StreamingModule](this)
  }

  class FusedTestTcpClient(override protected val dependency: ConfigurationModule with ActorModule) extends TcpClientModuleProto(dependency){

    override  protected def warmUpFlow(g: Flow[I, O, Future[Done]]) = g

    ApplicationService.register[TcpClientModule](this)
  }
  val tcpServer: TcpServerModule =  new TcpServerModuleProto(modules)
  val tcpClient: TcpClientModule =  new FusedTestTcpClient(modules)

  class FusedTestHttpClient(override protected val dependency: ConfigurationModule with ActorModule) extends HttpRestfulClientModuleProto(dependency){

    outer =>

    class FusedTestInnerHttpClient extends InnerHttpEntityWiseClientModule{

      override protected def warmUpFlow(g: Flow[ServerSentEvent, HttpRequest, Future[Done]]) = g
    }

    override protected val innerEntityWiseClientModuleProto = new FusedTestInnerHttpClient

    ApplicationService.register[HttpRestfulClientModule](this)
  }


  test("Testing Fused HTTP-viaDB-TCP streaming flow") {
    new PersistenceModuleImpl(modules)
    val serviceBus = new ServiceBusModuleImpl(modules)
    val streamingModule = new FusedTestStreamingModule(serviceBus)
    new HttpRestfulServerModuleProto(modules)
    new FusedTestHttpClient(modules)
    val b = (ApplicationService[PersistenceModule].start
    && ApplicationService[StreamingModule].start
    && ApplicationService[ServiceBusModule].start
    && ApplicationService[HttpRestfulServerModule].start
    && ApplicationService[HttpRestfulClientModule].start
    && ApplicationService[TcpServerModule].start
    && ApplicationService[TcpClientModule].start)
    assert(b, "Failed to start test services")
    val testEntities = data.testEntityDefinitions.filter(d => !d.nested.filter(_.subentityDefinition == AuditDef).isEmpty).map(x => Try{data.getTestEntity(x.tag)}).collect{
      case Success(e) => e}


    // have to sleep to have the flows warmed-up before publishing
    Thread.sleep(6000)
    val now = new Date()
    val f = serviceBus.receiveUntil(topic(TCP_CREATE)(EventBusEntityID.EntityCategory):_*)(10.seconds)({x => false})
    serviceBus.deliver(triggerEvent)
    // sleep for a while to wait until all DB entities are persisted and event bus quiets down
    Thread.sleep(3000)
    //start polling DB and routing to TCP
    testEntities.foreach(e => streamingModule.addRoutingService(DB_CREATED)(TCP_CREATED)()(e.asInstanceOf[BE].definition.tag))
    testEntities.foreach(e => streamingModule.addPollingService(1.seconds, Some(() => now))(e.asInstanceOf[BE].definition.tag))
    val receivedEntities = Await.result(f,10.seconds)
    val residual = testEntities.map(_.asInstanceOf[BE].namekey).toSet.diff(receivedEntities.map(_.value.key).toSet)
    assert(residual.isEmpty, s"Failed to send and receive all test entities from $testEntities into $receivedEntities")
    assert(testEntities.size == receivedEntities.size, s"Received duplicates in $receivedEntities")
  }

  protected def deleteTestData = {
    Await.result(StatisticDef.crud.read(triggerEvent.value.entity.get.namekey),5.seconds) match {
      case Some(e: Statistic) =>  {
        Await.result(Future.sequence(Seq(
          StatisticDef.crud.delete(e.namekey)
        )),5.seconds)
      }
      case _ => {}
    }
  }


  override protected def afterAll(): Unit = {
    deleteTestData
    ApplicationService[TcpClientModule].stop
    ApplicationService[TcpServerModule].stop
    ApplicationService[HttpRestfulClientModule].stop
    ApplicationService[HttpRestfulServerModule].stop
    ApplicationService[ServiceBusModule].stop
    ApplicationService[StreamingModule].stop
    ApplicationService[PersistenceModule].stop

    super.afterAll()
  }


}
