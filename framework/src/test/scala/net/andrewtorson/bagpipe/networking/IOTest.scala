/*
 *
 * Author: Andrew Torson
 * Date: Jan 25, 2017
 */

package net.andrewtorson.bagpipe.networking



import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Success, Try}
import akka.util.Timeout
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.entities._
import net.andrewtorson.bagpipe.eventbus.{EntityBusEvent, EventBusClassifiers, EventBusEntityID, EventBusMessageID}
import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._
import net.andrewtorson.bagpipe.utils._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.language.existentials
import scala.reflect.ClassTag
import akka.Done
import akka.stream.scaladsl.Flow


/**
 * Created by Andrew Torson on 1/25/17.
 */
class IOTest extends FunSuite with BeforeAndAfterEach{
  implicit val timeout = Timeout(5.seconds)
  val modules = new TestConfigurationModule with ActorModuleImpl
  val serviceBusModule = new ServiceBusModuleImpl(modules)
  val data: TestData = CoreTestData
  val tcpServer: TcpServerModule =   new TcpServerModuleProto(modules)
  val tcpClient: TcpClientModule =   new TcpClientModuleProto(modules)

  test("Testing TCP-IO") {
    ApplicationService[ServiceBusModule].start
    ApplicationService[TcpServerModule].start
    ApplicationService[TcpClientModule].start
    val testEntities = data.testEntityDefinitions.map(x => Try{data.getTestEntity(x.tag)}).collect{
      case Success(e) => e}
    // have to sleep to have the flows warmed-up before publishing
    Thread.sleep(1000)
    val f = serviceBusModule.receiveUntil(topic(TCP_CREATE)(EventBusEntityID.EntityCategory):_*)(1.seconds)({x => false})
    serviceBusModule.deliver(testEntities.map(e => EntityBusEvent(EntityOneOf(e), EventBusClassifiers(TCP_CREATED, entityPredicate()))): _*)
    val receivedEntities = Await.result(f,10.seconds).groupBy(_.value.key).mapValues(_.head).values.map(x => Try{x.value.entity.get.enforceAuditIfApplicable(true)})
      .collect{case Success(e) => e}
    val residual = testEntities.toSet[BE].diff(receivedEntities.toSet[BE])
    assert(residual.isEmpty, s"Failed to send and receive all test entities from $testEntities into $receivedEntities")
  }

  test("Testing HTTP-IO") {
    val p = Promise[Done]()
    val counter = new AtomicInteger(0)
    val testEntities = data.testEntityDefinitions.map(x => Try{data.getTestEntity(x.tag)}).collect{
      case Success(e) => e}
    val testEventRegistry = new ConcurrentMapSimpleRegistry[EntityBusEvent](Reg.cbBuilder[EntityBusEvent].WithAdd({(x,y) => {
      if (counter.incrementAndGet() == 2*testEntities.size) p.success(Done)
    }}).build())
    class TestStreamingModuleImpl(override protected val dependency: ServiceBusModule with ActorModule) extends CoreStreamingModule {

      override protected def addCoreService[E<:BaseEntity[E]](implicit ct: ClassTag[E]) =
        registerServiceFlow(dependency.addServiceFlow[E](REST_CREATE, REST_UPDATE)(testFlow), testFlow)

      override protected def bootstrapFlows() = {
        EntityDefinition.all().foldLeft[Boolean](true)((b, d) => b && addCoreService(d.tag))
      }

      private val testFlow = Flow[EntityBusEvent].map(x => x.value match {
        case EntityOneOf(v) => {
          testEventRegistry ++ (Reg.Add[EntityBusEvent], x.value.id, x)
          Seq(EntityBusEvent[x.E](x.value, EventBusClassifiers(EventBusMessageID(INTERNAL, x.topic.messageCategoryAncestors.diff(Set(REST)).head match {
            case CREATE => CREATED
            case UPDATE => UPDATED
            case z => z
          }), entityPredicate()), correlationID = x.correlationID)(x.value.etag))
        }
        case _ => Seq[EntityBusEvent]()
      })

      ApplicationService.register[StreamingModule](this)
    }
    new TestStreamingModuleImpl(serviceBusModule)
    new HttpRestfulServerModuleProto(modules)
    new HttpRestfulClientModuleProto(modules)
    ApplicationService[StreamingModule].start
    ApplicationService[ServiceBusModule].start
    ApplicationService[HttpRestfulServerModule].start
    ApplicationService[HttpRestfulClientModule].start
    // have to sleep to have the flows warmed-up before publishing
    Thread.sleep(3000)
    serviceBusModule.deliver(testEntities.map(e => EntityBusEvent(EntityOneOf(e), EventBusClassifiers(REST_CREATED, entityPredicate()))): _*)
    Await.ready(p.future,5.seconds)
    val receivedEntities = testEventRegistry.outer.regContext.registeredItems.values.groupBy(_.value.key).mapValues(_.head).values.map(x => Try{x.value.entity.get.enforceAuditIfApplicable(true)})
      .collect{case Success(e) => e}
    val residual = testEntities.toSet[BE].diff(receivedEntities.toSet[BE])
    assert(residual.isEmpty, s"Failed to send and receive all test entities from $testEntities into $receivedEntities")
  }

  protected def stopService[E<:ApplicationService[E]:ClassTag] = {
    Try{ApplicationService[E]} match {
      case Success(s) => s.stop
      case _ => {}
    }
  }

  override protected def afterEach(): Unit = {

    Try{ApplicationService[TcpClientModule].stop
    ApplicationService[TcpServerModule].stop
    ApplicationService[HttpRestfulClientModule].stop
    ApplicationService[HttpRestfulServerModule].stop
    ApplicationService[ServiceBusModule].stop
    ApplicationService[StreamingModule].stop}

    super.afterEach()
  }


}
