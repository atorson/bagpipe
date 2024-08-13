/*
 *
 * Author: Andrew Torson
 * Date: Aug 22, 2016
 */

package net.andrewtorson.bagpipe.messaging


import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.{ByteString, Timeout}
import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._
import net.andrewtorson.bagpipe.eventbus.{EntityBusEvent, EventBusClassifiers, EventBusEntityID, EventBusMessageID}
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.utils._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.language.existentials
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import net.andrewtorson.bagpipe.networking.BE



class MQTest extends FunSuite with BeforeAndAfterEach{
  implicit val timeout = Timeout(5.seconds)
  val modules = new TestConfigurationModule with ActorModuleImpl
  val data: TestData = CoreTestData
  implicit lazy val actorSystem: ActorSystem = modules.system
  implicit lazy val materializer = ActorMaterializer()
  implicit lazy val ec = actorSystem.dispatcher

  test("Testing MQ-IO") {
    val serviceBusModule = new ServiceBusModuleImpl(modules)
    new BagpipeMessagingModule(modules)
    ApplicationService[ServiceBusModule].start
    ApplicationService[MessagingModule].start
    val testEntitiesWithDefinitions = data.testEntityDefinitions.map(x => Try{(x,data.getTestEntity(x.tag))}).collect{
      case Success(e) => e}
    val testEntities = testEntitiesWithDefinitions.map(_._2)
    testEntitiesWithDefinitions.foreach(x =>{
        x._1.mq.outboundSource.to(x._1.mq.inboundSink).run()
    })
    val f = serviceBusModule.receiveUntil(topic(MQ_CREATE)(EventBusEntityID.EntityCategory):_*)(3.seconds)({x => false})
    serviceBusModule.deliver(testEntities.map(e => EntityBusEvent(EntityOneOf(e), EventBusClassifiers(MQ_CREATED, entityPredicate()))): _*)
    val receivedEntities = Await.result(f,10.seconds).groupBy(_.value.key).mapValues(_.head).values.map(x => Try{x.value.entity.get.enforceAuditIfApplicable(true)})
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

    stopService[MessagingModule]
    stopService[ServiceBusModule]

    super.afterEach()
  }


}

