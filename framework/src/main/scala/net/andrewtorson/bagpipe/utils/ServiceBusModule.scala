/*
 *
 * Author: Andrew Torson
 * Date: Nov 15, 2016
 */

package net.andrewtorson.bagpipe.utils

import java.util.Date

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.{Success, Try}
import akka.{Done, NotUsed}
import akka.actor.ActorRef
import akka.stream._
import akka.stream.scaladsl.{Flow, FlowOps, Keep, RunnableGraph, Sink, Source, SubFlow}
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.eventbus._
import net.andrewtorson.bagpipe.streaming.{StreamingFlowOps, StreamingPublisherActor}


/**
 * Created by Andrew Torson on 11/15/16.
 */
trait ServiceBusModule extends ApplicationService[ServiceBusModule]{

  type ServiceRepr = RunnableGraph[UniqueKillSwitch]

  val servicesRegistry: SimpleRegistryFacade[ServiceRepr]

  def eventBus: EntityEventBus = EntityEventBus

  def source(topics: EventBusTopicID*)(ordered: Boolean = true, async: Boolean = false): (ID, Source[EntityBusEvent, ActorRef])

  def receiveOne(topics: EventBusTopicID*)(duration: FiniteDuration)(correlationIdOption: Option[ID] = None): Future[Option[EntityBusEvent]]

  def receiveUntil(topics: EventBusTopicID*)(duration: FiniteDuration)(stop: EntityBusEvent => Boolean, correlationIdOption: Option[ID] = None): Future[List[EntityBusEvent]]

  def deliver(events: EntityBusEvent*): Unit = events.foreach(eventBus.publish(_))

  def addServiceFlow[E<:BaseEntity[E]](messageIDs: EventBusMessageID*)(flow: Flow[EntityBusEvent, Seq[EntityBusEvent], _], ordered: Boolean = true, async: Boolean = false, isPersistent: Boolean = true)(implicit ct: ClassTag[E]): ID

  def addPollingFlow[E<:BaseEntity[E]](pollingInterval: FiniteDuration, startDate: Option[() => Date] = None, isPersistent: Boolean = true)(implicit ct: ClassTag[E]): ID

  def removeFlow(flowID: ID): Boolean = servicesRegistry -- (Reg.Remove[ServiceRepr], flowID)

  def getFlows(): Set[ID]

}

object ServiceBusModule {

  def generateFlowID(topics: EventBusTopicID*):ID = ID(ID(ID.FlowCategory, topics.foldLeft(""){(x: String, y:ID) => x + "," + y.toString}))

}

class ServiceBusModuleImpl(actorModule: ActorModule) extends ServiceBusModule with ActorModule{

  override val system = actorModule.system

  implicit val actorSystem = system
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher


  override val servicesRegistry = new ConcurrentMapSimpleRegistry[ServiceRepr](Reg.cbBuilder[ServiceRepr].build(), Some(ec))

  override def getFlows() = Set[ID]() ++ servicesRegistry.outer.regContext.registeredItems.keySet

  override val tag = ClassTag[ServiceBusModule](classOf[ServiceBusModule])

  override val abnormalTerminationCode = 3

  protected val killSwitches =  new ConcurrentMapSimpleRegistry[UniqueKillSwitch](Reg.cbBuilder[UniqueKillSwitch].build(), Some(ec))

  protected var innerRepr: Option[NotUsed] = None

  override def source(topics: EventBusTopicID*)(ordered: Boolean = false, async: Boolean = false) = {
    val flowID = ServiceBusModule.generateFlowID(topics:_*)
    val subscriber = EntityBusSubscriber(StreamingFlowOps.getEventBusSubscriberID(flowID), ordered, async)
    (flowID, Source.actorPublisher[EntityBusEvent](StreamingPublisherActor.props[EntityBusEvent](Some(actorModule.streamingActorsRegistry), Some(StreamingFlowOps.getFlowActorID(flowID, false)), true, false))
      .mapMaterializedValue{x: ActorRef =>{
        if (!((for (topic<-topics) yield eventBus.subscribe(subscriber, topic))
          reduce {(x: Boolean,y: Boolean) => x && y})) throw new IllegalStateException(s"Could not initialize source${subscriber.subscriberID} for event bus subscription topics $topics")
        subscriber.setActor(x)
        x
      }})
  }



  override def receiveOne(topics: EventBusTopicID*)(duration: FiniteDuration)(correlationIdOption: Option[ID] = None) = {
    innerSource(topics:_*)(_ => false, correlationIdOption, false)(duration).take(1).toMat(Sink.lastOption[EntityBusEvent])(Keep.right).run()
  }

  private def innerSource(topics: EventBusTopicID*)(stop: EntityBusEvent => Boolean, correlationIdOption: Option[ID], ordered: Boolean)(duration: FiniteDuration) =
    source(topics:_*)(ordered, true)._2.takeWithin(duration).filter(x => x.correlationID == correlationIdOption.getOrElse(x.correlationID)).takeWhile(!stop(_))

  override def receiveUntil(topics: EventBusTopicID*)(duration: FiniteDuration)(stop: EntityBusEvent => Boolean, correlationIdOption: Option[ID] = None) = {
    innerSource(topics:_*)(stop, correlationIdOption, true)(duration).toMat(Sink.fold(List[EntityBusEvent]())(_ :+ _))(Keep.right).run()
  }

  override def addServiceFlow[E<:BaseEntity[E]](messageIDs: EventBusMessageID*)(flow: Flow[EntityBusEvent, Seq[EntityBusEvent], _], ordered: Boolean = true, async: Boolean = false, isPersistent: Boolean = true)(implicit ct: ClassTag[E]) = {
    val s = source(EventBusClassifiers.topic(messageIDs: _*)(EntityDefinition(ct).categoryID): _*)(ordered, async)
    val id = s._1
    val service: ServiceRepr = s._2.viaMat (KillSwitches.single) (Keep.right).via(flow).toMat(Sink.foreach (deliver) )(Keep.both).mapMaterializedValue(x =>{
        if (!isPersistent) x._2.onComplete(_ => removeFlow(id))
        x._1
    })
    servicesRegistry ++ (Reg.Add[ServiceRepr], id, service) && servicesRegistry >> (Reg.onRemove[ServiceRepr](_=>removeFlow(id)), id)
    if (isRunning) {
      servicesRegistry ? (Reg.Get[ServiceRepr], id) match {
        case Success(Some(service: ServiceRepr)) => killSwitches ++(Reg.Add[UniqueKillSwitch], id, service.run())
        case _ => {}
      }
    }
    id
  }

  override def addPollingFlow[E<:BaseEntity[E]](pollingInterval: FiniteDuration, startDate: Option[() => Date] = None, isPersistent: Boolean = true)(implicit ct: ClassTag[E]) = {
    import scala.concurrent.duration._
    import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._
    val definition = EntityDefinition(ct)
    val id = ServiceBusModule.generateFlowID(topic(INTERNAL_POLL)(definition.categoryID): _*)
    val tickingSource = Source.tick(0.millis, pollingInterval, false).mapMaterializedValue(x => NotUsed)
    val source = if (startDate.isDefined) {
        Source.zipN[Boolean](Seq(Source.unfold[Boolean,Boolean](true)(s => if (s) Some(false,s) else Some(s,s)),
        tickingSource).asInstanceOf[scala.collection.immutable.Seq[Source[Boolean,_]]]).map(_(0))
    } else {
        tickingSource
    }
    val service = source.viaMat(KillSwitches.single)(Keep.right).map(_ match {
      case true if (startDate.isDefined) => {
        val generatedStartDate = startDate.get()
        val currentTime = new Date()
        if (generatedStartDate.before(currentTime)) generatedStartDate.getTime else currentTime.getTime
      }
      case _ => System.currentTimeMillis
    }).map(x => EntityBusEvent[E](KeyOneOf(""), EventBusClassifiers(INTERNAL_POLL, trivialPredicate()), new Date(x)))
      .toMat(Sink.foreach[EntityBusEvent](deliver(_)))(Keep.both).mapMaterializedValue(x =>{
         if (!isPersistent) x._2.onComplete(_ => removeFlow(id))
         x._1
       })
    servicesRegistry ++ (Reg.Add[ServiceRepr], id, service) && servicesRegistry >> (Reg.onRemove[ServiceRepr](_=>removeFlow(id)), id)
    if (isRunning) {
      servicesRegistry ? (Reg.Get[ServiceRepr], id) match {
        case Success(Some(service: ServiceRepr)) => killSwitches ++(Reg.Add[UniqueKillSwitch], id, service.run())
        case _ => {}
      }
    }
    id
  }

  override def removeFlow(flowID: ID) = {
    val b = super.removeFlow(flowID)
    if (isRunning) {
      killSwitches ? (Reg.Get[UniqueKillSwitch], flowID) match {
        case Success(Some(switch: UniqueKillSwitch)) => {
          switch.shutdown()
          b && (killSwitches -- (Reg.Remove[UniqueKillSwitch], flowID))
        }
        case _ => false
      }
    } else {
      b
    }
  }

  override def isRunning = innerRepr.isDefined

  override def start = if (!isRunning) this.synchronized{
    Try{
      servicesRegistry.outer.regContext.registeredItems.foreach(x => {
        killSwitches ++(Reg.Add[UniqueKillSwitch], x._1, x._2.run())
      })
      innerRepr = Some(NotUsed)
    } match {
      case Success(_) => true
      case _ => false
    }
  } else {false}

  override def stop= if (isRunning) this.synchronized{
    Try{
      killSwitches.outer.regContext.registeredItems.values.foreach(_.shutdown())
      innerRepr = None
    } match {
      case Success(_) => true
      case _ => false
    }
  } else {false}

  ApplicationService.register[ServiceBusModule](this)

}
