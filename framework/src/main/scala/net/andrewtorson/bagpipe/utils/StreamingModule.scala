/*
 *
 * Author: Andrew Torson
 * Date: Jan 13, 2017
 */

package net.andrewtorson.bagpipe.utils

import java.util.Date

import scala.collection.mutable.Buffer
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.{Success, Try}

import akka.{Done, NotUsed}
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink}
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.entities.Audit
import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._
import net.andrewtorson.bagpipe.eventbus.{EntityBusEvent, EventBusClassifiers, EventBusMessageID, EventBusTopicID}
import net.andrewtorson.bagpipe.networking.OE
import scala.language.existentials
import scala.language.implicitConversions

import akka.actor.ActorSystem



/**
 * Created by Andrew Torson on 1/13/17.
 */
trait StreamingModule extends ApplicationService[StreamingModule]{

   type ServiceFlowRepr = Flow[EntityBusEvent, Seq[EntityBusEvent],_]
   type GenericFlowRepr = RunnableGraph[UniqueKillSwitch]

   val serviceRegistry: SimpleRegistryFacade[ServiceFlowRepr]
   val pollingRegistry: SimpleRegistryFacade[FiniteDuration]
   val extrasRegistry: SimpleRegistryFacade[GenericFlowRepr]

}

object StreamingModule {

  def getAsyncFlow(asyncOperation: EntityBusEvent => Future[Seq[EntityBusEvent]])(ordered: Boolean = true, parallelism: Int = 4)
    : Flow[EntityBusEvent, Seq[EntityBusEvent], NotUsed] = (if (ordered) {
    Flow[EntityBusEvent].mapAsync(parallelism)(asyncOperation)
  } else {
    Flow[EntityBusEvent].mapAsyncUnordered(parallelism)(asyncOperation)
  }).withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))

}

trait CoreStreamingModule extends StreamingModule{

  protected val dependency: ServiceBusModule with ActorModule

  implicit lazy val actorSystem: ActorSystem = dependency.system
  implicit lazy val materializer = ActorMaterializer()
  implicit lazy val ec = actorSystem.dispatcher

  override val serviceRegistry = new ConcurrentMapSimpleRegistry[ServiceFlowRepr](Reg.cbBuilder[ServiceFlowRepr].build(), Some(ec))
  override val pollingRegistry = new ConcurrentMapSimpleRegistry[FiniteDuration](Reg.cbBuilder[FiniteDuration].build(), Some(ec))
  override val extrasRegistry = new ConcurrentMapSimpleRegistry[GenericFlowRepr](Reg.cbBuilder[GenericFlowRepr].build(), Some(ec))

  protected def bootstrapFlows(): Boolean

  protected def addCoreService[E<:BaseEntity[E]](implicit ct: ClassTag[E]): Boolean = {
    // DB services
    val tempMap = Map(CREATE->createFlow, UPDATE->updateFlow, READ->readFlow, DELETE->deleteFlow, POLL->pollFlow)
    tempMap.foldLeft[Boolean](true)((b,e) => b && registerServiceFlow(dependency.addServiceFlow(e._1)(e._2, true, true)(ct), e._2))
  }

  protected def registerCustomService(id: ID, service: GenericFlowRepr) = {
    extrasRegistry ++ (Reg.Add[GenericFlowRepr], id, service)
  }

  protected def registerServiceFlow(id: ID, service: ServiceFlowRepr) =
    (serviceRegistry ++ (Reg.Add[ServiceFlowRepr], id, service)
    && serviceRegistry >> (Reg.onRemove[ServiceFlowRepr]{_ => dependency.removeFlow(id)}, id)
    && dependency.servicesRegistry >> (Reg.onRemove[dependency.ServiceRepr]{_ => serviceRegistry -- (Reg.Remove[ServiceFlowRepr], id)}, id))

  protected def registerPollingFlow(id: ID, duration: FiniteDuration) =
    (pollingRegistry ++ (Reg.Add[FiniteDuration], id, duration)
      && pollingRegistry >> (Reg.onRemove[FiniteDuration]{_ => dependency.removeFlow(id)}, id)
      && dependency.servicesRegistry >> (Reg.onRemove[dependency.ServiceRepr]{_ => pollingRegistry -- (Reg.Remove[FiniteDuration], id)}, id))

  implicit class RoutingWaitPredicateConverter(green: EventBusMessageID){
    def ~ (red: EventBusMessageID*) = {
      val greenSet = Set(green)
      val redSet = red.toSet
      Map[EventBusMessageID,Boolean]() ++ greenSet.diff(redSet).map(_ -> true) ++ redSet.diff(greenSet).map(_ -> false)
    }
  }

  def addRoutingService[E<:BaseEntity[E]](from: EventBusMessageID*)(to: EventBusMessageID*)(
    entityMapper: OneOf[E] => Future[OneOf[E]] = {x: OneOf[E] => Future.successful(x)},
    waitPredicate: Map[EventBusMessageID, Boolean] = (Map[EventBusMessageID, Boolean]() ++ from.map(_ -> true)),
    fuseWithBackpressure: Boolean = true)(implicit ct: ClassTag[E]): Boolean = {
    // routing services
    import scala.concurrent.duration._
    val predicateValues = waitPredicate.keySet
    val fromSet = from.toSet
    require(!waitPredicate.filter(_._2 == true).isEmpty, s"Wait predicate $waitPredicate prohibits routing of any event type")
    require(fromSet.filter(waitPredicate.get(_) == Some(false)).isEmpty,
      s"Some of the event type values in $from are explicitly prohibited to be routed by $waitPredicate and will never be routed")
    val flow = if (fromSet.diff(predicateValues).isEmpty) {
      // any 'from' event is approved to be routed immediately - no need to do complex waitPredicate logic
      StreamingModule.getAsyncFlow { x: EntityBusEvent => entityMapper(x.value.asInstanceOf[OneOf[E]]).map(y =>
        to.map(z => EntityBusEvent[E](y, EventBusClassifiers[E](z, trivialPredicate[E]()), x.timestamp, x.correlationID)(y.etag))
      )
      }()
    } else {Flow[EntityBusEvent]
        .groupBy(Int.MaxValue, x => (x.correlationID, x.value.key)) // combination of entity namekey and correlation/batch ID
        .map(x => (x, predicateValues.find(y => x.topic.isMessageSub(y)), fromSet.find(y => x.topic.isMessageSub(y)).isDefined))
        .scan[(Seq[EntityBusEvent], Option[Boolean])]((Seq[EntityBusEvent](), None))((x, y) => (y._2, y._3) match {
        case (Some(z), true) => (x._1 :+ y._1, waitPredicate.get(z))
        case (Some(z), false) => (x._1, waitPredicate.get(z))
        case (None, true) => (x._1 :+ y._1, x._2)
        case _ => x
      })
        //.keepAlive[(Seq[EntityBusEvent], Option[Boolean])](60.seconds, () =>
        // (Seq(EntityBusEvent[E](KeyOneOf[E](""), EventBusClassifiers[E](from(0), trivialPredicate[E]()))), Some(false)))
        .takeWithin(5.seconds) // this is a hard stop. it will kill any non-resolved flows
        .takeWhile(x => x._1.isEmpty || x._2.isEmpty, true)
        .fold[(Seq[EntityBusEvent], Option[Boolean])]((Seq[EntityBusEvent](), None))((x, y) => y)
        .mapAsync(4)(x => (x._1, x._2) match {
          case (s, Some(false)) => Future.successful(Seq[EntityBusEvent]()) // if waitPredicate stops this (bad) entity - do not route
          // otherwise route (if there is any event with 'from' message type to route), including waitPredicate timeouts
          case (s, _) => Future.sequence(s.map(x => entityMapper(x.value.asInstanceOf[OneOf[E]]).map(y => to.map(z =>
            EntityBusEvent[E](y, EventBusClassifiers[E](z, trivialPredicate[E]()), x.timestamp, x.correlationID))))).map(_.flatten.toSeq)
        }).withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider)).mergeSubstreams
    }
    registerServiceFlow(dependency.addServiceFlow[E](fromSet.union(predicateValues).toSeq:_*)(flow, true, !fuseWithBackpressure)(ct), flow)
  }

  def addPollingService[E<:BaseEntity[E]](period: FiniteDuration, startDate: Option[() => Date] = None)(implicit ct: ClassTag[E]): Boolean = {
    registerPollingFlow(dependency.addPollingFlow(period, startDate)(ct), period)
  }

  protected def fromClassTag[E <: BaseEntity[E]](value: OE)(implicit ct: ClassTag[E]): BaseEntity[E] = {
        value.entity.get.asInstanceOf[E].rehydrateFieldValue[Audit]._2
  }

  protected lazy val createFlow = StreamingModule.getAsyncFlow{e: EntityBusEvent => {
    implicit val ct = e.value.etag
    e.value match {
      case (EntityOneOf(value)) => {
        val buffer = Buffer.empty[OE]
        value.definition.crud.createOrUpdate(value, buffer, enforceCreate = true).collect{
        case Right(Done) => buffer.collect{case x: OE if (x.isHydrated) => {
            implicit val etag = x.definition.tag
            val y = fromClassTag(x)(etag)
            EntityBusEvent(EntityOneOf(y.native), EventBusClassifiers(y.version match {
              case v  if v > 1 => DB_UPDATED
              case _ => DB_CREATED
            }, entityPredicate()), correlationID = e.correlationID)
        }}
      }.recover{
        case _ => Seq(EntityBusEvent(KeyOneOf[e.E](e.value.key), EventBusClassifiers(DB_FAILURE, trivialPredicate()), correlationID = e.correlationID))
      }}
      case x => Future.failed[Seq[EntityBusEvent]](new IllegalArgumentException(s"Entity create request $x is invalid"))
    }}}()

  protected lazy val updateFlow = StreamingModule.getAsyncFlow{e: EntityBusEvent => {
    implicit val ct = e.value.etag
    e.value match {
      case (EntityOneOf(value)) => {
        val buffer = Buffer.empty[OE]
        value.definition.crud.createOrUpdate(value, buffer, enforceUpdate = true).collect{
        case Left(Done) => buffer.collect{case x: OE if (x.isHydrated) => {
          implicit val etag = x.definition.tag
          val y = fromClassTag(x)(etag)
          EntityBusEvent(EntityOneOf(y.native), EventBusClassifiers(y.version match {
            case v  if v > 1 => DB_UPDATED
            case _ => DB_CREATED
          }, entityPredicate()), correlationID = e.correlationID)
        }}
      }.recover{
        case _ => Seq(EntityBusEvent(KeyOneOf[e.E](e.value.key), EventBusClassifiers(DB_FAILURE, trivialPredicate()), correlationID = e.correlationID))
      }}
      case x => Future.failed[Seq[EntityBusEvent]](new IllegalArgumentException(s"Entity update request $x is invalid"))
    }}}()

  protected lazy val readFlow = StreamingModule.getAsyncFlow{e: EntityBusEvent => {
    implicit val ct = e.value.etag
    e.value.definition.crud.read(e.value.key).collect{
      case Some(x: e.E)  => Seq(EntityBusEvent(EntityOneOf(x), EventBusClassifiers(DB_RETRIEVED, entityPredicate()), correlationID = e.correlationID))
      case None => Seq(EntityBusEvent(KeyOneOf[e.E](e.value.key), EventBusClassifiers(DB_MISSING, trivialPredicate()), correlationID = e.correlationID))
    }.recover{
      case _ => Seq(EntityBusEvent(KeyOneOf[e.E](e.value.key), EventBusClassifiers(DB_FAILURE, trivialPredicate()), correlationID = e.correlationID))
    }
  }}()


  protected lazy val deleteFlow = StreamingModule.getAsyncFlow{e: EntityBusEvent => {
    implicit val ct = e.value.etag
    val buffer = Buffer.empty[OE]
    e.value.definition.crud.delete(e.value.key, buffer).collect{
      case Done =>  buffer.map(x => {
        implicit val etag = x.definition.tag
        EntityBusEvent(KeyOneOf(x.key)(x.etag), EventBusClassifiers(DB_DELETED, trivialPredicate()), correlationID = e.correlationID)
      })
    }.recover{
      case _ => Seq(EntityBusEvent(KeyOneOf[e.E](e.value.key), EventBusClassifiers(DB_FAILURE, trivialPredicate()), correlationID = e.correlationID))
    }
  }}()

  protected lazy val pollFlow =
    Flow[EntityBusEvent].groupBy(Int.MaxValue, _.topic).sliding(2).mapAsync(1) { s: Seq[EntityBusEvent] => {
      val e = s(0)
      implicit val ct = e.value.etag
      e.value.definition.crud.poll(s(0).timestamp, s(1).timestamp).map(Seq[EntityBusEvent]() ++ _.map(x => {
        val y = x.hydrateFieldValue[Audit]._2
        y.version match {
          case 0 => EntityBusEvent[e.E](EntityOneOf(y), EventBusClassifiers(DB_RETRIEVED, entityPredicate()), y.created, e.correlationID)
          case 1 => EntityBusEvent[e.E](EntityOneOf(y), EventBusClassifiers(DB_CREATED, entityPredicate()), y.created, e.correlationID)
          case _ => EntityBusEvent[e.E](EntityOneOf(y), EventBusClassifiers(DB_UPDATED, entityPredicate()), y.updated, e.correlationID)
        }
      }) :+ EntityBusEvent[e.E](e.value, EventBusClassifiers(DB_POLLED, trivialPredicate()), correlationID = e.correlationID)).recover {
        case _ => Seq(EntityBusEvent(KeyOneOf[e.E](e.value.key), EventBusClassifiers(DB_FAILURE, trivialPredicate()), correlationID = e.correlationID))
      }
    }}.withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider)).mergeSubstreams


  override val tag = ClassTag[StreamingModule](classOf[StreamingModule])

  override val abnormalTerminationCode = 2

  protected var killSwitches: Option[ConcurrentMapSimpleRegistry[UniqueKillSwitch]] = None

  override def isRunning = killSwitches.isDefined

  override def start = if (!isRunning) this.synchronized{
    Try{
      val b = bootstrapFlows()
      if (b) {
        val reg = new ConcurrentMapSimpleRegistry[UniqueKillSwitch](Reg.cbBuilder[UniqueKillSwitch].build())
        (Map[ID, GenericFlowRepr]() ++ extrasRegistry.outer.regContext.registeredItems).foreach(x => reg ++ (Reg.Add[UniqueKillSwitch], x._1, x._2.run()))
        killSwitches = Some(reg)
      }
      b
    } match {
      case Success(b) => b && isRunning
      case _ => false
    }
  } else {false}

  override def stop = if (isRunning) this.synchronized{
    Try{
      val serviceIDs = Set[ID]() ++ serviceRegistry.outer.regContext.registeredItems.keySet
      val pollingIDs = Set[ID]() ++ pollingRegistry.outer.regContext.registeredItems.keySet
      val extraIDs = Set[ID]() ++ extrasRegistry.outer.regContext.registeredItems.keySet
      killSwitches.get.outer.regContext.registeredItems.values.foreach(_.shutdown())
      killSwitches = None
      serviceIDs.foldLeft[Boolean](true)((b, e) => b && serviceRegistry -- (Reg.Remove[ServiceFlowRepr], e)) &&
      pollingIDs.foldLeft[Boolean](true)((b, e) => b && pollingRegistry -- (Reg.Remove[FiniteDuration], e)) &&
      extraIDs.foldLeft[Boolean](true)((b, e) => b && extrasRegistry -- (Reg.Remove[GenericFlowRepr], e))
    } match {
      case Success(b) => b && !isRunning
      case _ => false
    }
  } else {false}

}

class BagpipeStreamingModule(override protected val dependency: ServiceBusModule with ActorModule) extends CoreStreamingModule{
  override protected def bootstrapFlows() = {
    import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._

    // DB services
    EntityDefinition.all().foldLeft[Boolean](true)((b, d) => b && addCoreService(d.tag))

    EntityDefinition.all().foldLeft[Boolean](true)((b, d) => (b
      && addRoutingService(REST_CREATE)(TCP_CREATED, MQ_CREATED)(waitPredicate = DB_CREATED ~ DB_FAILURE)(d.tag)
      && addRoutingService(REST_UPDATE)(TCP_UPDATED, MQ_UPDATED)(waitPredicate = DB_UPDATED ~ DB_FAILURE)(d.tag)
      && addRoutingService(TCP_CREATE)(REST_CREATED, MQ_CREATED)(waitPredicate = DB_CREATED ~ DB_FAILURE)(d.tag)
      && addRoutingService(TCP_UPDATE)(REST_UPDATED, MQ_UPDATED)(waitPredicate = DB_UPDATED ~ DB_FAILURE)(d.tag)
      && addRoutingService(MQ_CREATE)(REST_CREATED, TCP_CREATED)(waitPredicate = DB_CREATED ~ DB_FAILURE)(d.tag)
      && addRoutingService(MQ_UPDATE)(REST_UPDATED, TCP_UPDATED)(waitPredicate = DB_UPDATED ~ DB_FAILURE)(d.tag)
      ))

  }

  ApplicationService.register[StreamingModule](this)
}