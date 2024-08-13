/*
 *
 * Author: Andrew Torson
 * Date: Feb 13, 2017
 */

package net.andrewtorson.bagpipe.streaming

import java.util.Date

import akka.actor.ActorRef
import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Flow, Source}
import com.typesafe.scalalogging.LazyLogging
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.entities._
import net.andrewtorson.bagpipe.eventbus.{EntityBusEvent, EventBusClassifiers}
import net.andrewtorson.bagpipe.utils._

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Success, Try}
import scala.language.reflectiveCalls
import net.andrewtorson.bagpipe.entities.TripAction.TripActionStatus.{Deliver, Finish, Receive}
import net.andrewtorson.bagpipe.entities.TripState.TripStateStatus.{Carrying, Driving, Undefined}
import net.andrewtorson.bagpipe.entities.Statistic.StatisticStatus.Finalized

import scala.reflect.ClassTag

/**
 * Created by Andrew Torson on 2/13/17.
 */
class BagpipeExampleStreamingModule(override protected val dependency: ServiceBusModule with ActorModule) extends CoreStreamingModule with LazyLogging{
  import scala.concurrent.duration._

  protected val idleTimeout = 180.seconds
  protected val statisticFlowName = "PEEDSEF"

  protected def getTimeNow = Timestamp.defaultInstance.withMillis(new Date().getTime)

  protected def loggingHandler(m: String)(b: Try[Done])(a: ActorRef): Unit = {
    logger.debug(m)
  }

  protected def getFlowID(namekey: String) = ID(ID.FlowCategory, namekey)

  class StatisticCommander(flowID: ID) extends FlowCommander[TripAction, Statistic, (TripState.TripStateStatus, scala.collection.mutable.Map[String,Statistic])] {

    import StatisticDef._

    override val returnStateOnExceptions = true

    val Idle = "Idle"
    val Unladen = "Unladen"
    val Laden = "Laden"
    val FamilyPrefix = "CumulativeCarDrivingTime"


    val entityID =  flowID.findAncestor(Seq[ID](ID.EntityCategory), true).get


    override def initialState = (TripState.TripStateStatus.Undefined, mutable.Map[String, Statistic]() ++ (Set(Idle, Unladen, Laden).map(v =>{
      val now = getTimeNow
      v -> newHollowInstance(s"$v:$entityID").set(TimeFrom)(now).set(TimeTo)(now).set(Family)(s"$FamilyPrefix:$v")})))

    def handler(statistic: Statistic) = loggingHandler(s"Statistics calculator updated ${statistic.namekey} with ${statistic.get(CurrentValue)}")(_)

    protected def createNewSource(action: TripAction, statisticType: String, statistics: mutable.Map[String, Statistic]) = {
      val old = statistics(statisticType)
      val previous = statistics(statisticType match {
        case Idle => Laden
        case Unladen => Idle
        case Laden => Unladen
      })
      val delta = getTimeNow.millis - previous.get(TimeTo).millis
      val result = old.set(TimeTo)(getTimeNow).set(CurrentValue)(old.get(CurrentValue) + delta).bumpUpAuditIfApplicable
      statistics.update(statisticType, result)
      val nextState = action.get(action.definition.Status) match {
        case Receive => Driving
        case Deliver => Carrying
        case _ => Undefined
      }
      (true, (nextState, statistics), Some(Source.single(result), getFlowID(action.namekey), handler(result)))
    }

    override def apply(in: ((TripState.TripStateStatus, mutable.Map[String,Statistic]), Option[TripAction])) =
      if (in._2.isEmpty) {(true, in._1, Some(Source.fromIterator(() => in._1._2.values.iterator), ID(ID.FlowCategory),
        loggingHandler(s"Statistics calculator initialized statistics ${in._1._2.values.map(_.namekey)}")(_)
      ))} else {
        import TripState.TripStateStatus._
        import TripAction.TripActionStatus._
        val action = in._2.get
        val statistics = in._1._2
        (action.get(action.definition.Status), in._1._1) match {

          case (Receive, TripState.TripStateStatus.Undefined) => createNewSource(action, Idle, statistics)

          case (Deliver, Driving) => createNewSource(action, Unladen, statistics)

          case  (Finish, Carrying) => createNewSource(action, Laden, statistics)

          case  _ => (true, in._1, None)

        }
      }

  }


  protected val statisticFlowGenerator = new FlowGenerator[TripAction, Statistic]{

    def extractID(in: TripAction) = CarDef.id(TripActionDef.Car.getScalar(in))

    override def apply[In<:TripAction](flowID: ID)(implicit ct: ClassTag[In]) =
      Success(Flow[TripAction].idleTimeout(idleTimeout).via(StreamingFlowOps.getAdaptiveControlledStreamingFlow[TripAction, Statistic, (TripState.TripStateStatus, scala.collection.mutable.Map[String, Statistic])](
        dependency.streamingActorsRegistry, flowID, new StatisticCommander(flowID)).mapMaterializedValue(x => {
        x._1.onComplete(_ match {
          case Success(y) =>{
            import StatisticDef._
            val entityID = flowID.findAncestor(Seq[ID](ID.EntityCategory), true).get
            logger.debug(s"Finalized Car driving statistics for $entityID")
            val parentFlowActorID = StreamingFlowOps.getFlowActorID(getFlowID(statisticFlowName), true)
            y._2.values.map(z => z.set(TimeTo)(getTimeNow).set(Status)(Finalized).bumpUpAuditIfApplicable).foreach(s =>
              dependency.streamingActorsRegistry >> (Reg.Call[A,B](_ ! (entityID, s)), parentFlowActorID))
          }
          case _ => {}
        })
        NotUsed
      })))
  }

  protected lazy val statisticFlow = StreamingFlowOps.getRoutingCompositeFlow[TripAction, Statistic, TripAction, Statistic](dependency.streamingActorsRegistry, getFlowID(statisticFlowName),
    Left(FlowCodec.getIdentityCodec[TripAction, Statistic](statisticFlowGenerator.extractID(_))), statisticFlowGenerator)


  override protected def bootstrapFlows() = {
    import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._

    val f = Flow[EntityBusEvent].map(_.value.entity.get.asInstanceOf[TripAction]).via(statisticFlow)
      .map(x => Seq(EntityBusEvent(EntityOneOf(x), EventBusClassifiers(if (x.version == 1) INTERNAL_CREATE else INTERNAL_UPDATE, entityPredicate()))))


    val tripActionHydrator = {x: OneOf[TripAction] =>x match {
      case EntityOneOf(v) => Future.fromTry(Try{
        val z = v.hydrateFieldValue[Audit]._2.hydrateFieldValue[TripState]._2.hydrateFieldValue[Trip]._2.hydrateFieldValue[Car]._2
        val trip = z.hydrateFieldValue[Trip]._1.get.hydrateFieldValue[Audit]._2
        val car = z.hydrateFieldValue[Car]._1.get.hydrateFieldValue[Audit]._2
        val state = z.hydrateFieldValue[TripState]._1.get.hydrateFieldValue[Audit]._2.hydrateFieldValue[TripStatePosition]._2
        EntityOneOf(TripActionDef.TriggerState.setHydrated(TripActionDef.Car.setHydrated(TripActionDef.Trip.setHydrated(z, trip), car), state))
      })
      case _ => Future.successful(x)
    }}

    val tripStatePositionHydrator = {x: OneOf[TripStatePosition] =>x match {
      case EntityOneOf(v) => Future.fromTry(Try{
        val state = v.hydrateFieldValue[TripState]._1.get.hydrateFieldValue[Trip]._2
        EntityOneOf(TripStatePositionDef.TripState.setHydrated(v, state))
      })
      case _ => Future.successful(x)
    }}

    val carHydrator = {x: OneOf[Car] =>x match {
      case EntityOneOf(v) => Future.fromTry(Try{
        val state = v.hydrateFieldValue[TripState]._1.get.hydrateFieldValue[TripStatePosition]._2
        EntityOneOf(CarDef.LastKnownState.setHydrated(v, state))
      })
      case _ => Future.successful(x)
    }}

    // DB services
    (EntityDefinition.all().foldLeft[Boolean](true)((b, d) => b && addCoreService(d.tag))
    // routing services
    && addRoutingService[TripAction](DB_CREATED)(TCP_CREATED)(entityMapper = tripActionHydrator)
    && addRoutingService[TripStatePosition](TCP_CREATE)(MQ_CREATED, REST_CREATED)(entityMapper = tripStatePositionHydrator, waitPredicate = DB_CREATED ~ DB_FAILURE)
    && addRoutingService[Car](DB_CREATED)(REST_CREATED)(entityMapper = carHydrator)
    && addRoutingService[Car](DB_UPDATED)(REST_UPDATED)(entityMapper = carHydrator)
    && addRoutingService[Statistic](DB_UPDATED)(REST_UPDATED)()
    && addRoutingService[Location](DB_CREATED)(REST_CREATED)()
    // polling services
    && addPollingService[TripAction](500.millis)
    // analytic services
    && registerServiceFlow(dependency.addServiceFlow[TripAction](TCP_CREATED)(f), f))
  }

  ApplicationService.register[StreamingModule](this)

}
