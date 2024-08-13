/*
 *
 * Author: Andrew Torson
 * Date: Feb 13, 2017
 */

package net.andrewtorson.bagpipe.streaming

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.LazyLogging
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.entities.{Car, _}
import net.andrewtorson.bagpipe.eventbus.{EntityBusEvent, EventBusClassifiers}
import net.andrewtorson.bagpipe.utils.{ID, _}

import scala.language.reflectiveCalls
import scala.reflect.ClassTag
import scala.util.{Success, Try}

/**
 * Created by Andrew Torson on 2/13/17.
 */
class FleetManagementExampleStreamingModule(override protected val dependency: ServiceBusModule with ActorModule) extends CoreStreamingModule with LazyLogging{

  import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._

  import scala.concurrent.duration._

  protected val nonDrivingOperationDuration = 5.seconds

  protected val locationsMap = new ConcurrentMapSimpleRegistry[Location](Reg.cbBuilder[Location].build())

  protected def loggingHandler(m: String)(b: Try[Done])(a: ActorRef): Unit = {
    logger.debug(m)
  }

  protected def getFlowID(namekey: String) = ID(ID.FlowCategory, namekey)

  protected val tripCounter = new AtomicInteger(0)
  protected val locationCounter = new AtomicInteger(0)


  protected lazy val orchestratorFlow = StreamingFlowOps.getRoutingCompositeFlow[TripStatePosition, TripAction, TripStatePosition, TripAction](dependency.streamingActorsRegistry, getFlowID("CSEDJOF"),
    Left(FlowCodec.getIdentityCodec[TripStatePosition, TripAction](orchestratorFlowGenerator.orchestratorCommander.extractID(_))), orchestratorFlowGenerator)

  protected lazy val schedulerFlow = StreamingFlowOps.getRoutingCompositeFlow[Car, (Trip,TripStatePosition), Car, (Trip,TripStatePosition)](dependency.streamingActorsRegistry, getFlowID("CSEDJSF"),
    Left(FlowCodec.getIdentityCodec[Car, (Trip,TripStatePosition)](schedulerFlowGenerator.schedulerCommander.extractID(_))), schedulerFlowGenerator)

  class SchedulerCommander extends FlowCommander[Car, (Trip,TripStatePosition), Car.CarStatus] {

    import TripDef._
    import TripState.TripStateStatus.Assigned
    import Car.CarStatus._

    override def initialState = Idle

    def extractID(in: Car) = in.id

    def handler(trip: Trip) = loggingHandler(s"Scheduler has generated Trip[${trip.namekey}]")(_)

    def getNewSource(car: Car) = {
      import scala.math._
      val size = locationCounter.get()
      // random origin and destination
      if (size > 0) {
        val origin = (locationsMap ? (Reg.Get[Location], generateLinearLocationID(max((random*size).toInt -1,0)))).get.get
        val destination =  (locationsMap ? (Reg.Get[Location], generateLinearLocationID(max((random*size).toInt -1,0)))).get.get
        val trip = Destination.setScalar(Origin.setScalar(newHollowInstance(s"Trip#${tripCounter.incrementAndGet()}"),
          origin.namekey), destination.namekey)
        val stateKey = s"${trip.namekey}:${Assigned.name}"
        val action = TripActionDef.TriggerState.setScalar(TripActionDef.Trip.setScalar(TripActionDef.Car.setScalar(TripActionDef.newHollowInstance(s"${trip.namekey}:${TripAction.TripActionStatus.Undefined.name}"),
          car.namekey), trip.namekey), stateKey)
        val lastLocation = TripStatePositionDef.CarLocation.getScalar(getLastStatePosition(car))
        val position = TripStatePositionDef.CarLocation.setScalar(TripStatePositionDef.TripState.setScalar(
          TripStatePositionDef.newHollowInstance(s"$stateKey:${lastLocation}"), stateKey), lastLocation)
        val state = TripStateDef.TriggerAction.setHydrated(TripStateDef.CurrentPosition.setHydrated(
          TripStateDef.Status.set(TripStateDef.Car.setScalar(TripStateDef.Trip.setScalar(TripStateDef.newHollowInstance(stateKey), trip.namekey),car.namekey), Assigned),
          position), action)
        val tripFinal = CurrentState.setHydrated(trip,state)
        val positionFinal = TripStatePositionDef.TripState.setHydrated(position,TripStateDef.Trip.setHydrated(state,tripFinal))

        (true, Busy, Some(Source.single((tripFinal, positionFinal)), getFlowID(trip.namekey), handler(tripFinal)))
      } else (true, Idle, None)
    }

    def getLastStatePosition(car: Car) = (car.hydrateFieldValue[TripState]._1.get).hydrateFieldValue[TripStatePosition]._1.get

    override def apply(in: (Car.CarStatus, Option[Car])) =
      if (in._2.isEmpty) {(true, in._1, None)} else {

        (in._1, in._2.get.get(CarDef.Status)) match {

          case (Idle, Idle) => {
            getNewSource(in._2.get)
          }

          case  (_, Busy) => {
            (true, Idle, None)
          }

          case  _ => (true, in._1, None)

        }
      }

  }


  class OrchestratorCommander extends FlowCommander[TripStatePosition, TripAction, TripAction.TripActionStatus] {


    import TripAction.TripActionStatus._
    import TripActionDef._
    import TripState.TripStateStatus._

    override def initialState = TripAction.TripActionStatus.Undefined

    def extractID(in: TripStatePosition) = getTripID(getState(in))

    def handler(action: TripAction) = loggingHandler(s"Orchestrator has ordered TripAction[${action.namekey}]")(_)

    def getNewSource(state: TripState, newStatus: TripAction.TripActionStatus, delay: FiniteDuration) = {
      val newAction = TriggerState.setScalar(Status.set(TripActionDef.Trip.setScalar(TripActionDef.Car.setScalar(TripActionDef.newHollowInstance(s"${getTripID(state).nameKey}:${newStatus.name}"),
        state.definition.Car.getScalar(state)), state.definition.Trip.getScalar(state)), newStatus), state.namekey)
      (true, Status.get(newAction), Some(Source.single(newAction).initialDelay(delay), getFlowID(newAction.namekey), handler(newAction)))
    }

    def getState(position: TripStatePosition) = position.hydrateFieldValue[TripState]._1.get

    def getTrip(state: TripState) = state.hydrateFieldValue[Trip]._1.get

    def getTripID(state: TripState) = TripDef.id(TripStateDef.Trip.getScalar(state))

    override def apply(in: (TripAction.TripActionStatus, Option[TripStatePosition])) =
      if (in._2.isEmpty) {(true, in._1, None)} else {
         val position = in._2.get
         val state = getState(position)
         (state.get(state.definition.Status), in._1) match {

           case (Assigned, TripAction.TripActionStatus.Undefined) => {
             getNewSource(state, Receive, nonDrivingOperationDuration)
           }

           case  (Driving, Receive) => {
            val trip = getTrip(state)
            if (trip.get(trip.definition.Origin).key == position.get(position.definition.CarLocation).key) {
              getNewSource(state, Deliver, nonDrivingOperationDuration)
            } else {
              (true, in._1,None)
            }
          }

          case  (Carrying, Deliver) => {
            val trip = getTrip(state)
            if (trip.get(trip.definition.Destination).key == position.get(position.definition.CarLocation).key) {
              getNewSource(state, Finish, nonDrivingOperationDuration)
            } else {
              (true, in._1, None)
            }
          }

          case  (Completed, Finish) => {
            (false, in._1, None)
          }

          case  _ => (true, in._1, None)

        }
      }

  }


  protected val orchestratorFlowGenerator = new FlowGenerator[TripStatePosition, TripAction]{

    val orchestratorCommander = new OrchestratorCommander

    override def apply[In<:TripStatePosition](flowID: ID)(implicit ct: ClassTag[In]) =
      Success(StreamingFlowOps.getAdaptiveControlledStreamingFlow[TripStatePosition, TripAction, TripAction.TripActionStatus](
        dependency.streamingActorsRegistry, flowID, orchestratorCommander).mapMaterializedValue(x => {
        x._1.onComplete(_ =>{
          val entityID = flowID.findAncestor(Seq[ID](ID.EntityCategory), true).get
          logger.debug(s"Completed CS Trip Orchestrator flow for $entityID")
        })
        NotUsed
      }))
  }

  protected val schedulerFlowGenerator = new FlowGenerator[Car, (Trip, TripStatePosition)]{

    val schedulerCommander = new SchedulerCommander

    override def apply[In<:Car](flowID: ID)(implicit ct: ClassTag[In]) =
      Success(StreamingFlowOps.getAdaptiveControlledStreamingFlow[Car, (Trip, TripStatePosition), Car.CarStatus](
        dependency.streamingActorsRegistry, flowID, schedulerCommander).mapMaterializedValue(x => {
        x._1.onComplete(_ =>{
          val entityID = flowID.findAncestor(Seq[ID](ID.EntityCategory), true).get
          logger.debug(s"Completed CS Trip Scheduler flow for $entityID")
        })
        NotUsed
      }))
  }


  protected val linearLocationCat = ID(LocationDef.categoryID, "LIN")

  protected def generateLinearLocationID(index: Int) = ID(linearLocationCat, index)

  override protected def bootstrapFlows() = {


   val s0 = dependency.source(topic(REST_CREATE)(LocationDef.categoryID):_*)(false, true)
   val id0 = s0._1
   val f0 = s0._2.viaMat(KillSwitches.single)(Keep.right).to(
        Sink.foreach(x => if (x.value.isHydrated){
          val loc = x.value.entity.get.asInstanceOf[Location]
          locationsMap ? (Reg.AddIfAbsent[Location], loc.id, Some(() => Some(loc))) match {
            case Success(Some(_)) => locationsMap ++ (Reg.AddIfAbsent[Location], generateLinearLocationID(locationCounter.getAndIncrement()), loc)
            case _ => {}
          }
        }))

    val f1 = Flow[EntityBusEvent].map(_.value.entity.get.asInstanceOf[Car]).via(schedulerFlow)
      .map(x => Seq(EntityBusEvent(EntityOneOf(x._1), EventBusClassifiers(REST_CREATED, entityPredicate())),
        // short-cut to avoid sending initial trip state position via real MQ interface.  This is not the position that EM emulates anyway
        EntityBusEvent(EntityOneOf(x._2),EventBusClassifiers(MQ_CREATE, entityPredicate()))))

    val f2 = Flow[EntityBusEvent].map(_.value.entity.get.asInstanceOf[TripStatePosition]).via(orchestratorFlow)
        .map(x => Seq(EntityBusEvent(EntityOneOf(x), EventBusClassifiers(INTERNAL_CREATE, entityPredicate()))))

    (addCoreService[TripAction]
     && registerCustomService(id0, f0)
     && registerServiceFlow(dependency.addServiceFlow[Car](REST_CREATE, REST_UPDATE)(f1), f1)
     && registerServiceFlow(dependency.addServiceFlow[TripStatePosition](MQ_CREATE)(f2), f2))
  }

  ApplicationService.register[StreamingModule](this)

}
