/*
 *
 * Author: Andrew Torson
 * Date: Feb 13, 2017
 */

package net.andrewtorson.bagpipe.streaming

import akka.actor.ActorRef
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{KillSwitches, ThrottleMode}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.LazyLogging
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.entities._
import net.andrewtorson.bagpipe.eventbus.{EntityBusEvent, EventBusClassifiers}
import net.andrewtorson.bagpipe.utils._

import scala.collection.immutable.Range
import scala.language.reflectiveCalls
import scala.reflect.ClassTag
import scala.util.{Success, Try}


/**
 * Created by Andrew Torson on 2/13/17.
 */


class PositionEmulatorExampleStreamingModule(override protected val dependency: ServiceBusModule with ActorModule) extends CoreStreamingModule with LazyLogging{

  import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._

  import scala.concurrent.duration._

  protected val mapSizeX = 10
  protected val mapSizeY = 10
  protected val fleetSize = 5

  protected val carCreationDelay = 8.seconds
  protected val startupDelay = 3.seconds
  protected val drivingOneTileTick = 1.seconds

  protected val emulatorFlowName = "EMEDJEF"

  protected val locationsMap = {
    import LocationDef._
    Map[ID, Location]() ++
      (for (x <- Range(0, mapSizeX); y <- Range(0, mapSizeY))
        yield Y.set(X.set(newHollowInstance(getLocationID(x,y).nameKey), x),y)).map(x => (x.id, x))
  }

  protected def getLocationID(x: Int, y: Int) = LocationDef.id(s"Loc:$x:$y")

  protected def getLocationID(namekey: String) = LocationDef.id(namekey)

  protected def getRandomLocation = {
    import scala.math._
    val x = max((random * mapSizeX).toInt -1, 0)
    val y = max((random * mapSizeY).toInt -1, 0)
    locationsMap(getLocationID(x,y))
  }

  protected def getInitialFleetSource = {
    import TripState.TripStateStatus.Completed
    import TripStateDef._
    import TripStatePositionDef._
    val fleet = for (x <- Range(0, fleetSize)) yield CarDef.newHollowInstance(s"Car#$x")
    Source.fromIterator(() => fleet.map(x => {
      val initialState = Trip.setScalar(TriggerAction.setScalar(Car.setScalar(Status.set(TripStateDef.newHollowInstance(s"${x.namekey}:${Completed.name}"), Completed), x.namekey), "NA"), "NA")
      val location = getRandomLocation
      val position = TripState.setScalar(CarLocation.setScalar(TripStatePositionDef.newHollowInstance(s"${initialState.namekey}:${location.namekey}"),
        location.namekey), initialState.namekey)
      CarDef.LastKnownState.setHydrated(x, CurrentPosition.setHydrated(initialState, position))
    }).toIterator).initialDelay(carCreationDelay)
  }

  protected def loggingHandler(m: String)(b: Try[Done])(a: ActorRef): Unit = {
    logger.debug(m)
  }
  protected def terminatingHandler(m: String, flowID: ID)(b: Try[Done])(a: ActorRef): Unit = {
    logger.debug(m)
    dependency.streamingActorsRegistry -- (Reg.Remove[A,B], StreamingFlowOps.getFlowActorID(flowID,true), true)
  }

  protected def getFlowID(namekey: String) = ID(ID.FlowCategory, namekey)


  class EmulatorCommander(flowID: ID) extends FlowCommander[TripAction, TripStatePosition, TripState.TripStateStatus] {


    override val isSwitching = true

    override def initialState = TripState.TripStateStatus.Undefined

    def handler(state: TripState) = {
      val message = s"Emulator started emulating a stream of positions in the TripState[${state.namekey}]"
      TripStateDef.Status.get(state) match {
        case TripState.TripStateStatus.Completed => terminatingHandler(message, flowID)(_)
        case _ => loggingHandler(message)(_)
      }

    }

    def generateNextValue(from: Int, to: Int) = {
      if (from < to) (from + 1) else (from -1)
    }

    def getNewDrivingSource(from: Location, to: Location, state: TripState, continue: Boolean = true) = {
      import TripStatePositionDef._
      import LocationDef._
      var dynamicState: TripState = state
      (true, TripStateDef.Status.get(state), Some(Source.unfold[Option[Location], Location](Some(from))(_ match{
        case Some(loc) =>
          if (loc.id == to.id) {
            Some(None, loc)
          } else {
            if (loc.get(X) == to.get(X)){
              // move in Y direction
              Some(Some(locationsMap(getLocationID(loc.get(X), generateNextValue(loc.get(Y), to.get(Y))))), loc)
            } else {
              // move in X direction
              Some(Some(locationsMap(getLocationID(generateNextValue(loc.get(X), to.get(X)), loc.get(Y)))), loc)
            }
          }
        case _ => None
      }).throttle(1, drivingOneTileTick, 0, ThrottleMode.shaping).map( x => {
         val newPositionNamekey = s"${state.namekey}:${x.namekey}"
         dynamicState = TripStateDef.CurrentPosition.setScalar(dynamicState, newPositionNamekey)
         val newPosition = TripState.setHydrated(CarLocation.setScalar(TripStatePositionDef.newHollowInstance(newPositionNamekey), x.namekey), dynamicState)
         if (dynamicState.version == 1) {
           dynamicState = dynamicState.dehydrateFieldValue[Car]._2.bumpUpAuditIfApplicable
         } else {
           dynamicState = dynamicState.bumpUpAuditIfApplicable
         }
         newPosition
      }), getFlowID(state.namekey), handler(state)))
    }

    def getState(action: TripAction) = action.hydrateFieldValue[TripState]._1.get

    def getTrip(action: TripAction) = action.hydrateFieldValue[Trip]._1.get

    def getTripID(action: TripAction) = TripDef.id(TripActionDef.Trip.getScalar(action))

    def getCar(action: TripAction) = action.hydrateFieldValue[Car]._1.get

    def getPosition(action: TripAction) = getState(action).hydrateFieldValue[TripStatePosition]._1.get

    def generateNewState(action: TripAction, status: TripState.TripStateStatus) = {
      import TripStateDef._
      import net.andrewtorson.bagpipe.entities.Trip.TripStatus._
      import net.andrewtorson.bagpipe.entities.Car.CarStatus._
      val carStatus = status match{
        case TripState.TripStateStatus.Completed => Idle
        case _ => Busy
      }
      val tripStatus = status match{
        case TripState.TripStateStatus.Completed => Completed
        case _ => Underway
      }
      val newStateNamekey = s"${getTripID(action).nameKey}:${status.name}"
      val trip = TripDef.CurrentState.setScalar(TripDef.Status.set(getTrip(action).bumpUpAuditIfApplicable, tripStatus), newStateNamekey)
      val car = CarDef.LastKnownState.setScalar(CarDef.Status.set(getCar(action).bumpUpAuditIfApplicable, carStatus), newStateNamekey)
      TriggerAction.setScalar(Trip.setHydrated(Car.setHydrated(Status.set(newHollowInstance(newStateNamekey),status),car),trip), action.namekey)
    }

    override def apply(in: (TripState.TripStateStatus, Option[TripAction])) =
      if (in._2.isEmpty) {(true, in._1, None)} else {
        import TripAction.TripActionStatus._
        import TripState.TripStateStatus._
        val action = in._2.get
        (action.get(action.definition.Status), in._1) match {

          case (Receive, TripState.TripStateStatus.Undefined) => {
            getNewDrivingSource(
              locationsMap(getLocationID(TripStatePositionDef.CarLocation.getScalar(getPosition(action)))),
              locationsMap(getLocationID(TripDef.Origin.getScalar(getTrip(action)))),
              generateNewState(action, Driving))
          }

          case  (Deliver, Driving) => {
            getNewDrivingSource(
              locationsMap(getLocationID(TripDef.Origin.getScalar(getTrip(action)))),
              locationsMap(getLocationID(TripDef.Destination.getScalar(getTrip(action)))),
              generateNewState(action, Carrying))
          }

          case  (Finish, Carrying) => {
            getNewDrivingSource(
              locationsMap(getLocationID(TripDef.Destination.getScalar(getTrip(action)))),
              locationsMap(getLocationID(TripDef.Destination.getScalar(getTrip(action)))),
              generateNewState(action, Completed), false)
          }

          case  _ => (true, in._1, None)

        }
      }

  }


  protected val emulatorFlowGenerator = new FlowGenerator[TripAction, TripStatePosition]{

    def extractID(in: TripAction) = TripDef.id(TripActionDef.Trip.getScalar(in))

    override def apply[In<:TripAction](flowID: ID)(implicit ct: ClassTag[In]) =
      Success(StreamingFlowOps.getAdaptiveControlledStreamingFlow[TripAction, TripStatePosition, TripState.TripStateStatus](
        dependency.streamingActorsRegistry, flowID, new EmulatorCommander(flowID)).mapMaterializedValue(x => {
        x._1.onComplete(_ =>{
          val entityID = flowID.findAncestor(Seq[ID](ID.EntityCategory), true).get
          logger.debug(s"Completed EM Trip Emulator flow for $entityID")
        })
        NotUsed
      }))
  }

  protected lazy val emulatorFlow = StreamingFlowOps.getRoutingCompositeFlow[TripAction, TripStatePosition, TripAction, TripStatePosition](dependency.streamingActorsRegistry, getFlowID(emulatorFlowName),
    Left(FlowCodec.getIdentityCodec[TripAction, TripStatePosition](emulatorFlowGenerator.extractID(_))), emulatorFlowGenerator)

  override protected def bootstrapFlows() = {

    val f0 = Source.fromIterator(() => locationsMap.valuesIterator).initialDelay(startupDelay)
    .map(x => EntityBusEvent(EntityOneOf(x), EventBusClassifiers(TCP_CREATED, entityPredicate())))
    .viaMat(KillSwitches.single)(Keep.right).to(Sink.foreach(dependency.deliver(_)))

    val f1 = getInitialFleetSource.map(x => EntityBusEvent(EntityOneOf(x), EventBusClassifiers(TCP_CREATED, entityPredicate())))
      .viaMat(KillSwitches.single)(Keep.right).to(Sink.foreach(dependency.deliver(_)))

    val f2 = Flow[EntityBusEvent].map(_.value.entity.get.asInstanceOf[TripAction]).via(emulatorFlow)
      .map(x => Seq(EntityBusEvent(EntityOneOf(x), EventBusClassifiers(TCP_CREATED, entityPredicate()))))

    (registerCustomService(getFlowID("EMEDLGF"), f0)
    && registerCustomService(getFlowID("EMEDRGF"), f1)
    && registerServiceFlow(dependency.addServiceFlow[TripAction](TCP_CREATE)(f2), f2))
  }

  ApplicationService.register[StreamingModule](this)
}
