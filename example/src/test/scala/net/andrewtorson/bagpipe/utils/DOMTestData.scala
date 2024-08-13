/*
 *
 * Author: Andrew Torson
 * Date: Jan 3, 2017
 */

package net.andrewtorson.bagpipe.utils

import net.andrewtorson.bagpipe.entities.TripAction.TripActionStatus
import net.andrewtorson.bagpipe.entities.TripState.TripStateStatus
import net.andrewtorson.bagpipe.entities.Car.CarStatus
import net.andrewtorson.bagpipe.entities._
import net.andrewtorson.bagpipe.networking.ED

import scala.reflect.ClassTag

/**
 * Created by Andrew Torson on 1/3/17.
 */

object DOMTestData extends TestData{
  val loc0 = LocationDef.newHollowInstance("00").set(LocationDef.X)(0).set(LocationDef.Y)(0)
  val loc1 = LocationDef.newHollowInstance("11").set(LocationDef.X)(1).set(LocationDef.Y)(1)
  val loc2 = LocationDef.newHollowInstance("22").set(LocationDef.X)(2).set(LocationDef.Y)(2)
  val car1 = CarDef.newHollowInstance("car1").set(CarDef.Status)(CarStatus.Idle).set(CarDef.LastKnownState)(KeyOneOf("trip1UndefinedState"))
  val trip1 = TripDef.newHollowInstance("trip1").set(TripDef.Origin)(EntityOneOf(loc1)).set(TripDef.Destination)(EntityOneOf(loc2)).
    set(TripDef.Status)(Trip.TripStatus.Planned).set(TripDef.CurrentState)(KeyOneOf("trip1UndefinedState"))
  val trip1action = TripActionDef.newHollowInstance("trip1UndefinedAction").set(TripActionDef.Trip)(EntityOneOf(trip1)).
    set(TripActionDef.Car)(EntityOneOf(car1)).set(TripActionDef.Status)(TripActionStatus.Undefined).
    set(TripActionDef.TriggerState)(KeyOneOf(trip1.get(TripDef.CurrentState).key))
  val trip1state = TripStateDef.newHollowInstance(trip1.get(TripDef.CurrentState).key).set(TripStateDef.Trip)(EntityOneOf(trip1)).
    set(TripStateDef.Car)(EntityOneOf(car1)).set(TripStateDef.Status)(TripStateStatus.Undefined).
    set(TripStateDef.TriggerAction)(EntityOneOf(trip1action)).set(TripStateDef.CurrentPosition)(KeyOneOf("trip1UndefinedStateLoc1"))
   val trip1position = TripStatePositionDef.newHollowInstance(trip1state.get(TripStateDef.CurrentPosition).key).set(TripStatePositionDef.CarLocation)(EntityOneOf(loc0)).
    set(TripStatePositionDef.TripState)(EntityOneOf(trip1state))

  def getTestEntity[E<:BaseEntity[E]](implicit ct: ClassTag[E]): E = {
    ct.runtimeClass match {
      case c if c == classOf[Location] => loc0.enforceAuditIfApplicable(true).asInstanceOf[E]
      case c if c == classOf[Car] => car1.enforceAuditIfApplicable(true).asInstanceOf[E]
      case c if c == classOf[Trip] => trip1.enforceAuditIfApplicable(true).asInstanceOf[E]
      case c if c == classOf[TripState] => trip1state.enforceAuditIfApplicable(true).asInstanceOf[E]
      case c if c == classOf[TripAction] => trip1action.enforceAuditIfApplicable(true).asInstanceOf[E]
      case c if c == classOf[TripStatePosition] => trip1position.enforceAuditIfApplicable(true).asInstanceOf[E]
      case _ => throw new IllegalArgumentException(s"Entity ${ct.runtimeClass} is not present in test data")
    }
  }

  override val testEntityDefinitions: Seq[ED] = Seq(AuditDef, LocationDef, CarDef, TripDef, TripStateDef, TripActionDef, TripStatePositionDef).map(_.asInstanceOf[ED])
}
