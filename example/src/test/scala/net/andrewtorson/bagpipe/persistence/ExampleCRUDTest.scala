/*
 *
 * Author: Andrew Torson
 * Date: Oct 28, 2016
 */

package net.andrewtorson.bagpipe.persistence

import java.util.Date

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.{global => ec}
import scala.concurrent.duration._
import akka.util.Timeout
import net.andrewtorson.bagpipe.{ApplicationService}
import net.andrewtorson.bagpipe.entities.Car.CarStatus
import net.andrewtorson.bagpipe.entities._
import net.andrewtorson.bagpipe.utils.{DOMTestData, PersistenceModule}
import org.scalatest.{BeforeAndAfterEach, FunSuite}



class ExampleCRUDTest extends FunSuite with AbstractPersistenceTest with BeforeAndAfterEach{
  implicit val timeout = Timeout(5.seconds)
  val modules = new Modules {}
  val data = DOMTestData

  ApplicationService[PersistenceModule].start

  test("Testing DOM CRUD") {
    val start = new Date(new Date().getTime - 1000)
    Await.result(TripStatePositionDef.crud.createOrUpdate(data.trip1position),5.seconds)
    assert (Await.result(LocationDef.crud.poll(start),5.seconds).size == 3, "Test locations were not polled")
    assert (Await.result(TripDef.crud.poll(start),5.seconds).size == 1, "Test trips were not polled")
    assert (Await.result(TripDef.crud.poll(new Date()),5.seconds).isEmpty, "Test trips polling did not work")
    val tripOpt : Option[Trip] = Await.result((TripDef.crud.read(data.trip1.namekey)),5.seconds)
    assert (!tripOpt.isEmpty, "Test trip was not created")
    val testTrip = tripOpt.get
    val auditOpt = testTrip.hydrateFieldValue[Audit]._1
    assert(!auditOpt.isEmpty && auditOpt.get.ver == 1, "Audit info was not properly read for a test trip")
    val loc1Opt = TripDef.Origin.getHydrated(testTrip)
    val loc2opt = TripDef.Destination.getHydrated(testTrip)
    val tripStateOpt = testTrip.hydrateFieldValue[TripState]._1
    assert(!tripStateOpt.isEmpty, "TripState info was not properly read for a test trip")
    val tripActionOpt = tripStateOpt.get.hydrateFieldValue[TripAction]._1
    assert(!tripActionOpt.isEmpty, "TripAction info was not properly read for a test trip")
    val carOpt = tripActionOpt.get.hydrateFieldValue[Car]._1
    assert(!carOpt.isEmpty, "Car info was not properly read for a test trip")
    val tripPositionOpt = tripStateOpt.get.hydrateFieldValue[TripStatePosition]._1
    assert(!tripPositionOpt.isEmpty && !tripPositionOpt.get.hydrateFieldValue[Location]._1.isEmpty, "TripPosition info was not properly read for a test trip")
    Await.result(CarDef.crud.createOrUpdate(carOpt.get.set(CarDef.Status)(CarStatus.Busy).bumpUpAuditIfApplicable),5.seconds)
    val carOpt2 = Await.result((CarDef.crud.read(data.car1.namekey)),5.seconds)
    assert (!carOpt2.isEmpty && (carOpt2.get.get(CarDef.Status) != data.car1.get(CarDef.Status)), "Update of the test data car status did not work")
    val auditOpt2 = carOpt2.get.hydrateFieldValue[Audit]._1
    assert(!auditOpt2.isEmpty && auditOpt2.get.ver == 2, "Audit info was not properly updated for a test car")
  }

  override protected def  afterEach(): Unit = {
    Await.result(TripStatePositionDef.crud.read(data.trip1position.namekey),5.seconds) match {
      case Some(jsp: TripStatePosition) =>  {
        Await.result(Future.sequence(Seq(
          TripStatePositionDef.crud.delete(jsp.namekey),
          LocationDef.crud.delete(data.loc0.namekey),
          LocationDef.crud.delete(data.loc1.namekey),
          LocationDef.crud.delete(data.loc2.namekey)
        )),5.seconds)
      }
      case _ => {}
    }
    ApplicationService[PersistenceModule].stop
    super.afterEach()
  }

}