/*
 *
 * Author: Andrew Torson
 * Date: Feb 8, 2017
 */

package net.andrewtorson.bagpipe.streaming
import akka.Done
import akka.stream.scaladsl.Flow
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.eventbus.{EntityBusEvent, EventBusClassifiers}
import net.andrewtorson.bagpipe.utils._
import net.andrewtorson.bagpipe.entities.{TripStatePosition, TripStatePositionDef, LocationDef}
import net.andrewtorson.bagpipe.eventbus.EventBusClassifiers._
import net.andrewtorson.bagpipe.networking.{ExampleTcpClientModuleProto, ExampleTcpServerModuleProto}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._


/**
 * Created by Andrew Torson on 2/8/17.
 */
class ExampleFusedStreamingServicesTest extends FusedStreamingServicesTest{

  override val data = DOMTestData
  override val triggerEvent = EntityBusEvent(EntityOneOf(data.trip1position), EventBusClassifiers(REST_CREATED, entityPredicate()))

  class ExampleFusedTestTcpClient(override protected val dependency: ConfigurationModule with ActorModule) extends ExampleTcpClientModuleProto(dependency){

    override  protected def warmUpFlow(g: Flow[I, O, Future[Done]]) = g

    ApplicationService.register[TcpClientModule](this)
  }

  override val tcpServer = new ExampleTcpServerModuleProto(modules)
  override val tcpClient = new ExampleFusedTestTcpClient(modules)


  override protected def deleteTestData = {
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
  }

}
