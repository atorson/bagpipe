/*
 *
 * Author: Andrew Torson
 * Date: Aug 26, 2016
 */

package net.andrewtorson.bagpipe.utils

import java.net.InetSocketAddress

import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Success, Try}
import akka.{Done, NotUsed}
import akka.stream._
import akka.stream.scaladsl.{BidiFlow, Flow, Framing, Keep, Sink, Tcp}
import akka.stream.scaladsl.Tcp.ServerBinding
import akka.util.ByteString
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.networking.{BE, _}

import scala.language.existentials
import net.andrewtorson.bagpipe.entities.EntityMessageWrapper
import net.andrewtorson.bagpipe.streaming.FlowCodec


trait TcpNetworkingModule extends ActorBasedNetworkingModule{

  override val protocol = IO.TCP_PROTOCOL

  protected def deserialize (bs: ByteString): I

  protected def serialize (v: O): ByteString

  protected val serializationProtocol: BidiFlow[ByteString, I, O, ByteString, NotUsed] =
    BidiFlow.fromFlows(Framing.simpleFramingProtocolDecoder(10000).via(Flow.fromFunction[ByteString, I](deserialize(_))),
      Flow.fromFunction[O, ByteString](serialize(_)).via(Framing.simpleFramingProtocolEncoder(10000)))

}


trait TcpServerModule extends TcpNetworkingModule with ApplicationService[TcpServerModule] {

  import scala.concurrent.duration._

  override type Repr = Any

  override val tag =  ClassTag[TcpServerModule](classOf[TcpServerModule])

  override val abnormalTerminationCode = 6

  protected var innerBinding: Option[ServerBinding] = None

  protected val localHost = dependency.config.getString("networking.tcp.localHost")
  protected val localPort = dependency.config.getInt("networking.tcp.localPort")


  override def isRunning = innerBinding.isDefined

  override def start = if (!isRunning) this.synchronized{
    Try{
      Await.result(
        Tcp().bind(localHost, localPort).toMat(Sink.foreach(x => {
            val connectionID = NetworkingModule.getConnectionFlowID(x.remoteAddress, x.localAddress, true, protocol)
            x.flow.join(serializationProtocol).join(flow(connectionID)).run()})
        )(Keep.left).run()
      , 10.seconds)
    } match {
      case Success(x) => {
        innerBinding = Some(x)
        true
      }
      case _ => false
    }
  } else {false}


  override def stop: Boolean = if (isRunning) this.synchronized{
    Try{
      Await.result(innerBinding.get.unbind(), 10.seconds)
    } match {
      case Success(_) => {
        innerBinding = None
        true
      }
      case _ => false
    }
  } else {false}

}

trait TcpClientModule extends TcpNetworkingModule with ApplicationService[TcpClientModule]{

  import scala.concurrent.duration._

  override type Repr = Future[Done]

  override val tag =  ClassTag[TcpClientModule](classOf[TcpClientModule])

  override val abnormalTerminationCode = 7

  protected var innerBinding: Option[UniqueKillSwitch] = None

  protected val remoteHost = dependency.config.getString("networking.tcp.remoteHost")
  protected val remotePort = dependency.config.getInt("networking.tcp.remotePort")
  protected val localHost = dependency.config.getString("networking.tcp.localHost")

  override def isRunning = innerBinding.isDefined

  override def start = if (!isRunning) this.synchronized{
    Try{
      val remoteAddress = new InetSocketAddress(remoteHost, remotePort)
      val localAddress = new InetSocketAddress(localHost, 0)
      val connectionID = NetworkingModule.getConnectionFlowID(localAddress, remoteAddress, false, protocol)
      val x = Tcp()
        .outgoingConnection(remoteHost, remotePort).viaMat(KillSwitches.single)(Keep.both)
        .join(serializationProtocol).joinMat(flow(connectionID))(Keep.both).run()
      x._2.onComplete({
        _ => stop
      })(actorSystem.dispatcher)
      Await.ready(x._1._1, 10.seconds)
      x._1._2
    } match {
      case Success(x: UniqueKillSwitch) => {
        innerBinding = Some(x)
        true
      }
      case _ => false
    }
  } else {false}


  override def stop: Boolean = if (isRunning) this.synchronized{
    Try{
      innerBinding.get.shutdown()
    } match {
      case Success(_) => {
        innerBinding = None
        true
      }
      case _ => false
    }
  } else {false}

}



trait ProtoTcpNetworkingModule extends EntityWiseNetworkingModule with TcpNetworkingModule{

  override type I = WrappedEntity
  override type O = WrappedEntity

  override implicit val itag = ClassTag[WrappedEntity](classOf[WrappedEntity])
  override implicit val otag = ClassTag[WrappedEntity](classOf[WrappedEntity])

  protected val defaultWrapper: WrappedEntity = EntityMessageWrapper.defaultInstance

  override val codec = Left(new FlowCodec[WrappedEntity, WrappedEntity, BE, BE]{

    override def encode[E<:BE](value: (ID, E)) =  defaultWrapper.setEntity(value._2)

    override def decode(value: I) = {
      val result = value.getEntity()
      (result.definition.categoryID, result)
    }
  })

  def deserialize (bs: ByteString): I  = {
    EntityMessageWrapper.parseFrom(bs.toArray[Byte])
  }


  def serialize(v: O) = {
    ByteString.fromArray(v.toByteArray)
  }


  override protected def transformWarmupEntity(warmUpEntity: EntityRepr) = Future{defaultWrapper.setEntity(warmUpEntity)}

}

class TcpServerModuleProto(override protected val dependency: ConfigurationModule with ActorModule) extends ProtoTcpNetworkingModule with TcpServerModule {

  ApplicationService.register[TcpServerModule](this)
}

class TcpClientModuleProto(override protected val dependency: ConfigurationModule with ActorModule) extends ProtoTcpNetworkingModule with TcpClientModule {

  ApplicationService.register[TcpClientModule](this)
}



