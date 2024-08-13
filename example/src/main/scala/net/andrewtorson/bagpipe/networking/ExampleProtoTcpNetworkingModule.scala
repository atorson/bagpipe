package net.andrewtorson.bagpipe.networking

import akka.util.ByteString
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.entities.ExampleEntityMessageWrapper
import net.andrewtorson.bagpipe.utils._


/**
  * Created by Andrew Torson on 3/1/2017.
  */
trait ExampleProtoTcpNetworkingModule extends ProtoTcpNetworkingModule{
  override protected val defaultWrapper =  ExampleEntityMessageWrapper.defaultInstance

  override def deserialize(bs: ByteString) = ExampleEntityMessageWrapper.parseFrom(bs.toArray[Byte])
}

class ExampleTcpServerModuleProto(override protected val dependency: ConfigurationModule with ActorModule) extends ExampleProtoTcpNetworkingModule with TcpServerModule {

  ApplicationService.register[TcpServerModule](this)
}

class ExampleTcpClientModuleProto(override protected val dependency: ConfigurationModule with ActorModule) extends ExampleProtoTcpNetworkingModule with TcpClientModule {

  ApplicationService.register[TcpClientModule](this)
}
