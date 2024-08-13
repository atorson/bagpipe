package net.andrewtorson.bagpipe

import com.typesafe.scalalogging.LazyLogging
import net.andrewtorson.bagpipe.entities._
import net.andrewtorson.bagpipe.utils.{BagpipeStreamingModule, _}

/**
  * Created by Andrew Torson on 3/1/2017.
  */
object BagpipeTestBoot extends App with LazyLogging{

  //ToDO: need to re-factor to use Guice injection here. By default, Scala singletons are lazyly loaded at the first explicit code reference
  val modules = new TestConfigurationModule with ActorModuleImpl

  launchServices

  private def launchServices = {
    for (x <- CoreTestData.testEntityDefinitions) {
      logger.info(s"Loaded definition of entity[${x.tag}]")
    }
    //ToDO: need to re-factor to use Guice injection below. By default, Scala singletons are lazyly loaded at the first explicit code reference
    new PersistenceModuleImpl(modules)
    new BagpipeStreamingModule(new ServiceBusModuleImpl(modules))
    new TestHttpRestfulServerModule(modules)
    new TcpServerModuleProto(modules)
    new BagpipeMessagingModule(modules)

    ApplicationService.start
  }

}
