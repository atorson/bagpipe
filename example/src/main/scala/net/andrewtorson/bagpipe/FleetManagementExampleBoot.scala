package net.andrewtorson.bagpipe

import com.typesafe.scalalogging.LazyLogging
import net.andrewtorson.bagpipe.entities._
import net.andrewtorson.bagpipe.streaming.FleetManagementExampleStreamingModule
import net.andrewtorson.bagpipe.utils._

/**
  * Created by Andrew Torson on 3/3/2017.
  */
object FleetManagementExampleBoot extends App with LazyLogging{

  //ToDO: need to re-factor to use Guice injection here. By default, Scala singletons are lazyly loaded at the first explicit code reference
  private val entityDefinitions = Seq[EntityDefinition[_]](AuditDef, StatisticDef, TripDef, CarDef, LocationDef, TripStateDef, TripActionDef, TripStatePositionDef)
  val modules = new ConfigurationModuleImpl with ActorModuleImpl

  launchServices

  private def launchServices = {
    for (x <- entityDefinitions) {
      logger.info(s"Loaded definition of entity[${x.tag}]")
    }
    //ToDO: need to re-factor to use Guice injection below. By default, Scala singletons are lazyly loaded at the first explicit code reference
    new PersistenceModuleImpl(modules)
    new FleetManagementExampleStreamingModule(new ServiceBusModuleImpl(modules))
    new HttpRestfulClientModuleProto(modules)
    new ExternalPeerMessagingModuleImpl(modules)

    ApplicationService.start
  }

}
