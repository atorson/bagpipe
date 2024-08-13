package net.andrewtorson.bagpipe.utils

import akka.http.scaladsl.server.{Route, RouteConcatenation}
import net.andrewtorson.bagpipe.ApplicationService

/**
  * Created by Andrew Torson on 3/1/2017.
  */
class TestHttpRestfulServerModule (override protected val dependency: ConfigurationModule with ActorModule) extends HttpRestfulServerModuleProto(dependency) with RouteConcatenation with CorsSupport{

  override lazy val serverRoute = Route.handlerFlow({
    // Swagger service for JSON-over-HTTP web services
    val swaggerService = new SwaggerDocService(dependency)

    // JSON-over-HTTP service
    corsHandler(EntityDefinition.all().map(_.rest.routes).reduce(_ ~ _) ~ swaggerService.assets ~ swaggerService.routes)
  })

  ApplicationService.register[HttpRestfulServerModule](this)

}
