/*
 *
 * Author: Andrew Torson
 * Date: Feb 24, 2017
 */

package net.andrewtorson.bagpipe.rest

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directives, Route}
import net.andrewtorson.bagpipe.ApplicationService
import net.andrewtorson.bagpipe.utils._


/**
 * Created by Andrew Torson on 2/24/17.
 */
class ExampleHttpRestfulServerModule(override protected val dependency: ConfigurationModule with ActorModule) extends HttpRestfulServerModuleProto(dependency) with Directives with CorsSupport{

  protected def assets = pathPrefix("demo") {
    getFromResourceDirectory("bagpipedemo") ~ pathSingleSlash(get(redirect("index.html", StatusCodes.PermanentRedirect))) }

  override lazy val serverRoute = Route.handlerFlow({
    // Swagger service for JSON-over-HTTP web services
    val swaggerService = new SwaggerDocService(dependency)

    // JSON-over-HTTP service
    corsHandler(EntityDefinition.all().map(_.rest.routes).reduce(_ ~ _) ~ swaggerService.assets ~ this.assets ~ swaggerService.routes)
  })

  ApplicationService.register[HttpRestfulServerModule](this)

}