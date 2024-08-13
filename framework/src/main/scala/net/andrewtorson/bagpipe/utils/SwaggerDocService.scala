/*
 *
 * Author: Andrew Torson
 * Date: Aug 8, 2016
 */

package net.andrewtorson.bagpipe.utils

import scala.reflect.runtime.{universe => ru}

import akka.http.scaladsl.model.{StatusCodes}
import com.github.swagger.akka._
import com.github.swagger.akka.model.Info
import io.swagger.models.Swagger
import akka.actor.ActorSystem
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.github.swagger.akka.SwaggerHttpService._
import io.swagger.util.Json
import akka.http.scaladsl.marshalling._
import scala.collection.JavaConverters._



class SwaggerDocService(dependency: ConfigurationModule with ActorModule) extends SwaggerHttpService with HasActorSystem {
  implicit val jackson: ToEntityMarshaller[Swagger] = Marshaller.stringMarshaller(`application/json`).compose(Json.pretty().writeValueAsString(_))
  implicit val swaggerMarhsaller: ToResponseMarshaller[Swagger] = PredefinedToResponseMarshallers.fromToEntityMarshaller[Swagger](StatusCodes.OK, Nil)
  override implicit val actorSystem: ActorSystem = dependency.system
  override implicit val materializer: ActorMaterializer = ActorMaterializer()
  override val apiTypes = {
    val m = ru.runtimeMirror(ru.getClass.getClassLoader)
    EntityDefinition.all().toSeq.map(x => m.staticClass(x.rest.getClass.getName).selfType)
  }

  override val host = s"${dependency.config.getString("networking.http.localHost")}:${dependency.config.getInt("networking.http.localPort")}"
  override val info = Info(version = "2.0")
  override lazy val routes: Route = get {
    path(apiDocsBase / "swagger.json") {
      get {
        complete(reader.read(toJavaTypeSet(apiTypes).asJava))
      }
    }
  }
  def assets = pathPrefix("swagger") {
    getFromResourceDirectory("swagger") ~ pathSingleSlash(get(redirect("index.html", StatusCodes.PermanentRedirect))) }
}
