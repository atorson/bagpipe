/*
 *
 * Author: Andrew Torson
 * Date: Aug 8, 2016
 */

package net.andrewtorson.bagpipe.utils

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route}
import akka.http.scaladsl.marshalling.ToResponseMarshallable.apply
import akka.http.scaladsl.model.StatusCode.int2StatusCode
import akka.http.scaladsl.server.Directive.addByNameNullaryApply

//see https://groups.google.com/forum/#!topic/akka-user/5RCZIJt7jHo
trait CorsSupport {

  //this directive adds access control headers to normal responses
  private def addAccessControlHeaders: Directive0 = {
    mapResponseHeaders { headers =>
      `Access-Control-Allow-Origin`.* +:
        `Access-Control-Allow-Credentials`(true) +:
        `Access-Control-Allow-Headers`("Authorization", "Content-Type", "Origin",
          "X-Atmosphere-tracking-id", "X-Atmosphere-Framework", "X-Cache-Date", "X-Atmosphere-Transport", "X-Requested-With", "*") +:
      headers
    }
  }

  //this handles preflight OPTIONS requests. TODO: see if can be done with rejection handler,
  //otherwise has to be under addAccessControlHeaders
  private def preflightRequestHandler: Route = options {
    complete(HttpResponse(200).withHeaders(
      `Access-Control-Allow-Methods`(OPTIONS, POST, PUT, GET, DELETE)
    )
    )
  }

  def corsHandler(r: Route) = addAccessControlHeaders {
    preflightRequestHandler ~ r
  }
}