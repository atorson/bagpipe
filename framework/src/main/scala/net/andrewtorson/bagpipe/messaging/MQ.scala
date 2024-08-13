/*
 *
 * Author: Andrew Torson
 * Date: Feb 6, 2017
 */

package net.andrewtorson.bagpipe.messaging

import scala.reflect.ClassTag

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import com.google.common.net.MediaType
import net.andrewtorson.bagpipe.networking.IO
import net.andrewtorson.bagpipe.{ApplicationService, ApplicationServiceUnavailableException}
import net.andrewtorson.bagpipe.rest.ProtoJsonSupport
import net.andrewtorson.bagpipe.utils._
import io.scalac.amqp._
import scala.concurrent.duration._

/**
 * Created by Andrew Torson on 2/6/17.
 */

object MQ {

  val inboundExchange = Exchange("bagpipe.inbound", Direct, durable = true)

  val outboundExchange = Exchange("bagpipe.outbound", Direct, durable = true)

  def getRoutingKey[E<:BaseEntity[E]: ClassTag] = EntityDefinition[E].mq.routingKey

}

trait MQ[E<:BaseEntity[E]] {

  protected val messageExpiration = 12.hours

  @throws(classOf[ApplicationServiceUnavailableException])
  protected def connection = ApplicationService[MessagingModule].connection

  protected val entityDefinition: EntityDefinition[E]

  type EntityRepr = entityDefinition.io.Repr

  lazy val outboundQueue =  Queue(s"${MQ.outboundExchange.name}.$routingKey", durable = true)

  lazy val inboundQueue =  Queue(s"${MQ.inboundExchange.name}.$routingKey", durable = true)

  lazy val routingKey = entityDefinition.categoryID.nameKey

  def messageID(entity:E) = s"[name:${entity.id.toString()},ver:${entity.version}]"

  lazy val inboundSource = Source.fromPublisher(connection.consume(inboundQueue.name)).map(x=>unmarshal(x.message))

  lazy val outboundSink = Flow[EntityRepr].map(x => marshal(x)).to(Sink.fromSubscriber(connection.publish(MQ.outboundExchange.name, routingKey)))

  lazy val outboundSource = Source.fromPublisher(connection.consume(outboundQueue.name)).map(x=>unmarshal(x.message))

  lazy val inboundSink = Flow[EntityRepr].map(x => marshal(x)).to(Sink.fromSubscriber(connection.publish(MQ.inboundExchange.name, routingKey)))

  protected lazy val jsonMarshaller = ProtoJsonSupport.toJsonStringTransformer[E]

  protected lazy val jsonUnmarshaller = ProtoJsonSupport.fromJsonStringTransformer[E](entityDefinition.companion)

  protected def marshal(entity: EntityRepr): Message = Message(ByteString(jsonMarshaller(entity._1)),
    Some(MediaType.JSON_UTF_8), correlationId = entity._2, replyTo = Some(inboundQueue.name), messageId = Some(messageID(entity._1)), expiration = messageExpiration)

  protected def unmarshal(message: Message): EntityRepr = (jsonUnmarshaller(ByteString(message.body:_*).utf8String), message.messageId.orElse(message.correlationId))

}
