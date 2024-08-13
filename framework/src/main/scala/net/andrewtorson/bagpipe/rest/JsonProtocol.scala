/*
 *
 * Author: Andrew Torson
 * Date: Oct 28, 2016
 */

package net.andrewtorson.bagpipe.rest





import java.io.InvalidObjectException

import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.unmarshalling._
import akka.util.ByteString
import com.fasterxml.jackson.databind.JsonMappingException
import net.andrewtorson.bagpipe.utils._
import com.trueaccord.scalapb.GeneratedMessageCompanion
import com.trueaccord.scalapb.json.JsonFormat
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods._
import scala.language.existentials



/**
 * Created by Andrew Torson on 10/28/16.
 */
object ProtoJsonSupport {

  private val jsonStringUnmarshaller =
    Unmarshaller.byteStringUnmarshaller
      .forContentTypes(`application/json`)
      .mapWithCharset {
        case (ByteString.empty, _) => throw Unmarshaller.NoContentException
        case (data, charset)       => data.decodeString(charset.nioCharset.name)
      }

  private val jsonStringMarshaller =
    Marshaller.stringMarshaller(`application/json`)


  /**
   * HTTP entity => `A`
   *
   * @tparam E type to decode
   * @return unmarshaller for `A`
   */
  def jsonProtoUnmarshaller[E<:BaseEntity[E]](implicit cmp: GeneratedMessageCompanion[E]): FromEntityUnmarshaller[E] =

  jsonStringUnmarshaller.map(fromJsonStringTransformer[E]).recover(
    _ =>
      _ => {
        case x: JsonMappingException  =>
          throw x.getCause
      }
  )


  /**
   * `A` => HTTP entity
   *
   * @tparam E type to encode, must be upper bounded by `AnyRef`
   * @return marshaller for any `A` value
   */
  def jsonProtoMarshaller[E<:BaseEntity[E]] : ToEntityMarshaller[E] = jsonStringMarshaller.compose(toJsonStringTransformer[E])


  def fromJsonStringTransformer[E<:BaseEntity[E]](implicit cmp: GeneratedMessageCompanion[E]) = {data: String => JsonFormat.fromJsonString[E](data)}


  def toJsonStringTransformer[E<:BaseEntity[E]] = {data: E => JsonFormat.toJsonString[E](data)}



  /**
   * `TraversableOnce[E]` => HTTP entity
   *
   * @tparam E type to encode, must be upper bounded by `AnyRef`
   * @return marshaller for any `E` value
   */
  def jsonProtoArrayMarshaller[E<:BaseEntity[E]] : ToEntityMarshaller[List[E]] = {
    jsonStringMarshaller.compose[List[E]](collectionToJsonString[E])
  }

  /**
   * HTTP entity => TraversableOnce[E] =>
   *
   * @tparam E type to encode, must be upper bounded by `AnyRef`
   * @return unmarshaller for any `E` value
   */
  def jsonProtoArrayUnmarshaller[E<:BaseEntity[E]](implicit cmp: GeneratedMessageCompanion[E]) : FromEntityUnmarshaller[List[E]] =
  jsonStringUnmarshaller.map(data => {
    parse(data) match {
      case JArray(arr) => arr.map(JsonFormat.fromJson[E](_))
      case _ => throw JsonMappingException.fromUnexpectedIOE(new InvalidObjectException(s"JSON parsing of $data did not produce JArray, as expected"))
    }
  }).recover(
    _ =>
      _ => {
        case x: JsonMappingException  =>
          throw x.getCause
      }
  )

  private final def collectionToJsonString[E<:BaseEntity[E]](c: List[E]): String ={
    import org.json4s.jackson.JsonMethods._
    compact(JArray(c.map(JsonFormat.toJson[E])))
  }

  /*import collection.JavaConverters._
  import Ordering._

  private val edParentID = ID(ID.UtilityCategory, "ED")

  protected def id(fields: Seq[String]) = ID(edParentID, fields.toString)

  protected val definitionCache = new ConcurrentMapSimpleRegistry[ED](Reg.cbBuilder[ED].build())

  protected lazy val entityDefinitions: Set[(Set[String], ED)] =
    EntityDefinition.all().map(x=>(iterableAsScalaIterableConverter(x.companion.descriptor.getFields).asScala.map(_.getJsonName).toSet, x))

  protected def getEntityDefinition(fields: Seq[String], onFailure: => Throwable): ED = {
    definitionCache  ? (Reg.GetOrAdd[ED], id(fields),
      Some({ () => {
        val nameset = fields.toSet
        entityDefinitions.find(x => nameset.diff(x._1).isEmpty) match{
          case Some(v) => Some(v._2)
          case _ =>  throw onFailure
        }
      }})) match {
      case Success(Some(fd)) => fd
      case _ => throw onFailure
    }
  }

  protected def fromClassTag[E <: GeneratedMessage with Message[E]](value: JValue)(implicit cmp: GeneratedMessageCompanion[E]): E = {
    JsonFormat.fromJson[E](value)
  }


  val genericJsonProtoUnmarshaller: FromEntityUnmarshaller[BE] =
    jsonStringUnmarshaller.map { data => {
      val v = JsonMethods.parse(data)
      v match {
        case JObject(fields) => {
          val definition = getEntityDefinition(fields.map(_._1).sorted, JsonMappingException.fromUnexpectedIOE(
            new InvalidObjectException(s"JSON object field set $fields did not match any registered base entity")))
          fromClassTag(v)(definition.companion).asInstanceOf[BE]
        }
        case _ =>  throw JsonMappingException.fromUnexpectedIOE(new InvalidObjectException(s"JSON parsing of $data did not produce JObject, as expected"))
      }

    }}.recover(
      _ =>
        _ => {
          case x: JsonMappingException  =>
            throw x.getCause
        }
    )

  */
}