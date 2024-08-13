/*
 * Author: Andrew Torson
 * Date: Oct 25, 2016
 */

package net.andrewtorson.bagpipe.gen

import java.io.FileInputStream
import java.util

import scala.collection.mutable

import com.fasterxml.jackson.core.{JsonParseException, JsonProcessingException, Version}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule


/**
 * Represents a type definition.
 */
case class TypeSchema(name: TypeName, comment: String, fields: Seq[Field])

object TypeSchema {


  /**
   * Loads a schema from a JSON file.
   */
  def fromJson(fileName: String): TypeSchema = {
    val inputStream = new FileInputStream(fileName)
    try {
      //val module = DefaultScalaModule
      //module.version()
      val mapper = new ObjectMapper()
      val schema = mapper.readValue(inputStream, classOf[Object])
      val result = deserialize(schema.asInstanceOf[util.LinkedHashMap[String, _<:Any]])
      result
    } catch {
        case x: Throwable => {
          throw new JsonParseException(null, s"Failed to create entity schema from JSON file $fileName", x)
        }
    } finally {
      inputStream.close()
    }
  }

  private def deserialize(obj: util.LinkedHashMap[String, _<:Any]): TypeSchema = {
    val name = TypeName(obj.get("name").asInstanceOf[util.LinkedHashMap[String, _<:Any]].get("fullName").asInstanceOf[String])
    val comment = obj.get("comment").asInstanceOf[String]
    val fields  = mutable.ArrayBuffer[Field]()
    val iter = obj.get("fields").asInstanceOf[util.ArrayList[util.LinkedHashMap[String, _<:Any]]].iterator()
    while (iter.hasNext()){
      val field = iter.next()
      val f = Field(field.get("name").asInstanceOf[String],TypeName(field.get("valueType").asInstanceOf[util.LinkedHashMap[String, _<:Any]].get("fullName").asInstanceOf[String]))
      fields += f
    }
    TypeSchema(name, comment, fields)
   }

  /**
   * Converts a schema to JSON.
   */
  def toJson(schema: TypeSchema): String = {
    val mapper = new ObjectMapper()
    mapper.writeValueAsString(schema)
  }

}
