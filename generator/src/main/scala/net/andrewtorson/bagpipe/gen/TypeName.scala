/*
 * Author: Andrew Torson
 * Date: Oct 25, 2016
 */

package net.andrewtorson.bagpipe.gen

/**
 * Represents a fully qualified type name.
 */
case class TypeName(fullName: String) {

  private def lastDot = fullName.lastIndexOf('.')

  def packageName: String = fullName.take(lastDot)

  def shortName: String = fullName.drop(lastDot + 1)

  def enumName = if (fullName.endsWith("enum")) fullName.stripSuffix("enum")

}
