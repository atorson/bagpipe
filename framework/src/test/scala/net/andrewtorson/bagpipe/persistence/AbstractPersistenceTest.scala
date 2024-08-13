/*
 *
 * Author: Andrew Torson
 * Date: Oct 28, 2016
 */

package net.andrewtorson.bagpipe.persistence

import net.andrewtorson.bagpipe.utils.{TestConfigurationModule, _}
import org.scalatest.{FunSuite, Suite}

trait AbstractPersistenceTest extends FunSuite {  this: Suite =>

  trait Modules extends TestConfigurationModule {
    val persistence = new PersistenceModuleImpl(this)
  }



}