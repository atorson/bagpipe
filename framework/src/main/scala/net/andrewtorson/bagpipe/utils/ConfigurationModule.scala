/*
 *
 * Author: Andrew Torson
 * Date: Aug 8, 2016
 */

package net.andrewtorson.bagpipe.utils

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

trait ConfigurationModule {
  val config: Config
}

object ConfigurationModule {
  def apply(): ConfigurationModule = default
  val default = new ConfigurationModuleImpl {}
}

trait ConfigurationModuleImpl extends ConfigurationModule {

  override val config: Config = {
    val configDefaults = ConfigFactory.load(this.getClass().getClassLoader(), "bagpipe.conf")
    
    scala.sys.props.get("application.config") match {
      case Some(filename) => ConfigFactory.parseFile(new File(filename)).withFallback(configDefaults)
      case None => configDefaults
    }
  }
  
}