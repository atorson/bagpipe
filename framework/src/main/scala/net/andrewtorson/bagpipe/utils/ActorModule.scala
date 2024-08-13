/*
 *
 * Author: Andrew Torson
 * Date: Aug 8, 2016
 */

package net.andrewtorson.bagpipe.utils

import akka.actor.ActorSystem
import net.andrewtorson.bagpipe.streaming.StreamingFlowOps


trait ActorModule {

  val system: ActorSystem

  lazy val streamingActorsRegistry = StreamingFlowOps.actorsReg(system)

}


trait ActorModuleImpl extends ActorModule {
  this: ConfigurationModule =>
  val system = ActorSystem("AkkaBagpipe", config)
}