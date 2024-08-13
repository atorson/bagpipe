/*
 *
 * Author: Andrew Torson
 * Date: Oct 10, 2016
 */

package net.andrewtorson.bagpipe




import akka.actor.ActorRef
import net.andrewtorson.bagpipe.utils.{RegistryFacade, SimpleRegistryFacade}


/**
 * Created by Andrew Torson on 10/10/16.
 */
package object streaming {

  type A = ActorRef
  type B = Boolean
  type AR = RegistryFacade[A, B]

  type C = (()=>Unit,()=>Unit)
  type CR = SimpleRegistryFacade[C]


}
