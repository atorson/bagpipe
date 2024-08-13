/*
 *
 * Author: Andrew Torson
 * Date: Jan 24, 2017
 */

package net.andrewtorson.bagpipe

import net.andrewtorson.bagpipe.utils.{BaseEntity, EntityDefinition, OneOf}

/**
 * Created by Andrew Torson on 1/24/17.
 */
package object networking {

  import scala.language.existentials

  type OE = OneOf[E] forSome {type E<:BaseEntity[E]}
  type BE = BaseEntity[E] forSome {type E<:BaseEntity[E]}
  type ED = EntityDefinition[E] forSome {type E<:BaseEntity[E]}

}
