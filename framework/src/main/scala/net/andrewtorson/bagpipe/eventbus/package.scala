/*
 *
 * Author: Andrew Torson
 * Date: Nov 15, 2016
 */

package net.andrewtorson.bagpipe

import net.andrewtorson.bagpipe.utils.ID

/**
 * Created by Andrew Torson on 11/15/16.
 */
package object eventbus {

  type R = (EntityBusSubscriber, Set[ID])
}
