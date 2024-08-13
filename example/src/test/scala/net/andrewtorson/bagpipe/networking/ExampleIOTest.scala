/*
 *
 * Author: Andrew Torson
 * Date: Jan 25, 2017
 */

package net.andrewtorson.bagpipe.networking

import net.andrewtorson.bagpipe.utils.DOMTestData


/**
 * Created by Andrew Torson on 1/25/17.
 */
class ExampleIOTest extends IOTest{

  override val data = DOMTestData
  override val tcpServer =   new ExampleTcpServerModuleProto(modules)
  override val tcpClient =   new ExampleTcpClientModuleProto(modules)

}
