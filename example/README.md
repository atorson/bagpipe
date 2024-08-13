# Example 
This example demonstrates a simple car trip dispatch and execution use-case in the context of car service fleet management.  Imagine a set of cars performing passenger trips with receive and deliver locations.  
Every car has an onboard control system that accepts the trips, controls the car and provides progress updates. Another important aspect is the car fleet management: this is a higher-level function that consists of generating and dispatching new trips to the fleet and supervising trip progress. This function is performed offboard in a centralized, fleet-wide fashion.

In this example 3 distinct Bagpipe applications are used:
  - [Fleet Management](https://github.com/atorson/bagpipe/blob/master/example/src/main/scala/net/andrewtorson/bagpipe/FleetManagementExampleBoot.scala) application: generates and dispatches new trips and supervises trip progress. Its logic in this example is very simple: it monitors the state of the fleet and generates a new trip (at random receive and deliver locations) once a car becomes Idle.  Next, for  underway/in-progress trips, it monitors trip position updates to identify when cars arrive at receive/deliver locations - and then it issues Deliver/Finish actionable commands to the control system
  - [Integration/Routing](https://github.com/atorson/bagpipe/blob/master/example/src/main/scala/net/andrewtorson/bagpipe/BagpipeIntegrationExampleBoot.scala) application: converts and broadcasts all entity data messages circulating between the Fleet Management and the Control System. In this example, the Control System is only accessible via TCP (it does not have access to DB or HTTP) and the FleetManagement system is using a combination of HTTP, MQ and SQL interfaces (each for a different entity type). Finally, the Integration application can optionally process data analytics flows: in this example, a single flow is deployed as an Akka-Stream computing accumulated drive and wait time Statistic entity updates for each Car in each of the following categories/families: Idle, Unladen and Laden.
  - [Control System](https://github.com/atorson/bagpipe/blob/master/example/src/main/scala/net/andrewtorson/bagpipe/PositionEmulatorExampleBoot.scala) application: it is responsible for trip execution (driven by supervisory commands issued by the Fleet Management) and for providing of regular position updates. Its logic in this example is very simple: it follows a Command-and-Control protocol based on the following Action-State workflow protocol: Receive->Driving, Deliver->Carrying, Finish->Completed. It also emulates regular spatial position updates (on the X,Y-grid) every XX secs (by default, 1 sec) based on simple lattice driving algorithm (first, drive in X-direction, then drive in Y-direction) and configured speed (by default, 1 tile per sec).
  
# Data 
The [DOM](https://github.com/atorson/bagpipe/blob/master/example/src/main/resources/proto/DOM.proto) data schema has the following entities: Location; Car; Trip; TripState; TripAction and TripStatePosition. They are tightly coupled by foreign key relations.  Some of these entities are mutable and have dependency on the core Audit entitiy (showing created/update/version for the last update). The Integration application is responsible for persisting all of these entities in the DB, except for the TripAction entities (which are managed/persisted by the FleetManagement application)

The FleetManagement application uses an HTTP client to subscribe to the Car entity updates to monitor the status of all cars. When a car turns Idle - this application creates a new Trip entity in a new 'Assigned' TripState and issues a new 'Receive' TripAction. All of these entities are persisted (by the FleetManagement, as part of the new Trip persistence transaction). The Integration application polls (every 1 sec, configurable) for new TripAction updates and forwards them to the TCP endpoint (properly hydrated so that dependent Car/Trip entities are provided with every TripAction). The Control System application receives TripActions over TCP, generates a new  TripState transition and starts emulating a periodic stream of TripPositionUpdates - which is reflected back to the Integration Application over TCP.

The Integration application persists new TripStatePositions (and TripState/Trip/Car updates, as part of the TripStatePosition cascade transaction) and forwards them (properly hydrated) to the outbound MQ endpoint. The FleetManagement application is subscirbed to the TripStatePosition endpoint and monitors position updates to determine when cars reach receive/deliver locations: then, in the Driving state it issues the Deliver action and in the Deliver state it issues the Finish action. All these new TripActions are persisted by the FleetManagement in the DB (via SQL endpoint interface) and then picked up by the Intgeration application via its polling of the TripAction table. Finally, when the Trip is completed and the Car turns Idle (as part of the last TripStatePosistion update for an underway trip, processed by the Integration application) - the Integration application is setup to forward all Car updates via HTTP to the FleetManagement application so that the FleetManagement can generate a new Trip for the Car.

Initially, a set of Locations and idle Cars (declared within the Control System application) is sent out by the Control System application over TCP - and is shared by the Integration application with the Fleet Management over HTTP. This kicks off the trip generation with the Fleet Management - and then the example will churn forever until stopped externally.

All of the custom flow processing logic is defined as Akka-Streams (leveraging the [Composite Workflow](https://github.com/atorson/bagpipe/blob/master/framework/src/main/scala/net/andrewtorson/bagpipe/streaming/StreamingFlowOps.scala) construct defined in the Bagpipe framework) and deployed by extending the core [Streaming Module](https://github.com/atorson/bagpipe/blob/master/framework/src/main/scala/net/andrewtorson/bagpipe/utils/StreamingModule.scala) implementation in each of the 3 applications: [Fleet Management Streaming Module](https://github.com/atorson/bagpipe/blob/master/example/src/main/scala/net/andrewtorson/bagpipe/streaming/FleetManagementExampleStreamingModule.scala), [Integration Streaming Module](https://github.com/atorson/bagpipe/blob/master/example/src/main/scala/net/andrewtorson/bagpipe/streaming/BagpipeExampleStreamingModule.scala) and [Control System Streaming Module](https://github.com/atorson/bagpipe/blob/master/example/src/main/scala/net/andrewtorson/bagpipe/streaming/PositionEmulatorExampleStreamingModule.scala).

# Demo
After the build stage, the example demo can be launched and visually demonstrated using the following sequence of steps:

- launch the Integration application by starting the [BagpipeIntegrationExampleBoot](https://github.com/atorson/bagpipe/blob/master/example/src/main/scala/net/andrewtorson/bagpipe/BagpipeIntegrationExampleBoot.scala) main class

- launch the Curl client to monitor Car position updates over HTTP (identical to those forwarded to the Fleet Management application):  
     
     curl -X GET --header 'Accept: text/event-stream' 'http://localhost:8080/Car/Stream'
     
- launch the Netcat client to monitor TripAction and TripStatePosition updates over TCP(identical to those circulated to/from the Control System application):
     
     nc localhost 6544
     
- launch the RabbitMQ mamagement console and focus on monitoring the bagpipe.outbound.TripStatePosition queue:
     
     http://localhost:15672/#/queues/%2F/bagpipe.outbound.TripStatePosition

- launch a SQL client connected to the 'bagpipe' DB and use the following SQL query to monitor the Trip table updates (refresh its results windown manually, as wanted):
    
    select * from trip where status in ('Planned', 'Underway')
    
- launch in-browser demo GUI (simple HTML5/JavaScript application showing car position updates on a 2D tiled map -see screenshot below):     
    
    http://localhost:8080/demo/index.html
  
- launch the Fleet Management application by starting the [FleetManagementExampleBoot](https://github.com/atorson/bagpipe/blob/master/example/src/main/scala/net/andrewtorson/bagpipe/FleetManagementExampleBoot.scala) main class

- launch the Control System application by starting the [PositionEmulatorExampleBoot](https://github.com/atorson/bagpipe/blob/master/example/src/main/scala/net/andrewtorson/bagpipe/PositionEmulatorExampleBoot.scala) main class

After a few seconds, the in-browser GUI should visualize the map showing 5 cars (as green rectangles), moving towards their next location goal (see the screenshot below). Both Curl and Netcat consoles should reflect regular data updates, showing data stream flowing through HTTP and TCP, respectively. The RabbitMQ management console should show a steady 4-5 m/sec Message Rate chart reflecting data stream flowing over MQ. Finally, the SQL client should show 4-5 new Trip records with Planned/Underway status.  

![alt tag](https://github.com/atorson/bagpipe/blob/master/example/src/test/resources/example_demo.jpg)
