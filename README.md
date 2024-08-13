# Bagpipe
The Bagpipe framework allows you to rapidly develop streaming data integration services.
Your data may flow into the Bagpipe server via any of its public endpoint protocol: HTTP, AMQP, TCP, SQL, gRPC, Kafka - this list is, of course, extensible (for example, Akka-Camel allows to support a whole bundle of endpoint protocols).
Then, the data stream is piped into the internal Service Bus (built on top of Akka-EventBus) where it can be shared with the Bagpipe subscriber services.
The data is persisted (using Akka-Persistence coupled to an internal Data Store accessed via Quill-IO), transformed (via Akka-Streaming flows,
with a powerful DSL available to declare them) and forwarded to be piped out via one or many of the Bagpipe public endpoints.
Thus, the Bagpipe design resembles a traditional bagpipe musical instrument - and hopefully you'll like its music.

# Akka usage
The Bagpipe is based on the Akka framework and allows to capitalize on all of its major features: Akka-Actor technology,
reactive Akka-Streams, Akka-HTTP and Akka-IO technologies, Akka-Persistence and Akka-Remote/Cluster.
All of these Akka modules are exploited and exposed in the Bagpipe. In particular, Akka-Streams is the cornerstone dependency:
the data in the Bagpipe is always flowing within one of the built-in or plugged-in Akka Streams. The Akka-Streams DSL is both powerful and friendly -
and the Bagpipe exposes this DSL to declare data transformation streams, pluggable into its Service Bus as publishing and subscription services.
Akka Streaming IO (in the form of Akka-TCP and Akka-HTTP) is used in the Bagpipe to define HTTP and TCP endpoints: the backpressured/reactive streaming nature
of Akka-Streams is fully preserved within the Bagpipe and does not terminate neither at the Service Bus, nor at the IO boundaries.
The Bagpipe does offer streaming client implementations, as well as built-in servers for all public endpoints: this way, clusters of connected,
fully-reactive streaming applications can be built using the Bagpipe framework (the Example module demonstrates that with 3 applications working together to
implement a simple car fleet management use-case). Other notable Bagpipe dependencies include ScalaPB (exposing Google Protobuf features in Scala) and
Quill-IO (function-relation library managing async-IO access to a variety of SQL and NoSQL database technologies, such as PostgreSQL and Cassandra, for example).

# Data schema
The data in the Bagpipe is defined through a Protobuf schema which allows to generate immutable Scala case classes that can be
marshalled/unmarshalled to/from JSON and Proto-binary serialized representations. In addition, Quill-IO allows to
generate (at compile time) and execute (in run-time) SQL queries (or CQL queries, for Cassandra) based on the generated case classes.  Finally, the Protobuf code generator
has been augmented in the Bagpipe via a custom, Twirl-based Scala code generator which produces entity meta-data based on the Bagpipe entity definition template (see the Generator module for details).
Any entity-based functionality within the Bagpipe can be accessed through this meta-data (accessible via a global EntityDefinition singleton): for example,
CRUD, REST, MQ, TCP-IO and other features are available there.

# Project structure
The Bagpipe project is structured into 3 modules:
  - [Generator](https://github.com/atorson/bagpipe/tree/master/generator): a small auxillary module containing a Twirl template and a code generation utility for custom meta-data generation. This code leverages the ScalaPB Protobuf plugin - and its usage is reflected in the Bagpipe SBT project build file.
  - [Framework](https://github.com/atorson/bagpipe/tree/master/framework): this module encapsulates the Bagpipe framework, abstract of any particular application logic. Applications should extend it via defining custom, rich data models (specified in the Protobuf IDL files placed into the src/main/resources/proto folder) and adding more data processing flows (as custom Akka-Flows), registered with the Bagpipe StreamingModule that takes care of plugging them into the Service Bus.
  - [Example](https://github.com/atorson/bagpipe/tree/master/example): this modules demonstrates a simple example that consists of the 3 Bagpipe application nodes implementing a car fleet management use-case. The example defines a small but rich data model for the use-case and injects custom data transformation logic to generate & schedule car service trips and emulate trip progress, including location/position driving updates. All this data is streaming in a duplex fashion through the central Bagpipe node that simply    converts data from one communication protocol to others (for example, position updates are streamed in via TCP and streamed out via MQ and HTTP, as server-side events).
    
For details, please see the README.md files within each module

Most of the credit for this work should go to the Akka, ScalaPB, Quill-IO, Reactive-Rabbit and Swagger-Akka-Http projects.
