# jedi-kafka-client

High level kafka library

# Abstraction&High Level Api: 
     Kafka is shipped with a java sdk which allows to interact with kafka clusters.
     However sdk consists of some low-level apis which makes it difficult to use them correctly.
     Kafka is an advanced queue and has many parameters that interacts each other. 
     Clients just need a config file and kafkaService instance then ready to go.

# Thread Safe Parallel Consumption : 
Kafka consumers are single threaded. All IO happens in the thread making the call. Its users’ responsibility to ensure multithreaded access.

# Retry : 
It is hard to achieve retry mechanisms using kafka infrastructure due to some rules applied on message transfer mechanisms(session timeout vs consumer poll interval, kafka server session timeout, consumer rebalancing actions managed by kafka due to timeouts..)

# Gracefull Shutdown: 
Preventing message loss is an important issue during shutdown of applications.  All messages in progress must be finished and kafka cluster must be informed before shutdown happens.

# BoilerPlateCodes: 
Whenever kafka integration is required, we need to write similar codes, construct similar architectures. 

# Features
Producer-Consumer configurations load from a json file. 
Validation of json file; checking mandatory fields, relational mappings etc.
Auto byte[] serialization-deserialization to send recieve any object
Sync, Async message options
ConsumerHandler implemantation for inclusive Consumer processing
Retry Policies for consumer configurations
Gracefull shutdown
Intercepting on before and after producing and consuming messages to control, concurrent processing, retrying and handling errors 

#Configuration
Requires kafka-config.json  as classpath resource.
It is JSON formatted file for configuring producers and consumers.

# Consuming A Message
Client Consumers must implement ConsumerHandler interface
Consumer method is called for every message in parallel.

# Consuming Bulk Messages
When messages are produced one by one by many clients, sometimes we may want to consume messages as bulk for transactional purposes.

Consider a scenario where many clients produce log messages and you want to persist these messages as bulk to use minimum amount of connection pool.

Traditionally, you can’t use a topic for producing a message T and consuming another type of object  List<T>. 

However,  you can use List<T> consumers in this scenario, if you override isBulkConsumer method ouput value to true in your ConsumerHandler implementation as below.

# Responding to Messages
Consumers must return Response object.
Unsuccess messages are checked with retry policies that are defined in configuration file.
responseCode 0 stands for Success operation.

# Initializing JediKafkaClient

Clients must simply create a JediKafkaClient instance.

JediKafkaClient looks for a kafka-config.json file in classpath and creates producers and consumers.

JediKafkaClient jediKafkaClient = JediKafkaClient.getInstance();


Alternatively different filename can be used.
JediKafkaClient jediKafkaClient = JediKafkaClient.getInstance("otherKafkaConfig.json");

# Consumer Registeration
Consumer handler code ,which is a client code, must be registered for specific topic.
Theres is no need to do anything for producer.

final String TOPIC = "test";

//initializing JediKafkaClient
JediKafkaClient jediKafkaClient = JediKafkaClient.getInstance();
//Sample Consumer
MessageConsumer consumer = new MessageConsumer();
//registering consumer to related topic
jediKafkaClient.registerConsumer(TOPIC, consumer);

# Retrying Messages
Retry is automated.
Retry consumer and producer declarations are not mandatory in configuration file, but can be overridden for overriding default kafka parameters.
errorCode field can be used for many errorcodes in csv format like  500,501,503,504 for same topic and seconds.

# Sample kafka Configuration file

{
	"producers": {
		"testProducer": {
			"topic": "test",
			"properties": {
				"bootstrap.servers": "0.0.0.0:9092"
			}
		}
	},
	"consumers": {
		"testConsumer": {
			"topic": "test",
			"properties": {
				"bootstrap.servers": "0.0.0.0:9092"
			},
		   "maxRetryCount": 2,
		   "retryPolicys": {
			    "retry15_500": {
			     "retryTopic": "test-retry",
			     "errorCode": "500,501,503,504",
			     "seconds": "10"
			    }
			}
		}
	}
}


# Handling Circular Dependancies in Spring
If spring is used and if it is required for a consumer to produce new messages for an other topic during consumption, circular dependancy occurs on startup.

KafkaService can not be autowired in consumer handler, because on startup kafkaService needs this consumer for registeration.
Handler needs kafkaService for producing another message.

There are more than 1 solution to this problem.
An easy solution provided goes below.

Bean declaration

@Bean 
  public JediKafkaClient jediKafkaClient(@Autowired MessageHandler messageHandler) {
    JediKafkaClient jediKafkaClient = JediKafkaClient.getInstance();
    jediKafkaClient.registerConsumer(" topic-1", messageHandler);
    return jediKafkaClient;
}

On consumer side:
 
@Override
  public Response onMessage(Message message) {
        JediKafkaClient jediKafkaClient = JediKafkaClient.getInstance();
        String message = "Message";
        jediKafkaClient.sendAsync("topic-2", message);


