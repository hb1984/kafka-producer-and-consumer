# kafka
kafka producer and cosumer, easy to integrate with spring

based on kafa 0.8.1

# producer
- configuration
```xml
	<bean id="producerProperties"
		class="org.springframework.beans.factory.config.PropertiesFactoryBean">
		<property name="locations">
			<list>
		    <value>classpath*:kafka.properties</value>
			</list>
		</property>
	</bean>

	<bean id="kafkaProducer" class="com.bb.kafka.producer.KafkaProducer" init-method="init" destroy-method="destroy" scope="singleton">
		<property name="properties" ref="producerProperties"></property>
	</bean>
```
<br>

# consumer(at most once)
- configuration

```xml
	<bean id="consumerProperties"
			class="org.springframework.beans.factory.config.PropertiesFactoryBean">
		<property name="locations">
		<list>
		    <value>classpath*:kafka.properties</value>
		</list>
		</property>
	</bean>		

	<bean id="testListener" class="com.bb.kafka.test.TestListener" scope="singleton">

	</bean>		

	<bean id="testConsumer" class="com.bb.kafka.consumer.KafkaConsumer" init-method="init" destroy-method="destroy" scope="singleton">
		<property name="properties" ref="consumerProperties" />
		<property name="consumerGroup" value="testGroup" />
		<property name="topic" value="tesTopic" />
		<property name="threadNum" value="1" />
		<property name="messageListener" ref="testListener"></property>
	</bean>
```
