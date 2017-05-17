package com.bb.kafka.producer;

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

    private final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private Producer<String,String> producer;  
    private Properties properties;

    /**
     * Spring bean init-method
     */
    public void init(){

		// 参数信息
		logger.info("Producer initialize!");
		logger.info(properties.toString());
		ProducerConfig config = new ProducerConfig(properties);
		producer = new Producer<String, String>(config);

		logger.info("Proudcer start success!");
    }

    /**
     * Spring bean destroy-method
     */
    public void destroy() {
    	 producer.close();
    }
    
	public void send(String topic, String value) {
		KeyedMessage<String, String> msg = new KeyedMessage<String, String>(topic, value);
        this.send(msg);
	}
	
	public void send(String topic, String key, String value) {
		KeyedMessage<String, String> msg = new KeyedMessage<String, String>(topic, key, value);
        this.send(msg);
	}
	
	public void send(KeyedMessage<String,String> message) {
		producer.send(message);
	}
	
	public void send(List<KeyedMessage<String,String>> messages) {
		producer.send(messages);
	}

	public Producer<String, String> getProducer() {
		return producer;
	}

	public void setProducer(Producer<String, String> producer) {
		this.producer = producer;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
	


}
