package com.bb.kafka.test;

import com.bb.kafka.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.message.MessageAndMetadata;

public class TestListener implements KafkaConsumer.Listener{
	
    private final Logger LOGGER = LoggerFactory.getLogger(TestListener.class);

	@Override
	public void consume(MessageAndMetadata<byte[], byte[]> message) {
		String key = "";
		if(null == message.key()){
			
		}else{
			key = new String(message.key());
		}
		
		LOGGER.info("message id:" + key + ", body:" +new String(message.message()));		
	}

}
