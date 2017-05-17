package com.bb.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.Consumer;  
import kafka.consumer.ConsumerIterator;  
import kafka.consumer.KafkaStream;  
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata; 

public class KafkaConsumer {
	
    private final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    private ConsumerConnector consumer;
    private String topic;
    private ExecutorService executor;
    private String consumerGroup;
    private Integer threadNum;
    private Properties properties;
    private Listener messageListener;
    
	/**
     * Spring bean init-method
     */
    public void init(){

        // 参数信息
    	LOGGER.info("Kafka Consumer initialize!");
        properties.put("group.id", consumerGroup);
        LOGGER.info(properties.toString());
		LOGGER.info("Topic:" + topic);
		LOGGER.info("threadNum:" + threadNum);
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumer = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, threadNum);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> partitions = consumerMap.get(topic);
     
     
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(threadNum);
     
        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (KafkaStream<byte[], byte[]> partition : partitions) {
            executor.execute(new ConsumerTask(partition, threadNumber, this.messageListener));
            threadNumber++;
        }
        
        LOGGER.info("Kafka Consumer start success!");
    }

    public static class ConsumerTask implements Runnable {
    	
        private final Logger LOGGER = LoggerFactory.getLogger(ConsumerTask.class);

        private KafkaStream<byte[], byte[]> partition;  
//        private int threadNumber;
        private Listener listener;
     
        public ConsumerTask(KafkaStream<byte[], byte[]> partition, int threadNumber, Listener listener) {
        	this.partition = partition;
//        	this.threadNumber = threadNumber;
        	this.listener = listener;
        }
     
        public void run() {
        	LOGGER.info("consumer thread: {} start.", Thread.currentThread().getName());
			ConsumerIterator<byte[], byte[]> it = partition.iterator();
			// connector.commitOffsets();手动提交offset,当autocommit.enable=false时使用
			while (it.hasNext()) {
				MessageAndMetadata<byte[], byte[]> message = it.next();
				LOGGER.info("receive message:" + message.toString());
				try {
					listener.consume(message);
				} catch (Exception e) {
					LOGGER.error("got errors whiel consume message", e);
				}
			}
			LOGGER.info("consumer thread: {} end.", Thread.currentThread().getName());
        }
    }
    
    public static interface Listener{
    	public void consume(MessageAndMetadata<byte[], byte[]> message);
    }
    
    /**
     * Spring bean destroy-method
     */
    public void destroy() {

    	this.close((ThreadPoolExecutor)executor);
    	 
//         if (consumer != null) consumer.shutdown();
//         if (executor != null) executor.shutdown();
//         try {
//             if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
//                 System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
//             }
//         } catch (InterruptedException e) {
//             System.out.println("Interrupted during shutdown, exiting uncleanly");
//         }
    }
    
	private void close(ThreadPoolExecutor poolExecutor){
		
		while(!poolExecutor.getQueue().isEmpty()){
			LOGGER.info("消息处理线程还有未完成任务，暂时不能关闭");
			
			try {
				TimeUnit.MILLISECONDS.sleep(1000);
			} catch (InterruptedException e) {
				LOGGER.error("Thread sleep error", e);
			}
			continue;
		}		
		
    	consumer.shutdown();
		poolExecutor.shutdown();
		
		try {
			while(!poolExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS)){
				LOGGER.info("线程池还未完全关闭");
				continue;
			}
		} catch (InterruptedException e) {
			LOGGER.error("Thread shutdown error", e);
		}
		
		LOGGER.info("shut down complete.");

	}

    // ----------------- setter --------------------

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public Integer getThreadNum() {
		return threadNum;
	}

	public void setThreadNum(Integer threadNum) {
		this.threadNum = threadNum;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public Listener getMessageListener() {
		return messageListener;
	}

	public void setMessageListener(Listener messageListener) {
		this.messageListener = messageListener;
	}

}
