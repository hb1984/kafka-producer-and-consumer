package com.bb.kafka.test;

import org.junit.Test;
import kafka.message.MessageAndMetadata;

public class TestListenerTest {

	@Test
	public void testConsume() {
		TestListener listener = new TestListener();
		listener.consume(new MessageAndMetadata<byte[], byte[]>(null, 0, null, 0, null, null){
			/**
			 * 
			 */
			private static final long serialVersionUID = 8716569859054589756L;

			@Override
			public byte[] key() {
				return String.valueOf(1l).getBytes();
			}
			
			@Override
			public byte[] message() {
				return "hello".getBytes();
			}			
		});
	}

}
