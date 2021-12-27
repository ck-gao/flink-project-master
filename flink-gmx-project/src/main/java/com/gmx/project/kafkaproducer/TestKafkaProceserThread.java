package com.gmx.project.kafkaproducer;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TestKafkaProceserThread {
    //Kafka配置文件
    public static final String TOPIC_NAME = "test";
    public static final String KAFKA_PRODUCER = "kafka-producer.properties";
    public static final int producerNum = 50;//实例池大小
    //阻塞队列实现生产者实例池,获取连接作出队操作，归还连接作入队操作
    public static BlockingQueue<KafkaProducer<String, String>> queue = new LinkedBlockingQueue<>(producerNum);

    //初始化producer实例池
    static {
        for (int i = 0; i < producerNum; i++) {
            Properties properties = new Properties();
//            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.30.232:9092");
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
            properties.put(ProducerConfig.ACKS_CONFIG, "0");
            properties.put(ProducerConfig.RETRIES_CONFIG, "0");
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
            properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
            queue.add(producer);
        }
    }

    static class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
            }
            System.out.println("offset:" + recordMetadata.offset() + ";partition:" + recordMetadata.partition());
        }
    }


    //生产者发送线程
    static class SendThread extends Thread {
        String topic;
        String message;
        String quantity;

        public SendThread(String topic, String message, String quantity) {
            this.topic = topic;
            this.message = message;
            this.quantity = quantity;

        }

        public void run() {
            ProducerRecord record = new ProducerRecord(topic, message);
            try {
                KafkaProducer<String, String> kafkaProducer = queue.take();//从实例池获取连接,没有空闲连接则阻塞等待
//                kafkaProducer.send(record, new DemoProducerCallback());
                kafkaProducer.send(record);
                //System.out.println("topic is " + record.topic() + " partition is " + record.partition() + " timestamp is " + record.timestamp() + " key is " + record.key());
                queue.put(kafkaProducer);//归还kafka连接到连接池队列
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    //测试
    public static void main(String[] args) throws Exception {
        String topic = "";
        String message = "";
        String quantity = "";

        if (args.length == 0) {
            throw new IllegalArgumentException("args: java -jar xxx.jar -topic xx -message xx -quantity xx");
        } else {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            topic = parameterTool.get("topic");
            message = parameterTool.get("message");
            quantity = parameterTool.get("quantity");
            for (int i = 0; i < Integer.parseInt(quantity); i++) {
                SendThread sendThread = new SendThread(topic, message, quantity);
                sendThread.start();
            }
        }

    }
}
