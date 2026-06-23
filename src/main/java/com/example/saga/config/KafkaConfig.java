package com.example.saga.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.beans.factory.annotation.Value;

@Configuration
public class KafkaConfig {

	@Value("${kafka.topic.order-created}")      
	private String orderCreated;
    @Value("${kafka.topic.payment-processed}")   
    private String paymentProcessed;
    @Value("${kafka.topic.payment-failed}")      
    private String paymentFailed;
    @Value("${kafka.topic.inventory-reserved}")  
    private String inventoryReserved;
    @Value("${kafka.topic.inventory-failed}")   
    private String inventoryFailed;
    @Value("${kafka.topic.order-completed}")     
    private String orderCompleted;
    @Value("${kafka.topic.order-cancelled}")     
    private String orderCancelled;
    @Value("${kafka.topic.notification-send}")   
    private String notificationSend;
 
    @Bean public NewTopic orderCreatedTopic()     
    { return build(orderCreated); }
    @Bean public NewTopic paymentProcessedTopic() 
    { return build(paymentProcessed); }
    @Bean public NewTopic paymentFailedTopic()    
    { return build(paymentFailed); }
    @Bean public NewTopic inventoryReservedTopic(){ return build(inventoryReserved); }
    @Bean public NewTopic inventoryFailedTopic()  { return build(inventoryFailed); }
    @Bean public NewTopic orderCompletedTopic()   { return build(orderCompleted); }
    @Bean public NewTopic orderCancelledTopic()   { return build(orderCancelled); }
    @Bean public NewTopic notificationSendTopic() { return build(notificationSend); }
 
    private NewTopic build(String name) {
        return TopicBuilder.name(name)
                .partitions(3)      // 3 partitions for parallel processing
                .replicas(1)        // 1 replica (use 3 in production)
                .build();
    }
    
    


//        @Bean
//        public ProducerFactory<String, Object> producerFactory(KafkaProperties  props) {
//            Map<String, Object> cfg = new HashMap<>(props.buildProducerProperties());
//            cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//            cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
//            return new DefaultKafkaProducerFactory<>(cfg);
//        }
//
//        @Bean
//        public KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> pf) {
//            return new KafkaTemplate<>(pf);
//        }
    
}
