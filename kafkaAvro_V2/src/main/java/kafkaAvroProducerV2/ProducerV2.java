package kafkaAvroProducerV2;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerV2 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<String, Customer>(properties);
        String topic = "customer-avro";

        Customer customer = Customer.newBuilder()
                .setFirstName("Arry")
                .setLastName("JVersion2")
                .setAge(17)
                .setHeight(187f)
                .setWeight(96f)
                .setPhoneNumber("1234-456-8976")
                .setEmail("arry@ghn.com")
                .build();
        ProducerRecord<String,Customer> producerRecord = new ProducerRecord<>(
                topic, customer
        );
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    System.out.println("Success!");
                    System.out.println(recordMetadata.toString());
                }else{
                    e.printStackTrace();
                }
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
