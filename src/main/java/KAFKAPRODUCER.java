import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KAFKAPRODUCER {
    public static void main(String[] args) {

        String bootstrapServers = "192.168.1.5:9092";
        String topic = "XML_ATM_CORR";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer
                .class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer
                .class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String key = "1";

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key,null);
            producer.send(record);

        producer.flush();
        producer.close();




    }


}
