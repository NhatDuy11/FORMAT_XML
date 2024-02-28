import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KAFKAPRODUCER {
    public static void main(String[] args) {

        String bootstrapServers = "172.19.182.0:9092";
        String topic = "xml_sc";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer
                .class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer
                .class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 1000; i++) {
            String message = "<?xml version='1.0' encoding='UTF-8'?><operation table='SACOM_SW_OWN.ATM_LOG' type='I' ts='2023-08-23 11:15:05.898835' current_ts='2023-08-23T11:17:35.723001' pos='00000000020001372473' numCols='8'><col name='SHCLOG_ID' index='0'><before missing='true'/><after><![CDATA[AAEAsgAkZN52XQAB ]]></after></col><col name='INSTITUTION_ID' index='1'><before missing='true'/><after><![CDATA[1]]></after></col><col name='GROUP_NAME' index='2'><before missing='true'/><after><![CDATA[SGE5050101]]></after></col><col name='UNIT' index='3'><before missing='true'/><after><![CDATA[97]]></after></col><col name='FUNCTION_CODE' index='4'><before missing='true'/><after><![CDATA[200]]></after></col><col name='LOGGED_TIME' index='5'><before missing='true'/><after><![CDATA[2023-08-18 02:34:53.000000000]]></after></col><col name='LOG_DATA' index='6'><before missing='true'/><after><![CDATA[12\u001C097002033\u001C\u001CP20]]></after></col><col name='SITE_ID' index='7'><before missing='true'/><after><![CDATA[1]]></after></col></operation> ";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            producer.send(record);
        }
        producer.flush();
        producer.close();




    }


}
