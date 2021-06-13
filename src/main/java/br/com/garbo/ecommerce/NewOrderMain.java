package br.com.garbo.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        /* KafkaProducer<KeyType, MsgType> */
        var producer = new KafkaProducer<String, String>(properties());
        var value = "077, 577, 5465458";
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
        producer.send(record, (data, ex) -> {
            if (ex != null){
                ex.printStackTrace();
                return;
            }
            System.out.println(
                    "Success, sending: " + data.topic() +
                    ", Partition: " + data.partition() +
                    ", Offset: " + data.offset() +
                    ", TimeStamp: " + data.timestamp());
        }).get(); //Send nao é blocante(eh assincrono), por isso utilizei o get, para aguardar terminar
    }

    private static Properties properties(){
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //Serialize String to bytes
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
