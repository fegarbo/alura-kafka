package br.com.garbo.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaDispatcher implements Closeable {

    private final KafkaProducer<String, String> producer;

    KafkaDispatcher(){
        this.producer = new KafkaProducer<>(properties());
    }

    private static Properties properties(){
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //Serialize String to bytes
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

    void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value);
        Callback callback = (data, ex) -> {
            if (ex != null){
                ex.printStackTrace();
                return;
            }
            System.out.println(
                    "Success, sending: " + data.topic() +
                            ", Partition: " + data.partition() +
                            ", Offset: " + data.offset() +
                            ", TimeStamp: " + data.timestamp());
        };
        //Send nao é blocante(eh assincrono), por isso utilizei o get, para aguardar terminar
        producer.send(record,callback).get();
    }

    //Necessario pois recursos como o Producer, deixam a porta aberta
    @Override
    public void close() {
        producer.close();
    }
}
