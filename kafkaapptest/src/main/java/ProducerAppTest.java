import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerAppTest {
    private int counter;
    public int ValAl(int min,int max){
        Random r = new Random();
        int valeur = min + r.nextInt(max - min);
        return valeur;
    }
    public ProducerAppTest() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "client-producer-1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        Random random = new Random();
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            String nomSoc[] = {"soc1","soc2","soc3","soc4","soc5","soc6"};
            String TypeOr[] = {"Achat","Vente"};
            String key = String.valueOf(++counter);
            String societe = nomSoc[ValAl(0,4)];
            String typeOrdre = TypeOr[ValAl(0,1)];
            String value = String.valueOf(random.nextDouble() * 999999);
            kafkaProducer.send(new ProducerRecord<String, String>("test4", key, value), (metadata, ex) -> {
                System.out.println("nome société => " + societe + " type d'ordre => " + typeOrdre
                        + " nombre d'action => " + key + " prix action => " + value );
            });
        }, 1000, 10, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) {
        new ProducerAppTest();
    }
}