package someClass;

import com.opencsv.CSVWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.opencsv.CSVReader;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {
    public static void main(String[] args) {
        Properties props = new Properties();

        //Kafka BootStrap server
        props.setProperty("bootstrap.servers", "kafka:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());

        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> con = new KafkaConsumer<String, String>(props);
        con.subscribe(Arrays.asList("test"));
        ConsumerRecords<String, String> conRec = null;
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(new File("/kafka/new.csv")));
            while (true) {
                {
                    conRec = con.poll(100);
                    for (ConsumerRecord<String, String> consumerRecord : conRec) {
                        System.out.println("Partitions :" + consumerRecord.partition() +
                                ", Offset :" + consumerRecord.offset() +
                                ", Key :" + consumerRecord.key() +
                                ", Value :" + consumerRecord.value());
                        writer.append(consumerRecord.value().substring(1,consumerRecord.value().length()-1));
                        writer.append("\n");
                        writer.flush();
                    }
                }
            }
        } catch (Exception e) {
        }finally {
            try{
                writer.close();
            }catch (Exception e){

            }
        }
    }
}
