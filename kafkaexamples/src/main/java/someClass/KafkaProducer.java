package someClass;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.opencsv.CSVReader;

import java.io.*;
import java.util.*;

public class KafkaProducer {

    public static void main(String[] args) {

        Properties props = new Properties();

        //Kafka BootStrap server
        props.setProperty("bootstrap.servers","kafka:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer",StringSerializer.class.getName());
        props.setProperty("linger.ms","1 ");

        //Producer acks
        props.setProperty("acks","1");
        props.setProperty("retries","3");

        Producer <String,String> prod = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);

        CSVReader csvReader = null;

        List<String> list = new ArrayList<String>();
        String[] employeeDetails = null;

        try
        {
            /**
             * Reading the CSV File
             * Delimiter is comma
             * Start reading from line 1
             */
            csvReader = new CSVReader(new FileReader("/kafka/old.csv"),',','"',0);
            //employeeDetails stores the values current line
            //String[] employeeDetails = null;
            while((employeeDetails = csvReader.readNext())!=null)
            {
                //Printing to the console
                System.out.println(Arrays.toString(employeeDetails));
                list.add(Arrays.toString(employeeDetails));
            }
        }
        catch(Exception ee)
        {
            ee.printStackTrace();
        }
        finally
        {
            try
            {
                //closing the reader
                csvReader.close();
            }
            catch(Exception ee)
            {
                ee.printStackTrace();
            }
        }

        int key=0;

        for(String lineToken: list){
            key+=1;
            ProducerRecord<String,String> prodRec =
                    new ProducerRecord<String, String>("test",Integer.toString(key),lineToken);
            prod.send(prodRec);
        }
        prod.close();

    }
}
