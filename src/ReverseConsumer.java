import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.sql.*;

import java.util.Arrays;
import java.util.Properties;

public class ReverseConsumer {
    public static void main(String[] args) {

        int evenNumber;

        KafkaConsumer consumer;
        String topic = "naturalNumbers";
        String broker = "localhost:9092";
        Properties props = new Properties();
        props.put("bootstrap.servers",broker);
        props.put("group.id", "test-group");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList(topic));

        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for(ConsumerRecord<String,String> record: records){
                System.out.println(record.value());

                evenNumber = Integer.parseInt(record.value());
                StringBuilder reverseNum = new StringBuilder();
                reverseNum.append(record.value());
                reverseNum.reverse();



                    try{
                        Class.forName("com.mysql.jdbc.Driver");
                        Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/kafkadb", "root","");
                        String sql = "INSERT INTO `reversenumbers`(`reverseNumbers`) VALUES (?)";
                        PreparedStatement stmt = con.prepareStatement(sql);

                        stmt.setString(1,String.valueOf(reverseNum));
                        stmt.executeUpdate();
                        System.out.println("Reverse number inserted to db "+reverseNum);


                    }
                    catch (Exception e){
                        System.out.println(e);
                    }


            }
        }
    }
}