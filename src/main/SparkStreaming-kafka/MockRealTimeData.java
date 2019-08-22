import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class MockRealTimeData extends Thread {
    private static final Random random = new Random();
    private static final String[] provinces = new String[]{"Jiangsu", "Hubei", "Hunan", "Henan", "Hebei"};
    private static final Map<String, String[]> provinceCityMap = new HashMap<String, String[]>();

    private KafkaProducer<Integer, String> producer;
    private String brokerList = "hadoop01:9092,hadoop02:9092,hadoop03:9092";
    private String topics = "AdRealTimeLog";

    public MockRealTimeData() {
        provinceCityMap.put("Jiangsu", new String[]{"Nanjing", "Suzhou"});
        provinceCityMap.put("Hubei", new String[]{"Wuhan", "Jingzhou"});
        provinceCityMap.put("Hunan", new String[]{"Changsha", "Xiangtan"});
        provinceCityMap.put("Henan", new String[]{"Zhengzhou", "Luoyang"});
        provinceCityMap.put("Hebei", new String[]{"Shijiazhuang", "Zhangjiakou"});

        producer = new KafkaProducer<Integer, String>(createProducerConfig());
    }

    private Properties createProducerConfig() {
        Properties prop = new Properties();
        // 指定请求的kafka集群列表
        prop.put("bootstrap.servers", brokerList);
        // 指定响应方式
        prop.put("acks", "0");
        // 请求失败重试次数
        prop.put("retries", "3");
        // 指定key的序列化方式, key是用于存放数据对应的offset
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 指定value的序列化方式
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return prop;
    }

    public void run() {
        while (true) {
            String province = provinces[random.nextInt(5)];
            String city = provinceCityMap.get(province)[random.nextInt(2)];
            // 数据格式为："timestamp province city userId adId"
            String log = new Date().getTime() + " " + province + " " + city + " "
                    + random.nextInt(1000) + " " + random.nextInt(10);
            producer.send(new ProducerRecord<Integer, String>(topics, log));

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 启动Kafka Producer
     *
     * @param args
     */
    public static void main(String[] args) {
        MockRealTimeData producer = new MockRealTimeData();
        producer.start();
    }

}
