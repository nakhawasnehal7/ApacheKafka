package wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {

    public static void main(String[] args) throws InterruptedException {

        // Kafka Producer Setup
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topic = "wikimedia-Snehal";

        // Wikimedia EventSource Setup
        EventHandler handler = new WikimediaChangeHandler(producer, topic);

        // ✅ Required User-Agent header
        Headers headers = new Headers.Builder()
                .add("User-Agent", "KafkaWikimediaProducer/1.0 (your@email.com)")
                .add("Accept", "text/event-stream")
                .build();

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource eventSource = new EventSource.Builder(
                handler, URI.create(url))
                .headers(headers)
                .build();

        eventSource.start();

        System.out.println("✅ Streaming from Wikimedia → Kafka topic: " + topic);

        // Run for 10 minutes
        Thread.sleep(TimeUnit.MINUTES.toMillis(10));

        eventSource.close();
        producer.close();
    }
}