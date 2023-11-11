package com.example.spring_cloud_streaming_kafka.services;

import com.example.spring_cloud_streaming_kafka.entities.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
@Configuration
public class PageEventService {
    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return pageEvent -> {
            System.out.println("--------------------");
            System.out.println(pageEvent.toString());
            System.out.println("--------------------");
        };
    }

    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> {
            PageEvent pageEvent = new PageEvent();
            pageEvent.setName(Math.random() > 0.5 ? "page1" : "page2");
            pageEvent.setUsername(Math.random() > 0.5 ? "user1" : "user2");
            pageEvent.setDate(new java.util.Date());
            pageEvent.setDuration(new Random().nextInt(10000));
            return pageEvent;
        };
    }

    @Bean
    public Function<PageEvent, PageEvent> pageEventFunction() {
        return pageEvent -> {
            pageEvent.setName("Length : " + pageEvent.getName().length());
            pageEvent.setUsername("user");
            pageEvent.setDate(new java.util.Date());
            pageEvent.setDuration(new Random().nextInt(10000));
            return pageEvent;
        };
    }

    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> KStreamFunction() {
        return (input) -> {
            return input
                    .filter((key, value) -> value.getDuration() > 100)
                    .map((key, value) -> new KeyValue<>(value.getName(), 0L))
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                    .count(Materialized.as("page-count"))
                    .toStream()
                    .map((key, value) -> new KeyValue<>("=>" + key.key() + key.window().startTime() + key.window().endTime(), value));
        };
    }
}
//    @Bean
//    public Consumer<KStream<String,PageEvent>> pageStreamConsumer(){
//        return (pageEvent -> pageEvent
//                .filter((k,v)->v.getDuration()>100)
//                .map((k,v)->new KeyValue<>(v.getName(),v))
//                .peek((k,v)-> System.out.println(k+"=>"+v))
//                .groupByKey(Grouped.with(Serdes.String(), AppSerdes.PageEventSerdes()))
//                .reduce((acc,v)->{
//                    v.setDuration(v.getDuration()+acc.getDuration());
//                    return v;
//                })
//                .toStream()
//                .peek((k,v)-> System.out.println(k+"=>"+v)));
//    }
//}


