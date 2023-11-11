package com.example.spring_cloud_streaming_kafka.controllers;


import com.example.spring_cloud_streaming_kafka.entities.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class PageEventRestController {
    @Autowired
    private StreamBridge streamBridge;
    @Autowired
    private InteractiveQueryService interactiveQueryService;

    @GetMapping("/publish/{topic}/{name}")
    public PageEvent publish(@PathVariable String topic, @PathVariable String name){
        PageEvent pageEvent = new PageEvent(name, Math.random()>0.5?"user1":"user2", new java.util.Date(), new Random().nextInt(10000));
        streamBridge.send(topic, pageEvent);
        return pageEvent;
    }

    @GetMapping(value = "/analysis",produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String,Long>> analysis(){
        return Flux.interval(Duration.ofSeconds(1))
                .map(seq->{
                    Map<String,Long> stringLongMap = new HashMap<>();
                    ReadOnlyWindowStore<String, Long> windowStore = interactiveQueryService.getQueryableStore("page-count", QueryableStoreTypes.windowStore());
                    Instant now=Instant.now();
                    Instant from=now.minusSeconds(5);
                    KeyValueIterator<Windowed<String>, Long> fetchAll = windowStore.fetchAll(from, now);
                    while (fetchAll.hasNext()){
                        KeyValue<Windowed<String>, Long> next = fetchAll.next();
                        stringLongMap.put(next.key.key(),next.value);
                    }
                    return stringLongMap;
                }).share();
    }

//    @GetMapping(value = "/analysis",produces = MediaType.TEXT_EVENT_STREAM_VALUE)
//    public Flux<Map<String,Long>> analysis(){
//        return Flux.interval(Duration.ofSeconds(1))
//                .map(seq->{
//                    Map<String,Long> map=new HashMap<>();
//                    ReadOnlyKeyValueStore<String, Long> stats = interactiveQueryService.getQueryableStore("count-store", QueryableStoreTypes.keyValueStore());
//                    Instant now=Instant.now();
//                    Instant from=now.minusSeconds(5);
//                    KeyValueIterator<String, Long> keyValueIterator = stats.all();
//                    while (keyValueIterator.hasNext()){
//                        KeyValue<String, Long> next = keyValueIterator.next();
//                        map.put(next.key,next.value);
//                    }
//                    return map;
//                }).share();
//    }

}

