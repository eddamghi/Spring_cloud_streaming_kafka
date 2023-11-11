package com.example.spring_cloud_streaming_kafka.entities;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Date;
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class PageEvent {
    private String name;
    private String username;
    private Date date;
    private long duration;
}

