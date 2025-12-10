package com.jobserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class JobServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(JobServerApplication.class, args);
    }
}

