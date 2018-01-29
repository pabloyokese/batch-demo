package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.task.configuration.EnableTask;

@EnableTask
@SpringBootApplication
public class BatchDemoApplication {

	public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(
            BatchDemoApplication.class, args)));
	}
}
