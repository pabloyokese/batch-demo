package com.example.demo;

import com.example.demo.configuration.DemoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BatchDemoApplication {

	public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(
            BatchDemoApplication.class, args)));
	}
}
