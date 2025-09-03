package com.example.dodoyfoxi_eruka_server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

@SpringBootApplication
@EnableEurekaServer
public class DodoyfoxiErukaServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DodoyfoxiErukaServerApplication.class, args);
    }

}
