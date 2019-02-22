package com.narai;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author: kaze
 * @date: 2019-02-20
 */
@SpringBootApplication
public class NaraiApplication {

    public static void main(String[] args) {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        SpringApplication.run(NaraiApplication.class, args);
    }

}
