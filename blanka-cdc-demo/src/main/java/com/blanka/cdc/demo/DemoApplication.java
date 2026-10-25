package com.blanka.cdc.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * blanka cdc demo 启动类
 * <p>
 * 极简接入：pom引入starter + 业务原有rocketmq配置 + 环境变量CANAL_SERVER_ADDR，即可自动投递binlog
 *
 * @author blanka
 */
@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
