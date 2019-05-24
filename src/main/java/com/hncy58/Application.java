package com.hncy58;

import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication(scanBasePackages = "com.hncy58")
@MapperScan(basePackages = { "com.hncy58.riskwarning.msg.mapper" })
@EnableTransactionManagement
public class Application {
	
	static Logger log = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) {
		log.info("start app start-------------------------------------------");
		SpringApplication.run(Application.class, args);
		log.info("start app end-------------------------------------------");
	}

}
