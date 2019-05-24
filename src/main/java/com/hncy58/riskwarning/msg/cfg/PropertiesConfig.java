package com.hncy58.riskwarning.msg.cfg;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * 
 * @author tokings
 * @company hncy58 湖南长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2018年12月11日 上午11:23:33
 *
 */
@Configuration
//@PropertySource(encoding = "utf-8", value = "classpath:app.properties")
@PropertySource(encoding = "utf-8", value = { "file:${config.path}" })
public class PropertiesConfig {

}