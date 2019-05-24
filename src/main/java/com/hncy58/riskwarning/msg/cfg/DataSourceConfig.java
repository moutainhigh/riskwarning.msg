package com.hncy58.riskwarning.msg.cfg;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 数据源配置
 * 
 * @author tokings
 * @company hncy58 湖南长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2019年5月21日 上午9:37:26
 *
 */
@Configuration
public class DataSourceConfig {

	@Bean(name = "mysqlDataSource")
	@Qualifier("mysqlDataSource")
	@ConfigurationProperties(prefix = "spring.datasource.mysql")
	public DataSource mysqlDataSource() {
		return DataSourceBuilder.create().build();
	}

	@Bean(name = "impalaDataSource")
	@Qualifier("impalaDataSource")
	@Primary
	@ConfigurationProperties(prefix = "spring.datasource.impala")
	public DataSource impalaDataSource() {
		return DataSourceBuilder.create().build();
	}

	@Bean(name = "mysqlJdbcTemplate")
	public JdbcTemplate primaryJdbcTemplate(@Qualifier("mysqlDataSource") DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}

	@Bean(name = "impalaJdbcTemplate")
	public JdbcTemplate secondaryJdbcTemplate(@Qualifier("impalaDataSource") DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}
}