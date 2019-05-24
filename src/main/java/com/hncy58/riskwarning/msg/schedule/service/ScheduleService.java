package com.hncy58.riskwarning.msg.schedule.service;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class ScheduleService {

	Logger log = LoggerFactory.getLogger(ScheduleService.class);

	@Value("${audit.data.deleteDays.:7}")
	private int deleteDays;
	
	@Autowired
	@Qualifier("mysqlJdbcTemplate")
	JdbcTemplate mysqlJdbcTemplate;
	
	@Autowired
	@Qualifier("impalaJdbcTemplate")
	JdbcTemplate impalaJdbcTemplate;

	public void deleteAuditRecord() {
		String sql = "delete from sys_user_opr_log where DATEDIFF(now(), opr_time) > " + deleteDays;
		int cnt = mysqlJdbcTemplate.update(sql);
		log.info("deleted {} audit records.", cnt);
	}
	
	public void testImpala() {
		String sql = "select now()";
		List<Map<String, Object>> ret = impalaJdbcTemplate.queryForList(sql);
		log.info("test impala sql: {}, records: {}", sql, ret);
	}

}
