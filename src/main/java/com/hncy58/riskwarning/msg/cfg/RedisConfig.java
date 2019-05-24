package com.hncy58.riskwarning.msg.cfg;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Redis初始化配置类
 * 
 * @author tdz
 * @company hncy58 湖南长银五八
 * @website http://www.hncy58.com
 * @date 2018年8月7日 上午9:33:02
 */
@Configuration
// @EnableRedisHttpSession(maxInactiveIntervalInSeconds = 3600)
public class RedisConfig {

	/**
	 * 实例化Redis连接工厂
	 * 
	 * @return
	 */
//	@Bean
//	public RedisConnectionFactory redisConnectionFactory() {
//		return new JedisConnectionFactory();
//	}

	/**
	 * 注入 Redis连接工厂
	 */
	@Autowired
	RedisConnectionFactory redisConnectionFactory;

	/**
	 * 实例化 RedisTemplate 对象（这个redis模板是用来存储对象使用）
	 * 
	 * @return
	 */
	@Bean(name = "redisTemplate")
	public RedisTemplate<String, Object> functionDomainRedisTemplate() {
		RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
		initDomainRedisTemplate(redisTemplate, redisConnectionFactory);
		return redisTemplate;
	}

	/**
	 * 实例化 RedisTemplate 对象（这个redis模板是用来存储JSON对象使用，例如发送pub/sub消息）
	 * 
	 * @return
	 */
	@Bean(name = "plainRedisTemplate")
	public RedisTemplate<String, Object> plainTextRedisTemplate() {
		RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
		initPlainTextRedisTemplate(redisTemplate, redisConnectionFactory);
		return redisTemplate;
	}

	/**
	 * 设置对象数据存入 redis 的序列化方式
	 * 
	 * @param redisTemplate
	 * @param factory
	 */
	private void initDomainRedisTemplate(RedisTemplate<String, Object> redisTemplate, RedisConnectionFactory factory) {
		redisTemplate.setKeySerializer(new StringRedisSerializer());
		redisTemplate.setHashKeySerializer(new StringRedisSerializer());
		redisTemplate.setHashValueSerializer(new JdkSerializationRedisSerializer());
		redisTemplate.setValueSerializer(new JdkSerializationRedisSerializer());
		redisTemplate.setConnectionFactory(factory);
	}

	/**
	 * 设置普通文本数据存入 redis 的序列化方式
	 * 
	 * @param redisTemplate
	 * @param factory
	 */
	private void initPlainTextRedisTemplate(RedisTemplate<String, Object> redisTemplate,
			RedisConnectionFactory factory) {
		// redisTemplate.setConnectionFactory(factory);
		// redisTemplate.setKeySerializer(new StringRedisSerializer());
		// redisTemplate.setHashKeySerializer(new StringRedisSerializer());
		// redisTemplate.setHashValueSerializer(new StringRedisSerializer());
		// redisTemplate.setValueSerializer(new StringRedisSerializer());
		// redisTemplate.afterPropertiesSet();

		Jackson2JsonRedisSerializer<Object> serializer = new Jackson2JsonRedisSerializer<Object>(Object.class);
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
		objectMapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		serializer.setObjectMapper(objectMapper);

		redisTemplate.setConnectionFactory(factory);
		redisTemplate.setKeySerializer(new StringRedisSerializer());
		redisTemplate.setValueSerializer(serializer);
		redisTemplate.setHashKeySerializer(new StringRedisSerializer());
		redisTemplate.setHashValueSerializer(serializer);
		redisTemplate.afterPropertiesSet();
	}

	/**
	 * 实例化 HashOperations 对象,可以使用 Hash 类型操作
	 * 
	 * @param redisTemplate
	 * @return
	 */
	@Bean
	public HashOperations<String, String, Object> hashOperations(RedisTemplate<String, Object> redisTemplate) {
		return redisTemplate.opsForHash();
	}

	/**
	 * 实例化 ValueOperations 对象,可以使用 String 操作
	 * 
	 * @param redisTemplate
	 * @return
	 */
	@Bean
	public ValueOperations<String, Object> valueOperations(RedisTemplate<String, Object> redisTemplate) {
		return redisTemplate.opsForValue();
	}

	/**
	 * 实例化 ListOperations 对象,可以使用 List 操作
	 *
	 * @param redisTemplate
	 * @return
	 */
	@Bean
	public ListOperations<String, Object> listOperations(RedisTemplate<String, Object> redisTemplate) {
		return redisTemplate.opsForList();
	}

	/**
	 * 实例化 SetOperations 对象,可以使用 Set 操作
	 *
	 * @param redisTemplate
	 * @return
	 */
	@Bean
	public SetOperations<String, Object> setOperations(RedisTemplate<String, Object> redisTemplate) {
		return redisTemplate.opsForSet();
	}

	/**
	 * 实例化 ZSetOperations 对象,可以使用 ZSet 操作
	 *
	 * @param redisTemplate
	 * @return
	 */
	@Bean
	public ZSetOperations<String, Object> zSetOperations(RedisTemplate<String, Object> redisTemplate) {
		return redisTemplate.opsForZSet();
	}
}
