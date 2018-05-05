package com.its.redis;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.GeoOperations;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.repository.CrudRepository;
import org.springframework.session.data.redis.config.annotation.web.http.EnableRedisHttpSession;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.SessionAttributes;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@EnableCaching
@EnableRedisHttpSession
@SpringBootApplication
public class RedisApplication {

	public static void main(String[] args) {
		SpringApplication.run(RedisApplication.class, args);
	}
	
	@Bean
	CacheManager redisCache(RedisConnectionFactory redisConnectionFactory) {
		return RedisCacheManager
				.builder(redisConnectionFactory)
				.build();
	}
	
	@Bean
	ApplicationRunner pubSub(RedisTemplate<String, String> rt){
		log.info("Entering and leaving RedisApplication : pubSub");
		return titledRunner("publish / subscribe", args -> {
			rt.convertAndSend(TOPIC, "Hello World @" + Instant.now().toString());
		});
	}
	
	@Bean
	RedisMessageListenerContainer listener(RedisConnectionFactory cf) {
		log.info("Entering RedisApplication : redisMessageListenerContainer");
		MessageListener ml = (message, pattern) -> {
				String str = new String(message.getBody());
				log.info("Message from '" + TOPIC + "' : " + str);
		};
		
		RedisMessageListenerContainer mlc = new RedisMessageListenerContainer();
		mlc.setConnectionFactory(cf);
		mlc.addMessageListener(ml, new PatternTopic(this.TOPIC));
		
		log.info("Leaving RedisApplication : redisMessageListenerContainer");
		return mlc;
	}
	
	@Bean
	ApplicationRunner repositories(OrderRepository orderRepository, LineItemRepository lineItemRepository) {
		log.info("Entering and leaving RedisApplication : redisMessageListenerContainer");
		return titledRunner("repositories", arg -> {
			Long orderId = generatedId();
			List <LineItem> itemList = new ArrayList<LineItem>();
			
			itemList.add(new LineItem(orderId, generatedId(), "plunger"));
			itemList.add(new LineItem(orderId, generatedId(), "soup"));
			itemList.add(new LineItem(orderId, generatedId(), "coffee"));
			
			/*List<Object> itemList = Arrays.asList(
					new LineItem(orderId, generatedId(), "plunger"));
			,
					new LineItem(orderId, generatedId(), "soup"),
					new LineItem(orderId, generatedId(), "coffee"));*/
			
			itemList
				.stream()
				.map(lineItemRepository :: save)
				.forEach(li -> log.info(li.toString()));
			
			Order order = new Order(orderId, new Date(), itemList);
			orderRepository.save(order);
			
			Collection<Order> found = orderRepository.findByWhen(order.getWhen());
			
			found.forEach(o -> log.info(o.toString()));
		});
	}
	
	private Long generatedId() {
		long tmp = new Random().nextLong();
		return Math.max(tmp, tmp * -1);
	}

	private ApplicationRunner titledRunner(String title, ApplicationRunner rr) {
		log.info("Entering RedisApplication : titledRunner");
		return args -> {
			log.info(title.toUpperCase() + ":");
			rr.run(args);
		};
	}
	
	@Bean
	ApplicationRunner geography(RedisTemplate<String, String> rt){
		return titledRunner("geography", args -> {
			GeoOperations <String, String> geo = rt.opsForGeo();
			
			geo.add("Sicily", new Point(13.361389, 38.1155556), "Arigento");
			geo.add("Sicily", new Point(15.087269, 37.502669), "Catania");
			geo.add("Sicily", new Point(13.583333, 37.316667), "Palermo");
			
			Circle circle = new Circle (new Point(13.583333, 37.316667)
					, new Distance(100, RedisGeoCommands.DistanceUnit.KILOMETERS));
			
			GeoResults<RedisGeoCommands.GeoLocation<String>> geoResults = 
					geo.radius("Sicily", circle);
			
			geoResults
				.getContent()
				.forEach(c -> log.info(c.toString()));
		});
	}
	private long measure (Runnable r) {
		long start = System.currentTimeMillis();
		r.run();
		long end = System.currentTimeMillis();
		return end - start;
	}
	
	@Bean
	ApplicationRunner cache (OrderService orderService) {
		return titledRunner("caching", a -> {
			Runnable measure = () -> orderService.byId(1L);
			log.info("first " + measure(measure));
			log.info("Two " + measure(measure));
			log.info("Three " + measure(measure));
		});
	}
	
	private final String TOPIC = "chat";
	
	
	
	/*public static class Cat {
		
	}*/
	
	/*
	 * Below implementation shows, how to customize redistemplate configuration
	 * 
	 * @Bean
	//@ConditionalOnMissingBean(name = "redisTemplate")
	RedisTemplate<String, Cat> redisTemplate(
			RedisConnectionFactory redisConnectionFactory)  {
		RedisTemplate<String, Cat> template = new RedisTemplate<>();
		
		RedisSerializer keys = new StringRedisSerializer();
		RedisSerializer<?> values = new Jackson2JsonRedisSerializer<>(Cat.class);
		
		template.setConnectionFactory(redisConnectionFactory);
		template.setKeySerializer(keys);
		template.setValueSerializer(values);
		template.setHashKeySerializer(keys);
		template.setHashValueSerializer(values);
		
		
		return template;
	}*/
}

interface OrderRepository extends CrudRepository<Order, Long> {
	Collection<Order> findByWhen(Date d);
}

interface LineItemRepository extends CrudRepository<LineItem, Long> {
	
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@RedisHash("orders")
class Order implements Serializable {
	@Id
	private Long id;
	
	@Indexed
	private Date when;
	
	@Reference
	private List<LineItem> lineItems;
}

@RedisHash("lineItems")
@Data
@AllArgsConstructor
@NoArgsConstructor
class LineItem implements Serializable{
	@Indexed
	private Long orderId;
	
	@Id
	private Long id;
	
	private String description;
}

@Service
class OrderService {
	@Cacheable("order-by-id")
	public Order byId(Long id) {
		try {
			Thread.sleep(1000 *10);
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
		return new Order(id, new Date(), Collections.emptyList());
	}
}

class ShoppingCart implements Serializable{
	private final Collection <Order> orders  = new ArrayList<>();
	
	public void addOrder(Order order) {
		this.orders.add(order);
	}
	
	public Collection <Order> getOrders() {
		return orders;
	}
}
@Slf4j
@Controller
@SessionAttributes("cart")
class CartSessionController {
	private final AtomicLong ids = new AtomicLong();
	
	@ModelAttribute("cart")
	ShoppingCart cart() {
		log.info("Creating new cart");
		return new ShoppingCart();
	}
	
	@GetMapping("/orders")
	String orders (@ModelAttribute ("cart") ShoppingCart cart, Model model) {
		cart.addOrder(new Order(ids.incrementAndGet(), new Date(), Collections.emptyList()));
		model.addAttribute("orders", cart.getOrders());
		return "orders";
	}
}
