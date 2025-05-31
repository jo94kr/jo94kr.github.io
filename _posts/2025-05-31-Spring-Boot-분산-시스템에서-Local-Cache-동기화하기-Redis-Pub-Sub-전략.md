---
title: Spring Boot 분산 시스템에서 Local Cache 동기화하기 – Redis Pub/Sub 전략
date: 2025-05-31 05:36:46 +0900
categories:
  - 아키텍처
tags:
  - redis
  - Redis-Pub/Sub
  - Local-Cache
  - 2-level-cache
  - cache
  - 분산-캐시-일관성
  - caffeine-cache
---

## 개요

분산 시스템에서 **Redis만으로 캐시를 처리하는 것은 한계가 있습니다.**  
그래서 많은 시스템은 **로컬 메모리 캐시(Caffeine 등)**와 Redis를 함께 사용하는 **2단계 캐싱(two-level caching)** 구조를 채택합니다.

하지만 이 구조는 **인스턴스 간 캐시 데이터의 불일치** 문제를 일으킬 수 있습니다.  
이 글에서는 이를 해결하기 위한 전략으로, **Redis Pub/Sub을 활용한 캐시 무효화 메시지 브로드캐스트 방식**을 소개하고, 구현 예제도 함께 설명합니다.

---

## 문제 정의: 왜 Local Cache는 불일치가 발생할까?

2단계 캐싱에서는 각 인스턴스가 **자체의 Local Cache를 유지**합니다.  
이로 인해 하나의 인스턴스에서 데이터가 변경되더라도, **다른 인스턴스는 이를 인지하지 못해 오래된 캐시를 사용할 수 있습니다.**

### 예시 시나리오

- 인스턴스 A에서 사용자 정보를 수정하고 해당 캐시를 삭제함
- 인스턴스 B는 이를 알지 못하고 기존 캐시 데이터를 그대로 사용함
- 이로 인해 **데이터 불일치 발생**

---

## 해결 전략 : Redis Pub/Sub으로 Cache 무효화 메시지 브로드캐스트

Redis의 Pub/Sub 기능을 활용하면, **데이터 변경 이벤트를 모든 인스턴스에 전파**할 수 있습니다.  
각 인스턴스는 이 메시지를 수신해 **자신의 Local Cache에서 해당 항목을 삭제(evict)**하게 됩니다.

### 왜 Pub/Sub인가?

- **구현이 간단**하며 Redis 자체 기능만으로 가능
- **외부 메시지 브로커(Kafka 등) 없이도 실시간 전파 가능**
- 실시간성이 중요하지만, **완전한 메시지 보장은 필요 없는 상황**에 적합

### 처리 흐름

1. 인스턴스 A에서 사용자 정보를 변경
2. 변경 후, Redis의 특정 채널에 캐시 무효화 메시지 발행
3. 모든 인스턴스는 해당 채널을 구독 중
4. 메시지를 수신한 인스턴스들이 해당 캐시 키를 Local Cache에서 삭제

---

## 구현 예제 (Spring Boot + Redis + Caffeine)

### 1. 캐시 무효화 메시지 발행 (Publisher)

> 데이터 변경 시 Redis 채널에 메시지를 발행합니다.

```java
@Component
@RequiredArgsConstructor
public class RedisCachePublisher {
    private final RedisTemplate<String, Object> redisTemplate;

    public void publishEviction(String cacheName, String key) {
        String message = cacheName + ":" + key;
        redisTemplate.convertAndSend(CACHE_EVICT_CHANNEL, message);
    }
}
```

### 2. 메시지 수신 및 Local Cache 삭제 (Subscriber)

> 메시지를 수신하면, Local Cache에서 해당 키를 삭제합니다.
> (필요에 따라 수정 로직 추가)

```java
@Component
@RequiredArgsConstructor
public class RedisCacheSubscriber implements MessageListener {

    private final LocalCacheManager localCacheManager;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String payload = new String(message.getBody(), StandardCharsets.UTF_8);
        String[] tokens = payload.split(":");
        if (tokens.length != 2) return;

        String cacheName = tokens[0];
        String key = tokens[1];
        
		Cache cache = cacheManager.getCache(cacheName);
        if (cache instanceof CaffeineCache caffeineCache) {
            caffeineCache.getNativeCache().invalidate(key);
        }
    }
}
```

### 3. Redis Listener 설정

> 인스턴스가 스케일 아웃되더라도 자동으로 구독할 수 있도록 `RedisMessageListenerContainer`를 설정합니다.

```java
@Configuration
public class RedisListenerConfig {

    @Bean
    public RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory, RedisCacheSubscriber subscriber) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(subscriber, new ChannelTopic(CACHE_EVICT_CHANNEL));
        return container;
    }
}
```

---

## 주의할 점

### 메시지 유실 가능성

- Redis Pub/Sub은 메시지 큐가 아니므로, **구독자가 일시적으로 오프라인일 경우 메시지가 유실될 수 있습니다.**
- **보다 강한 일관성이 필요한 경우**, Redis Streams나 Kafka 같은 메시지 큐 기반 시스템을 사용하는 것이 적합합니다.

### Redis 연결 불안정 시

- Redis 서버가 일시적으로 중단되면 Pub/Sub도 작동하지 않음
- 이 경우 Redis 재연결 및 메시지 재처리를 위한 재시도 전략 고려

---

## 결론: 간단하지만 충분히 효과적인 전략

이 구조는 아주 강력한 구조는 아닙니다. 하지만 **간단하고, 운영 중인 서비스에 부담 없이 적용할 수 있으며, 대부분의 문제 상황에서는 충분히 효과적**입니다.

- Redis Pub/Sub 기반 전략은 **구현이 간단하고 성능에도 영향을 주지 않으면서**, 일정 수준의 캐시 일관성을 유지할 수 있습니다.
- 특히 **API 서버 간 데이터 동기화가 필요하거나, 읽기 성능이 중요한 서비스에 적합**합니다.
- 완전한 동기화가 필요하지 않다면, **Caffeine + Redis 조합은 현실적이고 효과적인 선택**입니다.

---
