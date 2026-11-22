package ru.delivery;

import jakarta.validation.constraints.NotBlank;
import lombok.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.*;
import org.springframework.http.MediaType;
import org.springframework.stereotype.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootApplication
public class ReactiveDeliveryApplication {
    public static void main(String[] args) {
        SpringApplication.run(ReactiveDeliveryApplication.class, args);
    }
}

/* ======================= МОДЕЛЬ ======================= */

enum DeliveryStatus { CREATED, ACCEPTED, IN_TRANSIT, DELIVERED }

@Data @Builder @NoArgsConstructor @AllArgsConstructor
class Delivery {
    private String id;
    private String orderId;      // id заказа из Order Service
    private String address;
    private DeliveryStatus status;
    private LocalDateTime updatedAt;
}

/* ======================= DTO для Order Service ======================= */

@Data @NoArgsConstructor @AllArgsConstructor
class OrderInfo {
    private String orderId;
    private String customer;
    private String item;
}

/* ======================= ОШИБКИ ======================= */

class NotFoundException extends RuntimeException {
    public NotFoundException(String msg) { super(msg); }
}

@RestControllerAdvice
class GlobalExceptionHandler {
    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(org.springframework.http.HttpStatus.NOT_FOUND)
    public Map<String, String> notFound(NotFoundException e) {
        return Map.of("error", e.getMessage());
    }
}

/* ======================= РЕПОЗИТОРИЙ (in-memory) ======================= */

@Repository
class DeliveryRepository {
    private final Map<String, Delivery> store = new ConcurrentHashMap<>();

    public Flux<Delivery> findAll() { return Flux.fromIterable(store.values()); }

    public Mono<Delivery> findById(String id) {
        return Mono.justOrEmpty(store.get(id));
    }

    public Mono<Delivery> save(Delivery d) {
        if (d.getId() == null) d.setId(UUID.randomUUID().toString());
        d.setUpdatedAt(LocalDateTime.now());
        store.put(d.getId(), d);
        return Mono.just(d);
    }

    public Mono<Delivery> update(String id, Delivery d) {
        return findById(id)
                .flatMap(existing -> {
                    d.setId(id);
                    d.setUpdatedAt(LocalDateTime.now());
                    store.put(id, d);
                    return Mono.just(d);
                });
    }

    public Mono<Void> deleteById(String id) {
        return Mono.fromRunnable(() -> store.remove(id)).then();
    }
}

/* ======================= СЕРВИС ======================= */

@Service
@RequiredArgsConstructor
class DeliveryService {

    private final DeliveryRepository repository;
    private final WebClient orderClient;

    // "Шина" событий: id доставки -> Sinks.Many<Delivery>
    private final Map<String, Sinks.Many<Delivery>> streams = new ConcurrentHashMap<>();

    private Sinks.Many<Delivery> sinkFor(String id) {
        return streams.computeIfAbsent(id,
                k -> Sinks.many().multicast().onBackpressureBuffer());
    }

    /** Один объект — Mono<Delivery>. Демонстрируем switchIfEmpty + onErrorResume */
    public Mono<Delivery> getById(String id) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new NotFoundException("Delivery " + id + " not found")))
                .onErrorResume(NotFoundException.class, Mono::error) // явно, для наглядности
                .map(this::decorate);
    }

    /** Много объектов — Flux<Delivery>. Демонстрируем filter + map */
    public Flux<Delivery> getAll(String status) {
        return repository.findAll()
                .filter(d -> status == null || d.getStatus().name().equalsIgnoreCase(status))
                .map(this::decorate)
                .defaultIfEmpty(Delivery.builder().id("none").address("empty").build())
                .filter(d -> !"none".equals(d.getId()));
    }

    private Delivery decorate(Delivery d) {
        // просто копия — чтобы показать map в цепочке
        return Delivery.builder()
                .id(d.getId()).orderId(d.getOrderId()).address(d.getAddress())
                .status(d.getStatus()).updatedAt(d.getUpdatedAt()).build();
    }

    /** Создание: сначала внешний вызов Order Service (WebClient), затем сохранение */
    public Mono<Delivery> create(Delivery d) {
        return fetchOrder(d.getOrderId())
                .onErrorResume(ex -> {
                    // Если Order Service недоступен — не падаем, идём дальше
                    System.err.println("Order Service error: " + ex.getMessage());
                    return Mono.empty();
                })
                .flatMap(order -> {
                    d.setAddress(order.getItem() != null
                            ? order.getItem() + " -> " + d.getAddress()
                            : d.getAddress());
                    d.setStatus(DeliveryStatus.CREATED);
                    return repository.save(d);
                })
                .switchIfEmpty(Mono.defer(() -> {
                    d.setStatus(DeliveryStatus.CREATED);
                    return repository.save(d);
                }))
                .doOnNext(saved -> sinkFor(saved.getId()).tryEmitNext(saved));
    }

    public Mono<Delivery> update(String id, Delivery d) {
        return repository.update(id, d)
                .switchIfEmpty(Mono.error(new NotFoundException("Delivery " + id + " not found")))
                .doOnNext(saved -> sinkFor(id).tryEmitNext(saved));
    }

    public Mono<Void> delete(String id) {
        return repository.deleteById(id)
                .then(Mono.fromRunnable(() -> {
                    Sinks.Many<Delivery> sink = streams.remove(id);
                    if (sink != null) sink.tryEmitComplete();
                }))
                .then();
    }

    /** Поток обновлений статуса через SSE */
    public Flux<Delivery> streamStatus(String id, DeliveryStatus newStatus) {
        // Эмулятор внешнего «драйвера»: генерируем статусы по таймеру, если запрошен переход
        Flux<Delivery> generated = Flux.interval(Duration.ofSeconds(2))
                .take(4)
                .map(i -> DeliveryStatus.values()[i.intValue() % DeliveryStatus.values().length])
                .flatMap(st -> repository.findById(id)
                        .flatMap(d -> {
                            d.setStatus(st);
                            d.setUpdatedAt(LocalDateTime.now());
                            return repository.save(d);
                        }))
                .doOnNext(saved -> sinkFor(id).tryEmitNext(saved));

        // Клиент получает и уже существующие события, и новые
        return Flux.concat(
                        repository.findById(id).flux(),
                        sinkFor(id).asFlux()
                ).mergeWith(generated.takeUntil(d -> d.getStatus() == DeliveryStatus.DELIVERED))
                .distinct(Delivery::getStatus)
                .subscribeOn(Schedulers.parallel());
    }

    /** Внешний реактивный вызов Order Service */
    private Mono<OrderInfo> fetchOrder(String orderId) {
        return orderClient.get()
                .uri("/api/orders/{id}", orderId)
                .retrieve()
                .bodyToMono(OrderInfo.class)
                .timeout(Duration.ofSeconds(2));
    }
}

/* ======================= КОНТРОЛЛЕР ======================= */

@RestController
@RequestMapping("/api/deliveries")
@RequiredArgsConstructor
class DeliveryController {

    private final DeliveryService service;

    @GetMapping
    public Flux<Delivery> getAll(@RequestParam(required = false) String status) {
        return service.getAll(status);
    }

    @GetMapping("/{id}")
    public Mono<Delivery> getById(@PathVariable String id) {
        return service.getById(id);
    }

    @PostMapping
    @ResponseStatus(org.springframework.http.HttpStatus.CREATED)
    public Mono<Delivery> create(@RequestBody Delivery d) {
        return service.create(d);
    }

    @PutMapping("/{id}")
    public Mono<Delivery> update(@PathVariable String id, @RequestBody Delivery d) {
        return service.update(id, d);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(org.springframework.http.HttpStatus.NO_CONTENT)
    public Mono<Void> delete(@PathVariable String id) {
        return service.delete(id);
    }

    /** SSE: поток изменения статуса доставки */
    @GetMapping(value = "/{id}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Delivery> stream(@PathVariable String id) {
        return service.streamStatus(id, DeliveryStatus.CREATED);
    }
}

/* ======================= КОНФИГУРАЦИЯ WebClient ======================= */

@Configuration
class WebClientConfig {
    @Bean
    public WebClient orderClient(@Value("${order.service.url:http://localhost:8081}") String baseUrl) {
        return WebClient.builder().baseUrl(baseUrl).build();
    }
}