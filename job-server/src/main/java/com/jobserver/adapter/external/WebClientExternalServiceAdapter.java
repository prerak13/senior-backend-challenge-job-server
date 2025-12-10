package com.jobserver.adapter.external;

import com.jobserver.domain.port.out.ExternalServiceClientPort;
import com.jobserver.domain.port.out.ExternalServiceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.util.retry.Retry;

import java.time.Duration;

@Component
public class WebClientExternalServiceAdapter implements ExternalServiceClientPort {
    private static final Logger logger = LoggerFactory.getLogger(WebClientExternalServiceAdapter.class);
    private final WebClient webClient;

    public WebClientExternalServiceAdapter(
            WebClient.Builder webClientBuilder,
            @Value("${external.service.url}") String externalServiceUrl) {
        this.webClient = webClientBuilder
                .baseUrl(externalServiceUrl)
                .build();
    }

    @Override
    public ExternalServiceResponse processJob(String jobId) {
        try {
            return webClient.post()
                    .uri("/process")
                    .bodyValue(new ProcessRequest(jobId))
                    .retrieve()
                    .bodyToMono(ExternalServiceResponse.class)
                    .timeout(Duration.ofSeconds(30))
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                            .maxBackoff(Duration.ofSeconds(5))
                            .filter(throwable -> {
                                // Retry on 5xx server errors
                                if (throwable instanceof WebClientResponseException ex) {
                                    return ex.getStatusCode().is5xxServerError();
                                }
                                // Retry on timeouts and network errors (transient failures)
                                return throwable instanceof java.util.concurrent.TimeoutException
                                        || throwable instanceof java.net.ConnectException
                                        || throwable instanceof java.io.IOException;
                            })
                            .doBeforeRetry(retrySignal -> 
                                logger.warn("Retrying job {} after failure: {}", jobId, retrySignal.failure().getMessage())))
                    .block();
        } catch (Exception e) {
            throw new ExternalServiceException("Failed to process job: " + jobId, e);
        }
    }

    private record ProcessRequest(String jobId) {}
}

class ExternalServiceException extends RuntimeException {
    public ExternalServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}

