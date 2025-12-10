package com.jobserver.domain.port.out;

public interface ExternalServiceClientPort {
    ExternalServiceResponse processJob(String jobId);
}

