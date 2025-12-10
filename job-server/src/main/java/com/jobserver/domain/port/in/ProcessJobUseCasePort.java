package com.jobserver.domain.port.in;

public interface ProcessJobUseCasePort {
    void processJobAsync(String jobId);
}

