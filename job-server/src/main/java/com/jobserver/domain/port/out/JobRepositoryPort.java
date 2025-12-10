package com.jobserver.domain.port.out;

import com.jobserver.domain.model.Job;

import java.util.Optional;

public interface JobRepositoryPort {
    Job save(Job job);
    Optional<Job> findByJobId(String jobId);
    Job update(Job job);
}
