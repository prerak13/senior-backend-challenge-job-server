package com.jobserver.domain.port.in;

import com.jobserver.domain.model.Job;

import java.util.Optional;

public interface JobUseCasePort {
    Job submitJob(Long userId, Long projectId, String parameters);
    Optional<Job> getJobByJobId(String jobId);
}
