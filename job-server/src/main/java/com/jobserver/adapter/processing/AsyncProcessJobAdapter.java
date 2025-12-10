package com.jobserver.adapter.processing;

import com.jobserver.domain.model.Job;
import com.jobserver.domain.model.JobStatus;
import com.jobserver.domain.port.in.ProcessJobUseCasePort;
import com.jobserver.domain.port.out.ExternalServiceClientPort;
import com.jobserver.domain.port.out.ExternalServiceResponse;
import com.jobserver.domain.port.out.JobRepositoryPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Component
public class AsyncProcessJobAdapter implements ProcessJobUseCasePort {
    private static final Logger logger = LoggerFactory.getLogger(AsyncProcessJobAdapter.class);

    private final JobRepositoryPort jobRepository;
    private final ExternalServiceClientPort externalServiceClient;

    public AsyncProcessJobAdapter(
            JobRepositoryPort jobRepository,
            ExternalServiceClientPort externalServiceClient) {
        this.jobRepository = jobRepository;
        this.externalServiceClient = externalServiceClient;
    }

    @Override
    @Async("taskExecutor")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processJobAsync(String jobId) {
        logger.info("Starting async processing for job: {}", jobId);
        
        Optional<Job> jobOpt = jobRepository.findByJobId(jobId);
        if (jobOpt.isEmpty()) {
            logger.error("Job not found: {}", jobId);
            return;
        }

        Job job = jobOpt.get();
        job.setStatus(JobStatus.PROCESSING);
        jobRepository.update(job);

        try {
            // Call external service
            ExternalServiceResponse response = externalServiceClient.processJob(jobId);
            
            // Update job with result
            job.setStatus(JobStatus.COMPLETED);
            job.setResult(String.format("{\"jobId\":\"%s\",\"value\":%d,\"status\":\"%s\"}", 
                    response.getJobId(), response.getValue(), response.getStatus()));
            jobRepository.update(job);
            
            logger.info("Job completed successfully: {}", jobId);
        } catch (Exception e) {
            logger.error("Job processing failed: {}", jobId, e);
            job.setStatus(JobStatus.FAILED);
            job.setErrorMessage(e.getMessage());
            jobRepository.update(job);
        }
    }
}

