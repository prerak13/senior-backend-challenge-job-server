package com.jobserver.service;

import com.jobserver.domain.model.Job;
import com.jobserver.domain.model.JobStatus;
import com.jobserver.domain.model.Project;
import com.jobserver.domain.model.User;
import com.jobserver.domain.port.in.JobUseCasePort;
import com.jobserver.domain.port.in.ProcessJobUseCasePort;
import com.jobserver.domain.port.out.JobRepositoryPort;
import com.jobserver.domain.port.out.ProjectRepositoryPort;
import com.jobserver.domain.port.out.UserRepositoryPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
public class JobService implements JobUseCasePort {
    private static final Logger logger = LoggerFactory.getLogger(JobService.class);

    private final JobRepositoryPort jobRepository;
    private final UserRepositoryPort userRepository;
    private final ProjectRepositoryPort projectRepository;
    private final ProcessJobUseCasePort processJobUseCase;

    public JobService(
            JobRepositoryPort jobRepository,
            UserRepositoryPort userRepository,
            ProjectRepositoryPort projectRepository,
            ProcessJobUseCasePort processJobUseCase) {
        this.jobRepository = jobRepository;
        this.userRepository = userRepository;
        this.projectRepository = projectRepository;
        this.processJobUseCase = processJobUseCase;
    }

    @Override
    @Transactional
    public Job submitJob(Long userId, Long projectId, String parameters) {
        // Validate user exists
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new IllegalArgumentException("User not found: " + userId));

        // Validate project if provided
        Project project = null;
        if (projectId != null) {
            project = projectRepository.findById(projectId)
                    .orElseThrow(() -> new IllegalArgumentException("Project not found: " + projectId));
        }

        // Create job
        String jobId = UUID.randomUUID().toString();
        Job job = Job.builder()
                .jobId(jobId)
                .user(user)
                .project(project)
                .status(JobStatus.PENDING)
                .parameters(parameters)
                .build();

        Job savedJob = jobRepository.save(job);
        logger.info("Job submitted: {}", jobId);

        // Process job asynchronously using the adapter
        processJobUseCase.processJobAsync(jobId);

        // Re-fetch with eagerly loaded relationships to avoid LazyInitializationException
        return jobRepository.findByJobId(jobId)
                .orElseThrow(() -> new IllegalStateException("Job not found immediately after save - this should never happen: " + jobId));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Job> getJobByJobId(String jobId) {
        return jobRepository.findByJobId(jobId);
    }
}

