package com.jobserver.api;

import com.jobserver.api.dto.JobResponse;
import com.jobserver.api.dto.SubmitJobRequest;
import com.jobserver.domain.model.Job;
import com.jobserver.domain.port.in.JobUseCasePort;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/jobs")
public class JobController {
    private final JobUseCasePort jobUseCase;

    public JobController(JobUseCasePort jobUseCase) {
        this.jobUseCase = jobUseCase;
    }

    @PostMapping
    public ResponseEntity<JobResponse> submitJob(@Valid @RequestBody SubmitJobRequest request) {
        Job job = jobUseCase.submitJob(
                request.getUserId(),
                request.getProjectId(),
                request.getParameters()
        );
        return ResponseEntity.status(HttpStatus.CREATED).body(JobResponse.from(job));
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<JobResponse> getJob(@PathVariable String jobId) {
        return jobUseCase.getJobByJobId(jobId)
                .map(job -> ResponseEntity.ok(JobResponse.from(job)))
                .orElse(ResponseEntity.notFound().build());
    }
}

