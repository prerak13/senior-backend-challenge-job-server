package com.jobserver.api.dto;

import com.jobserver.domain.model.Job;
import com.jobserver.domain.model.JobStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobResponse {
    private String jobId;
    private Long userId;
    private String username;
    private Long projectId;
    private String projectName;
    private JobStatus status;
    private String parameters;
    private String result;
    private String errorMessage;

    public static JobResponse from(Job job) {
        return JobResponse.builder()
                .jobId(job.getJobId())
                .userId(job.getUser().getId())
                .username(job.getUser().getUsername())
                .projectId(job.getProject() != null ? job.getProject().getId() : null)
                .projectName(job.getProject() != null ? job.getProject().getName() : null)
                .status(job.getStatus())
                .parameters(job.getParameters())
                .result(job.getResult())
                .errorMessage(job.getErrorMessage())
                .build();
    }
}

