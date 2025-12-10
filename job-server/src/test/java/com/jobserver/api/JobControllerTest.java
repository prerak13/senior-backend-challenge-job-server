package com.jobserver.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobserver.api.dto.SubmitJobRequest;
import com.jobserver.domain.model.Job;
import com.jobserver.domain.model.JobStatus;
import com.jobserver.domain.model.User;
import com.jobserver.domain.port.in.JobUseCasePort;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(JobController.class)
class JobControllerTest {
    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private JobUseCasePort jobUseCase;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void submitJob_WithValidRequest_ReturnsCreated() throws Exception {
        // Given
        SubmitJobRequest request = new SubmitJobRequest();
        request.setUserId(1L);
        request.setParameters("test params");

        User user = User.builder().id(1L).username("alice").email("alice@example.com").build();
        Job job = Job.builder()
                .id(1L)
                .jobId("test-job-id")
                .user(user)
                .status(JobStatus.PENDING)
                .parameters("test params")
                .build();

        when(jobUseCase.submitJob(any(), any(), any())).thenReturn(job);

        // When/Then
        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.jobId").value("test-job-id"))
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    void getJob_WithExistingJobId_ReturnsJob() throws Exception {
        // Given
        String jobId = "test-job-id";
        User user = User.builder().id(1L).username("alice").email("alice@example.com").build();
        Job job = Job.builder()
                .id(1L)
                .jobId(jobId)
                .user(user)
                .status(JobStatus.COMPLETED)
                .result("{\"value\":123}")
                .build();

        when(jobUseCase.getJobByJobId(jobId)).thenReturn(Optional.of(job));

        // When/Then
        mockMvc.perform(get("/api/jobs/{jobId}", jobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobId").value(jobId))
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.result").value("{\"value\":123}"));
    }

    @Test
    void getJob_WithNonExistingJobId_ReturnsNotFound() throws Exception {
        // Given
        String jobId = "non-existent-job-id";
        when(jobUseCase.getJobByJobId(jobId)).thenReturn(Optional.empty());

        // When/Then
        mockMvc.perform(get("/api/jobs/{jobId}", jobId))
                .andExpect(status().isNotFound());
    }
}

