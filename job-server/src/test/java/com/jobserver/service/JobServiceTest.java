package com.jobserver.service;

import com.jobserver.domain.model.Job;
import com.jobserver.domain.model.JobStatus;
import com.jobserver.domain.model.Project;
import com.jobserver.domain.model.User;
import com.jobserver.domain.port.in.ProcessJobUseCasePort;
import com.jobserver.domain.port.out.ExternalServiceClientPort;
import com.jobserver.domain.port.out.JobRepositoryPort;
import com.jobserver.domain.port.out.ProjectRepositoryPort;
import com.jobserver.domain.port.out.UserRepositoryPort;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobServiceTest {
    @Mock
    private JobRepositoryPort jobRepository;
    
    @Mock
    private UserRepositoryPort userRepository;
    
    @Mock
    private ProjectRepositoryPort projectRepository;
    
    @Mock
    private ExternalServiceClientPort externalServiceClient;
    
    @Mock
    private ProcessJobUseCasePort processJobUseCase;

    @InjectMocks
    private JobService jobService;

    private User testUser;
    private Project testProject;

    @BeforeEach
    void setUp() {
        testUser = User.builder()
                .id(1L)
                .username("alice")
                .email("alice@example.com")
                .build();

        testProject = Project.builder()
                .id(1L)
                .name("Project Alpha")
                .user(testUser)
                .build();
    }

    @Test
    void submitJob_WithValidUser_ReturnsJobWithPendingStatus() {
        // Given
        when(userRepository.findById(1L)).thenReturn(Optional.of(testUser));
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0);
            job.setId(1L);
            return job;
        });

        // When
        Job result = jobService.submitJob(1L, null, "test parameters");

        // Then
        assertNotNull(result);
        assertNotNull(result.getJobId());
        assertEquals(JobStatus.PENDING, result.getStatus());
        assertEquals("test parameters", result.getParameters());
        assertEquals(testUser, result.getUser());
        assertNull(result.getProject());
        
        verify(userRepository).findById(1L);
        verify(jobRepository).save(any(Job.class));
        verify(processJobUseCase).processJobAsync(any(String.class));
    }

    @Test
    void submitJob_WithValidUserAndProject_ReturnsJobWithProject() {
        // Given
        when(userRepository.findById(1L)).thenReturn(Optional.of(testUser));
        when(projectRepository.findById(1L)).thenReturn(Optional.of(testProject));
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0);
            job.setId(1L);
            return job;
        });

        // When
        Job result = jobService.submitJob(1L, 1L, "test parameters");

        // Then
        assertNotNull(result);
        assertEquals(testProject, result.getProject());
        
        verify(userRepository).findById(1L);
        verify(projectRepository).findById(1L);
        verify(jobRepository).save(any(Job.class));
        verify(processJobUseCase).processJobAsync(any(String.class));
    }

    @Test
    void submitJob_WithInvalidUser_ThrowsException() {
        // Given
        when(userRepository.findById(999L)).thenReturn(Optional.empty());

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> {
            jobService.submitJob(999L, null, "test parameters");
        });
        
        verify(userRepository).findById(999L);
        verify(jobRepository, never()).save(any());
        verify(processJobUseCase, never()).processJobAsync(any(String.class));
    }

    @Test
    void getJobByJobId_WithExistingJob_ReturnsJob() {
        // Given
        String jobId = "test-job-id";
        Job job = Job.builder()
                .id(1L)
                .jobId(jobId)
                .user(testUser)
                .status(JobStatus.PENDING)
                .build();
        
        when(jobRepository.findByJobId(jobId)).thenReturn(Optional.of(job));

        // When
        Optional<Job> result = jobService.getJobByJobId(jobId);

        // Then
        assertTrue(result.isPresent());
        assertEquals(jobId, result.get().getJobId());
        verify(jobRepository).findByJobId(jobId);
    }

    @Test
    void getJobByJobId_WithNonExistingJob_ReturnsEmpty() {
        // Given
        String jobId = "non-existent-job-id";
        when(jobRepository.findByJobId(jobId)).thenReturn(Optional.empty());

        // When
        Optional<Job> result = jobService.getJobByJobId(jobId);

        // Then
        assertTrue(result.isEmpty());
        verify(jobRepository).findByJobId(jobId);
    }
}

