package com.jobserver.adapter.persistence;

import com.jobserver.domain.model.Job;
import com.jobserver.domain.port.out.JobRepositoryPort;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
interface JobJpaRepository extends JpaRepository<Job, Long> {
    @Query("SELECT j FROM Job j LEFT JOIN FETCH j.user LEFT JOIN FETCH j.project WHERE j.jobId = :jobId")
    Optional<Job> findByJobId(@Param("jobId") String jobId);
}

@org.springframework.stereotype.Repository
public class JpaJobRepository implements JobRepositoryPort {
    private final JobJpaRepository jpaRepository;

    public JpaJobRepository(JobJpaRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    @Override
    public Job save(Job job) {
        return jpaRepository.save(job);
    }

    @Override
    public Optional<Job> findByJobId(String jobId) {
        return jpaRepository.findByJobId(jobId);
    }

    @Override
    public Job update(Job job) {
        return jpaRepository.save(job);
    }
}

