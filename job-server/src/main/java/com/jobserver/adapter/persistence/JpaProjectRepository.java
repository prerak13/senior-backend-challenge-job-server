package com.jobserver.adapter.persistence;

import com.jobserver.domain.model.Project;
import com.jobserver.domain.port.out.ProjectRepositoryPort;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
interface ProjectJpaRepository extends JpaRepository<Project, Long> {
}

@org.springframework.stereotype.Repository
public class JpaProjectRepository implements ProjectRepositoryPort {
    private final ProjectJpaRepository jpaRepository;

    public JpaProjectRepository(ProjectJpaRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    @Override
    public Optional<Project> findById(Long id) {
        return jpaRepository.findById(id);
    }
}

