package com.jobserver.domain.port.out;

import com.jobserver.domain.model.Project;

import java.util.Optional;

public interface ProjectRepositoryPort {
    Optional<Project> findById(Long id);
}

