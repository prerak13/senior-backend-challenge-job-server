package com.jobserver.domain.port.out;

import com.jobserver.domain.model.User;

import java.util.Optional;

public interface UserRepositoryPort {
    Optional<User> findById(Long id);
}

