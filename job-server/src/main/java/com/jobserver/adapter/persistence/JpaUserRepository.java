package com.jobserver.adapter.persistence;

import com.jobserver.domain.model.User;
import com.jobserver.domain.port.out.UserRepositoryPort;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
interface UserJpaRepository extends JpaRepository<User, Long> {
}

@org.springframework.stereotype.Repository
public class JpaUserRepository implements UserRepositoryPort {
    private final UserJpaRepository jpaRepository;

    public JpaUserRepository(UserJpaRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    @Override
    public Optional<User> findById(Long id) {
        return jpaRepository.findById(id);
    }
}

