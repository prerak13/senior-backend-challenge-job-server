package com.jobserver.api.dto;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class SubmitJobRequest {
    @NotNull(message = "userId is required")
    private Long userId;
    
    private Long projectId;
    
    private String parameters;
}

