package com.jobserver.domain.port.out;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExternalServiceResponse {
    private String jobId;
    private Integer value;
    private String status;
}

