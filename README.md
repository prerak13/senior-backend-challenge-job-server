# Take-Home Challenge: Async Job Server 

## Overview

Build a job server that allows users to submit asynchronous jobs, processes them by calling an external service, and stores results in a database. The goal is to evaluate your skills in:

- Java
- Clean architecture (Hexagonal / Ports & Adapters)
- SOLID principles
- Async / parallel processing
- Database modeling with associations
- Dockerization

---

## Functional User Stories

### 1. Submit Job
- **Story:** As a user, I want to submit a job with parameters, so that it will be processed asynchronously.  
- **Acceptance Criteria:**  
  - Submission immediately returns a `jobId` and `status=PENDING`.  
  - Job is processed in the background without blocking the API request.  
  - The job must call an external endpoint:
    ```
    POST http://mock-external:8081/process
    Content-Type: application/json
    Body: { "jobId": "<your-job-id>" }
    ```
  - The endpoint returns a delayed random result; your service should store it in the database.

### 2. Retrieve Job Status / Result
- **Story:** As a user, I want to query the status and result of a submitted job.  
- **Acceptance Criteria:**  
  - Response includes `jobId`, `status` (`PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`), and any results or errors.  
  - If tasks are part of the job, they should be returned with their own `status` and metadata.

### 3. Linked Entities
- **Story:** As a user, I want jobs to be associated with my account and optionally a project.  
- **Acceptance Criteria:**  
  - Jobs reference a valid user (mandatory) and project (optional).  
  - Users and projects can be assumed to already exist in the database.  
  - Provide SQL insert statements (or migrations) for sample users and projects.  
  - Job retrieval returns associated user and project information.

### 4. Async Processing
- **Story:** As a system, jobs may involve calling a slow external service.  
- **Acceptance Criteria:**  
  - Multiple jobs can run in parallel without blocking API requests.  
  - Failures, timeouts, or slow responses in one job do not block others.  
  - Job results returned from `/process` must be persisted in the database.

---

## Non-Functional Requirements / Guidelines

### Architecture
- Write the project in Java in the framework of your choice
- Follow Hexagonal / Ports & Adapters principles.  
- Apply SOLID principles throughout (SRP, OCP, LSP, ISP, DIP).

### Persistence
- Use a relational database (MySQL or Postgres).  
- Model relationships using primary keys and foreign keys.  
- Provide insert data for users and projects.

### Async / Concurrency
- Jobs must be processed in parallel (worker pool, virtual threads, or asynchronous futures).  
- Handle errors, retries, or timeouts gracefully.

### Dockerization
- Provide a `Dockerfile` for your service.  
- Provide a `docker-compose.yml` to run:
  - DB (MySQL/Postgres)  
  - Mock external service at `http://mock-external:8081/process` (delayed random JSON response)

### Testing
- Provide unit tests for at least:
  - Job submission (happy path)  
  - Retrieving a job and its results

---

## Deliverables

1. Forked GitHub repository with:
   - Source code  
   - `Dockerfile` and `docker-compose.yml` modified with the candidate's service  
   - DB migrations and insert data for sample users/projects  
   - Unit tests
2. `README.md` with:
   - Instructions to run the service and tests  
   - Design notes (architecture decisions, async handling, failure strategies)  
   - Any assumptions made

---

## Optional Bonus
- Metrics/logging for jobs processed per minute or failure rates.  
- Generated OpenAPI 3 specification from code, or generate code from a spec

---

## Estimated Completion Time
~4–6 hours

---

## Implementation

This project has been implemented as a Java Spring Boot application following hexagonal architecture principles.

### Project Structure

```
job-server/
├── src/
│   ├── main/
│   │   ├── java/com/jobserver/
│   │   │   ├── domain/              # Domain layer (core business logic)
│   │   │   │   ├── model/           # Domain entities (Job, User, Project, JobStatus)
│   │   │   │   └── port/            # Ports (interfaces)
│   │   │   │       ├── in/          # Inbound ports (use cases)
│   │   │   │       │   ├── JobUseCasePort.java
│   │   │   │       │   └── ProcessJobUseCasePort.java
│   │   │   │       └── out/         # Outbound ports (repositories, external services)
│   │   │   │           ├── JobRepositoryPort.java
│   │   │   │           ├── ProjectRepositoryPort.java
│   │   │   │           ├── UserRepositoryPort.java
│   │   │   │           ├── ExternalServiceClientPort.java
│   │   │   │           └── ExternalServiceResponse.java
│   │   │   ├── adapter/             # Adapters (implementations)
│   │   │   │   ├── persistence/     # JPA repositories
│   │   │   │   │   ├── JpaJobRepository.java
│   │   │   │   │   ├── JpaProjectRepository.java
│   │   │   │   │   └── JpaUserRepository.java
│   │   │   │   ├── external/        # External service client
│   │   │   │   │   └── WebClientExternalServiceAdapter.java
│   │   │   │   └── processing/      # Async job processing adapter
│   │   │   │       └── AsyncProcessJobAdapter.java
│   │   │   ├── service/             # Application services (use case implementations)
│   │   │   │   └── JobService.java
│   │   │   ├── api/                 # REST controllers (inbound adapter)
│   │   │   │   ├── dto/             # Data transfer objects
│   │   │   │   │   ├── JobResponse.java
│   │   │   │   │   └── SubmitJobRequest.java
│   │   │   │   ├── JobController.java
│   │   │   │   └── GlobalExceptionHandler.java
│   │   │   ├── config/              # Configuration
│   │   │   │   ├── AsyncConfig.java
│   │   │   │   └── WebClientConfig.java
│   │   │   └── JobServerApplication.java
│   │   └── resources/
│   │       ├── application.yml      # Application configuration
│   │       └── import.sql           # Sample data
│   └── test/                        # Unit tests
│       └── java/com/jobserver/
│           ├── api/
│           │   └── JobControllerTest.java
│           └── service/
│               └── JobServiceTest.java
├── Dockerfile
└── pom.xml
```

#### Async Processing

**Implementation:**
- Uses Spring's `@Async` annotation with a custom `ThreadPoolTaskExecutor`
- Thread pool configuration:
  - **Core pool size**: 5 threads
  - **Max pool size**: 10 threads
  - **Queue capacity**: 100 jobs
  - **Thread naming**: `job-processor-{n}` for easy identification in logs

**How it works:**
1. When a job is submitted, it's immediately saved with `PENDING` status
2. The `submitJob()` method triggers `processJobAsync()` asynchronously
3. Uses self-injection pattern (`@Lazy ProcessJobUseCase`) to ensure `@Async` proxy works correctly
4. Each job runs in a separate thread from the pool
5. Jobs are processed in parallel without blocking API requests
6. Each job processing is isolated - failures in one job don't affect others

**Benefits:**
- Non-blocking API responses (immediate return with jobId)
- Parallel processing of multiple jobs
- Configurable concurrency (can adjust thread pool size)
- Graceful degradation (queue prevents overwhelming the system)

**Queue Full Behavior:**
When the thread pool queue is full (100 jobs queued) and all threads are busy (10 max threads):
- Spring's `ThreadPoolTaskExecutor` will throw a `RejectedExecutionException`
- The job is **already saved** in the database with `PENDING` status before the async call
- The exception propagates to the controller, resulting in a **500 Internal Server Error** response
- **Important**: The job remains in `PENDING` status and will **never be processed** automatically
- **Current capacity**: System can handle up to **110 concurrent jobs** (10 active threads + 100 queued)
- **Recommendation**: Monitor queue usage and consider:
  - Increasing `queueCapacity` or `maxPoolSize` for higher throughput
  - Implementing a `RejectedExecutionHandler` (e.g., `CallerRunsPolicy`) to handle overflow
  - Adding retry logic or a background job to process stuck `PENDING` jobs

#### Error Handling & Failure Strategies

**Retry Strategy:**
- **Retry count**: 3 attempts (initial + 2 retries)
- **Backoff strategy**: Exponential backoff starting at 1 second
- **Max backoff**: 5 seconds 
- **Retry conditions**:
  - 5xx server errors (transient server issues)
  - Timeout exceptions (network delays)
  - Connection errors (network connectivity issues)
  - IO exceptions (transient I/O failures)
- **No retry on**: 4xx client errors (bad requests won't be retried)

**Timeout Protection:**
- **Request timeout**: 30 seconds per attempt
- Prevents indefinite blocking on unresponsive external services
- Total maximum time: ~90 seconds (30s × 3 attempts)

**Error Handling:**
- Failed jobs are marked with `FAILED` status
- Error messages are stored in the `errorMessage` field
- All errors are logged with context (jobId, exception details)
- Global exception handler provides consistent HTTP error responses
- Transaction rollback ensures data consistency on failures

**Failure Isolation:**
- Each job's failure is contained - doesn't affect other jobs
- Database transactions ensure partial updates don't corrupt data
- External service failures are caught and stored, not propagated to API layer

## Running the Service

### Prerequisites
- **Docker Desktop** (must be running)
- **Java 21** (for local development only)
- **Maven 3.9+** (for local development only)

### Quick Start with Docker Compose (Recommended)

1. **Ensure Docker Desktop is running**

2. **Start all services:**
   ```bash
   docker-compose up --build
   ```
   This will:
   - Build the Spring Boot application
   - Start MySQL database (port 3306)
   - Start mock external service (port 8081)
   - Start job server (port 8080)

3. **Wait for services to be ready** (about 10-15 seconds for initial startup)

4. **Verify services are running:**
   ```bash
   docker-compose ps
   ```
   All three services should show "Up" status.

5. **Sample data**: Sample users and projects are automatically loaded from `import.sql` on application startup.

6. **Test the API:**
   ```bash
   # Submit a job
   curl -X POST http://localhost:8080/api/jobs \
     -H "Content-Type: application/json" \
     -d '{"userId": 1, "projectId": 1, "parameters": "test"}'
   
   # Get job status (replace {jobId} with the jobId from above)
   curl http://localhost:8080/api/jobs/{jobId}
   ```

7. **Stop services:**
   ```bash
   docker-compose down
   ```

### API Endpoints

#### 1. Submit a Job
```bash
POST /api/jobs
Content-Type: application/json

{
  "userId": 1,
  "projectId": 1,  # optional
  "parameters": "optional job parameters"
}
```

**Response:**
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": 1,
  "username": "alice",
  "projectId": 1,
  "projectName": "Project Alpha",
  "status": "PENDING",
  "parameters": "optional job parameters",
  "result": null,
  "errorMessage": null
}
```

#### 2. Get Job Status
```bash
GET /api/jobs/{jobId}
```

**Response:**
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": 1,
  "username": "alice",
  "projectId": 1,
  "projectName": "Project Alpha",
  "status": "COMPLETED",
  "parameters": "optional job parameters",
  "result": "{\"jobId\":\"...\",\"value\":123,\"status\":\"DONE\"}",
  "errorMessage": null
}
```

### Database Schema Design

**Tables:**
- `users`: User accounts (id, username, email)
- `projects`: User projects (id, name, description, user_id)
- `jobs`: Job records (id, job_id, user_id, project_id, status, parameters, result, error_message)

**Constraints:**
- Foreign key constraints ensure referential integrity
- `ON DELETE CASCADE` for users (deleting user deletes their jobs)
- `ON DELETE SET NULL` for projects (deleting project doesn't delete jobs)
- Unique constraint on `job_id` (business identifier)

**Indexes:**
- `idx_job_id`: Fast lookup by jobId (most common query)
- `idx_user_id`: Fast lookup of user's jobs
- `idx_status`: Fast filtering by status

**Timestamps:**
- `created_at`: When job was created (audit trail)
- `updated_at`: When job was last updated (tracks status changes)

### Async Processing Design

**Why @Async with Thread Pool:**
- Spring's `@Async` provides simple annotation-based async execution
- Thread pool gives control over concurrency and resource usage
- Better than virtual threads for this use case (more predictable, easier to configure)

### Failure Strategy Details

**Retry Logic:**
- **Why exponential backoff:** Prevents overwhelming a struggling service
- **Why max backoff:** Prevents excessive wait times (caps at 5 seconds)
- **Selective retries:** Only retries transient failures (5xx, timeouts, network errors)
- **No retry on 4xx:** Client errors won't succeed on retry

**Error Storage:**
- Errors are stored in database (`errorMessage` field)
- Allows users to query failed jobs and see what went wrong
- Enables future features like retry mechanisms or error analysis

**Timeout Strategy:**
- 30 seconds per attempt
- Prevents indefinite blocking
- Total max time (30x3=90s)

**Isolation:**
- Each job's failure is independent
- Database transactions ensure partial updates don't corrupt data
- No shared state between job processing threads

### Concurrency Considerations

**Thread Safety:**
- No shared mutable state in job processing
- Each job is processed independently
- Database transactions provide isolation

**Scalability:**
- Thread pool can be adjusted based on load
- Queue prevents overwhelming the system
- Can scale horizontally (multiple instances) if needed (with shared database)

**Resource Management:**
- Thread pool limits concurrent processing
- Queue prevents unbounded memory growth
- Database connections are managed by HikariCP connection pool

### Testing

Unit tests cover:
- Job submission with valid/invalid users
- Job submission with/without projects
- Job retrieval by jobId
- Error handling scenarios

Test files:
- `JobServiceTest.java` - Service layer tests
- `JobControllerTest.java` - API layer tests

### Future Enhancements

Potential improvements:
- Job retry mechanism with configurable retry policies
- Job cancellation endpoint
- Pagination for job listing
- Metrics and monitoring (Prometheus, Actuator)
- OpenAPI/Swagger documentation
- Job priority queues
- Scheduled jobs

---

## Testing the API with Postman/Curl

### 1. Submit a Job
- **Request type**: POST
- **URL:** `http://localhost:8080/api/jobs`
- **Body:**
  ```json
  {
    "userId": 1,
    "projectId": 1,
    "parameters": "test parameters"
  }
  ```

### 2. Get Job Status
- **Request type**: GET
- **URL:** `http://localhost:8080/api/jobs/{jobId}` (replace `{jobId}` with the value from above)
- **Send the request.**
- You should receive a job status JSON response. If the job is still `PROCESSING`, wait and try again until it is `COMPLETED` or `FAILED`.
