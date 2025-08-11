# REST API Authentication Design

## Overview

This document outlines the design for adding Bearer token authentication to the Moonlink REST API. The authentication system will be optional, configurable, and provide basic security for production deployments while maintaining simplicity for development and demo scenarios.

## Current Architecture Analysis

Based on the current REST API implementation in `src/moonlink_service/src/rest_api.rs`, the system uses:

- **Axum framework** for HTTP routing and middleware
- **Tower HTTP** for CORS middleware 
- **State-based architecture** with `ApiState` containing shared backend reference
- **Three main endpoints**:
  - `GET /health` - Health check (should remain public)
  - `POST /tables/:table` - Table creation (requires auth when enabled)
  - `POST /ingest/:table` - Data ingestion (requires auth when enabled)

## Design Goals

1. **Optional Authentication**: System works without auth for dev/demo, enabled for production
2. **Simple Bearer Token**: Use industry-standard `Authorization: Bearer <token>` header
3. **Configurable**: Support both environment variables and configuration parameters
4. **Selective Protection**: Health endpoint remains public, mutating operations require auth
5. **Clear Error Responses**: Return proper 401 Unauthorized responses with clear messages

## Architecture Design

### 1. Authentication Middleware

Create an Axum middleware layer that:
- Checks for `Authorization: Bearer <token>` header on protected endpoints
- Validates the token against the configured value
- Returns 401 Unauthorized for invalid/missing tokens
- Allows requests through for public endpoints

```rust
// In src/moonlink_service/src/rest_api.rs
use axum::{
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};

struct AuthConfig {
    token: Option<String>,
}

async fn auth_middleware<B>(
    req: Request<B>,
    next: Next<B>,
) -> Result<Response, StatusCode> {
    // Implementation details below
}
```

### 2. Configuration Integration

Extend `ServiceConfig` in `src/moonlink_service/src/lib.rs`:

```rust
pub struct ServiceConfig {
    // ... existing fields ...
    
    /// Optional authentication token for REST API endpoints
    /// When None, authentication is disabled
    pub rest_auth_token: Option<String>,
}
```

### 3. Environment Variable Support

Support configuration via environment variable:
- `MOONLINK_REST_AUTH_TOKEN` - When set, enables authentication with this token value

### 4. Protected vs Public Endpoints

- **Public (no auth required)**:
  - `GET /health` - Always accessible for health checks and monitoring
  - `GET /ready` - Readiness probe endpoint (in main service)

- **Protected (auth required when enabled)**:
  - `POST /tables/:table` - Table creation
  - `POST /ingest/:table` - Data ingestion
  - Any future mutating endpoints

## Implementation Details

### 1. Middleware Implementation

```rust
use axum::{
    extract::Request,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Json, Response},
};

#[derive(Clone)]
struct AuthConfig {
    required_token: Option<String>,
}

async fn auth_middleware(
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Result<Response, impl IntoResponse> {
    let auth_config = /* extract from request extensions */;
    
    // Skip auth if not configured
    let Some(required_token) = &auth_config.required_token else {
        return Ok(next.run(req).await);
    };
    
    // Check Authorization header
    let auth_header = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok());
    
    match auth_header {
        Some(header) if header.starts_with("Bearer ") => {
            let token = &header[7..]; // Skip "Bearer "
            if token == required_token {
                Ok(next.run(req).await)
            } else {
                Err(unauthorized_response("Invalid token"))
            }
        }
        Some(_) => Err(unauthorized_response("Invalid authorization format")),
        None => Err(unauthorized_response("Authorization header required")),
    }
}

fn unauthorized_response(message: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(ErrorResponse {
            error: "unauthorized".to_string(),
            message: message.to_string(),
        }),
    )
}
```

### 2. Router Configuration

Update `create_router` function to conditionally apply auth middleware:

```rust
pub fn create_router(state: ApiState, auth_token: Option<String>) -> Router {
    let mut router = Router::new()
        .route("/health", get(health_check)) // Public endpoint
        .route("/tables/:table", post(create_table))
        .route("/ingest/:table", post(ingest_data))
        .with_state(state);

    // Apply auth middleware to protected routes if token is configured
    if let Some(token) = auth_token {
        let auth_config = AuthConfig {
            required_token: Some(token),
        };
        
        router = router
            .route_layer(axum::middleware::from_fn_with_state(
                auth_config,
                auth_middleware
            ));
    }
    
    router.layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods([Method::GET, Method::POST])
            .allow_headers(Any),
    )
}
```

### 3. Service Integration

Update service startup in `src/moonlink_service/src/lib.rs`:

```rust
impl ServiceConfig {
    pub fn from_env() -> Self {
        Self {
            // ... existing field initialization ...
            rest_auth_token: std::env::var("MOONLINK_REST_AUTH_TOKEN").ok(),
        }
    }
}

// In start_with_config function:
let rest_api_handle = if let Some(port) = config.rest_api_port {
    let api_state = rest_api::ApiState::new(backend.clone());
    let router = rest_api::create_router(api_state, config.rest_auth_token.clone());
    // ... rest of server startup
};
```

## Security Considerations

### 1. Token Management
- Tokens should be randomly generated, high-entropy strings (recommend 32+ characters)
- Support for token rotation by restarting service with new token
- Consider logging authentication failures for security monitoring

### 2. Transport Security
- Authentication tokens are transmitted in headers, requires HTTPS in production
- Document requirement for TLS termination (load balancer, reverse proxy, etc.)

### 3. Token Storage
- Environment variables are appropriate for simple deployment scenarios
- For advanced deployments, consider integration with secret management systems
- Never log or expose the configured token value

## Error Handling

### 1. Authentication Errors
Return consistent 401 responses with descriptive error messages:

```json
{
  "error": "unauthorized",
  "message": "Authorization header required"
}
```

### 2. Error Message Types
- `"Authorization header required"` - Missing Authorization header
- `"Invalid authorization format"` - Header present but not "Bearer <token>" format
- `"Invalid token"` - Token present but doesn't match configured value

## Testing Strategy

### 1. Unit Tests
- Test auth middleware with valid tokens
- Test auth middleware with invalid/missing tokens
- Test bypassing auth when not configured
- Test public endpoints remain accessible

### 2. Integration Tests
- End-to-end API calls with and without authentication
- Environment variable configuration testing
- CORS compatibility with auth headers

## Usage Examples

### 1. Development (No Auth)
```bash
# Start service without authentication
MOONLINK_REST_API_PORT=8080 moonlink_service

# API calls work without headers
curl -X POST http://localhost:8080/tables/demo \
  -H "Content-Type: application/json" \
  -d '{"database_id": 1, "table_id": 1, "schema": [...]}'
```

### 2. Production (With Auth)
```bash
# Start service with authentication
MOONLINK_REST_AUTH_TOKEN=abc123xyz789 \
MOONLINK_REST_API_PORT=8080 \
moonlink_service

# API calls require Bearer token
curl -X POST http://localhost:8080/tables/demo \
  -H "Authorization: Bearer abc123xyz789" \
  -H "Content-Type: application/json" \
  -d '{"database_id": 1, "table_id": 1, "schema": [...]}'

# Health check remains public
curl http://localhost:8080/health
```

## Dependencies

Add the following to `Cargo.toml` if needed:
- No additional dependencies required beyond existing Axum and Tower HTTP

## Migration Path

1. **Phase 1**: Implement auth middleware with optional configuration
2. **Phase 2**: Add environment variable support and documentation
3. **Phase 3**: Add integration tests and production deployment guides
4. **Phase 4**: Consider advanced features (multiple tokens, token rotation, audit logging)

## Future Enhancements

Potential future improvements (out of scope for initial implementation):

1. **Multiple Tokens**: Support for multiple valid tokens (different clients/services)
2. **Token Scopes**: Different permission levels (read-only vs write access)
3. **JWT Support**: Structured tokens with expiration and claims
4. **API Key Authentication**: Alternative to Bearer tokens
5. **Audit Logging**: Log all authentication attempts and API usage
6. **Rate Limiting**: Per-token rate limiting for abuse prevention

## Files to Modify

1. `src/moonlink_service/src/lib.rs` - Add auth_token to ServiceConfig
2. `src/moonlink_service/src/rest_api.rs` - Implement auth middleware and update router
3. `src/moonlink_service/Cargo.toml` - No changes required
4. Documentation/examples - Update with auth examples

This design provides a solid foundation for REST API authentication while maintaining flexibility and simplicity appropriate for the Moonlink project's needs.