package teranet.mapdev.ingest.config;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

/**
 * Servlet filter that generates or extracts correlation ID for request tracking.
 * The correlation ID is propagated throughout the entire request lifecycle using SLF4J MDC.
 * 
 * Best Practices:
 * - Generates unique correlation ID for each request
 * - Accepts existing correlation ID from X-Correlation-ID header (for distributed tracing)
 * - Adds correlation ID to response headers for client tracking
 * - Cleans up MDC after request completion to prevent memory leaks
 */
@Component
@Order(1) // Execute first in filter chain
public class CorrelationIdFilter implements Filter {
    
    private static final Logger logger = LoggerFactory.getLogger(CorrelationIdFilter.class);
    
    // MDC key for correlation ID
    public static final String CORRELATION_ID_KEY = "correlationId";
    
    // HTTP header for correlation ID
    public static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        
        try {
            // Generate or extract correlation ID
            String correlationId = getOrGenerateCorrelationId(httpRequest);
            
            // Store in MDC for logging throughout request lifecycle
            MDC.put(CORRELATION_ID_KEY, correlationId);
            
            // Add to response headers for client tracking
            httpResponse.setHeader(CORRELATION_ID_HEADER, correlationId);
            
            // Log request start with correlation ID
            logger.info("Request started: {} {} | Client: {}", 
                       httpRequest.getMethod(), 
                       httpRequest.getRequestURI(),
                       httpRequest.getRemoteAddr());
            
            // Continue filter chain
            long startTime = System.currentTimeMillis();
            chain.doFilter(request, response);
            long duration = System.currentTimeMillis() - startTime;
            
            // Log request completion
            logger.info("Request completed: {} {} | Status: {} | Duration: {}ms",
                       httpRequest.getMethod(),
                       httpRequest.getRequestURI(),
                       httpResponse.getStatus(),
                       duration);
            
        } catch (Exception e) {
            // Log exception with correlation ID
            logger.error("Request failed with exception", e);
            throw e;
            
        } finally {
            // CRITICAL: Always clean up MDC to prevent memory leaks in thread pools
            MDC.remove(CORRELATION_ID_KEY);
        }
    }
    
    /**
     * Gets correlation ID from request header or generates a new one
     * @param request HTTP request
     * @return correlation ID
     */
    private String getOrGenerateCorrelationId(HttpServletRequest request) {
        // Check if client provided correlation ID (for distributed tracing)
        String correlationId = request.getHeader(CORRELATION_ID_HEADER);
        
        if (correlationId == null || correlationId.trim().isEmpty()) {
            // Generate new correlation ID
            correlationId = UUID.randomUUID().toString();
            logger.debug("Generated new correlation ID: {}", correlationId);
        } else {
            logger.debug("Using existing correlation ID from header: {}", correlationId);
        }
        
        return correlationId;
    }
}
