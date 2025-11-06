package teranet.mapdev.ingest.util;

import org.slf4j.MDC;

/**
 * Utility class for managing correlation IDs throughout the application.
 * Uses SLF4J MDC (Mapped Diagnostic Context) for thread-local storage.
 * 
 * Usage:
 * - Automatically managed by CorrelationIdFilter for HTTP requests
 * - Can be used manually for background jobs or async operations
 */
public class CorrelationIdUtil {
    
    private static final String CORRELATION_ID_KEY = "correlationId";
    
    /**
     * Gets the current correlation ID from MDC
     * @return correlation ID or "NO-CORRELATION-ID" if not set
     */
    public static String getCurrentCorrelationId() {
        String correlationId = MDC.get(CORRELATION_ID_KEY);
        return correlationId != null ? correlationId : "NO-CORRELATION-ID";
    }
    
    /**
     * Sets a correlation ID in MDC (for background jobs)
     * @param correlationId correlation ID to set
     */
    public static void setCorrelationId(String correlationId) {
        MDC.put(CORRELATION_ID_KEY, correlationId);
    }
    
    /**
     * Removes correlation ID from MDC
     * IMPORTANT: Always call this in finally blocks to prevent memory leaks
     */
    public static void clearCorrelationId() {
        MDC.remove(CORRELATION_ID_KEY);
    }
    
    /**
     * Checks if correlation ID is set
     * @return true if correlation ID exists in MDC
     */
    public static boolean hasCorrelationId() {
        return MDC.get(CORRELATION_ID_KEY) != null;
    }
}
