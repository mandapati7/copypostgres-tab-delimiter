package teranet.mapdev.ingest.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import java.util.concurrent.Executor;

/**
 * Application configuration for async processing and file uploads
 */
@Configuration
@EnableAsync
public class AppConfig {
    
    /**
     * Thread pool executor for async CSV processing
     */
    @Bean(name = "csvProcessingExecutor")
    public Executor csvProcessingExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        
        // Core pool size - number of threads to keep alive
        executor.setCorePoolSize(2);
        
        // Maximum pool size - maximum number of threads
        executor.setMaxPoolSize(5);
        
        // Queue capacity - number of tasks to queue when all threads are busy
        executor.setQueueCapacity(100);
        
        // Thread name prefix
        executor.setThreadNamePrefix("CSV-Processing-");
        
        // Rejection policy - what to do when queue is full
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        
        // Wait for tasks to complete on shutdown
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        
        executor.initialize();
        
        return executor;
    }
    
    // Note: Multipart configuration is handled automatically by Spring Boot
    // You can configure it in application.properties:
    // spring.servlet.multipart.max-file-size=500MB
    // spring.servlet.multipart.max-request-size=500MB
}