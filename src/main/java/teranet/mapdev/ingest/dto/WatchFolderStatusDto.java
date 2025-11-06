package teranet.mapdev.ingest.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * DTO for watch folder status information
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WatchFolderStatusDto {
    
    @JsonProperty("upload_count")
    private Integer uploadCount;
    
    @JsonProperty("wip_count")
    private Integer wipCount;
    
    @JsonProperty("error_count")
    private Integer errorCount;
    
    @JsonProperty("archive_count")
    private Integer archiveCount;
    
    @JsonProperty("watch_enabled")
    private Boolean watchEnabled;
    
    @JsonProperty("watch_running")
    private Boolean watchRunning;
    
    @JsonProperty("last_check")
    private LocalDateTime lastCheck;
    
    @JsonProperty("total_processed_today")
    private Integer totalProcessedToday;
}
