package teranet.mapdev.ingest.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import teranet.mapdev.ingest.model.FileValidationRule;

import java.util.Optional;

@Repository
public interface FileValidationRuleRepository extends JpaRepository<FileValidationRule, Long> {

    /**
     * Find validation rule by file pattern (e.g., "PM3", "IM2")
     */
    Optional<FileValidationRule> findByFilePattern(String filePattern);

    /**
     * Find validation rule by table name
     */
    Optional<FileValidationRule> findByTableName(String tableName);
}
