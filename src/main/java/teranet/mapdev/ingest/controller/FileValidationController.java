package teranet.mapdev.ingest.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import teranet.mapdev.ingest.model.FileValidationIssue;
import teranet.mapdev.ingest.model.FileValidationRule;
import teranet.mapdev.ingest.repository.FileValidationIssueRepository;
import teranet.mapdev.ingest.repository.FileValidationRuleRepository;
import teranet.mapdev.ingest.service.FileValidationService;

import java.util.List;
import java.util.UUID;

/**
 * REST API for managing file validation rules and viewing validation reports
 */
@RestController
@RequestMapping("/api/validation")
@Slf4j
public class FileValidationController {

    private final FileValidationRuleRepository ruleRepository;
    private final FileValidationIssueRepository issueRepository;
    private final FileValidationService validationService;

    public FileValidationController(
            FileValidationRuleRepository ruleRepository,
            FileValidationIssueRepository issueRepository,
            FileValidationService validationService) {
        this.ruleRepository = ruleRepository;
        this.issueRepository = issueRepository;
        this.validationService = validationService;
    }

    // ===== Validation Rules Management =====

    /**
     * Get all validation rules
     */
    @GetMapping("/rules")
    public ResponseEntity<List<FileValidationRule>> getAllRules() {
        return ResponseEntity.ok(ruleRepository.findAll());
    }

    /**
     * Get validation rule by file pattern
     */
    @GetMapping("/rules/{filePattern}")
    public ResponseEntity<FileValidationRule> getRuleByPattern(@PathVariable String filePattern) {
        return ruleRepository.findByFilePattern(filePattern)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Create or update validation rule
     */
    @PostMapping("/rules")
    public ResponseEntity<FileValidationRule> createOrUpdateRule(@RequestBody FileValidationRule rule) {
        log.info("Creating/updating validation rule for pattern: {}", rule.getFilePattern());
        FileValidationRule saved = ruleRepository.save(rule);
        return ResponseEntity.ok(saved);
    }

    /**
     * Delete validation rule
     */
    @DeleteMapping("/rules/{id}")
    public ResponseEntity<Void> deleteRule(@PathVariable Long id) {
        log.info("Deleting validation rule with ID: {}", id);
        ruleRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    /**
     * Enable/disable validation for a file pattern
     */
    @PutMapping("/rules/{filePattern}/enabled")
    public ResponseEntity<FileValidationRule> toggleValidation(
            @PathVariable String filePattern,
            @RequestParam boolean enabled) {

        return ruleRepository.findByFilePattern(filePattern)
                .map(rule -> {
                    rule.setValidationEnabled(enabled);
                    FileValidationRule saved = ruleRepository.save(rule);
                    log.info("Validation {} for pattern: {}", enabled ? "enabled" : "disabled", filePattern);
                    return ResponseEntity.ok(saved);
                })
                .orElse(ResponseEntity.notFound().build());
    }

    // ===== Validation Issues & Reports =====

    /**
     * Get all validation issues for a batch
     */
    @GetMapping("/issues/batch/{batchId}")
    public ResponseEntity<List<FileValidationIssue>> getIssuesByBatch(@PathVariable UUID batchId) {
        List<FileValidationIssue> issues = issueRepository.findByBatchIdOrderByLineNumber(batchId);
        return ResponseEntity.ok(issues);
    }

    /**
     * Get validation issues by batch and severity
     */
    @GetMapping("/issues/batch/{batchId}/severity/{severity}")
    public ResponseEntity<List<FileValidationIssue>> getIssuesBySeverity(
            @PathVariable UUID batchId,
            @PathVariable FileValidationIssue.Severity severity) {

        List<FileValidationIssue> issues = issueRepository.findByBatchIdAndSeverity(batchId, severity);
        return ResponseEntity.ok(issues);
    }

    /**
     * Get validation report for a batch
     */
    @GetMapping("/report/batch/{batchId}")
    public ResponseEntity<FileValidationService.ValidationReport> getValidationReport(
            @PathVariable UUID batchId) {

        FileValidationService.ValidationReport report = validationService.generateReport(batchId);
        return ResponseEntity.ok(report);
    }

    /**
     * Get count of issues by severity for a batch
     */
    @GetMapping("/issues/batch/{batchId}/summary")
    public ResponseEntity<List<Object[]>> getIssueSummary(@PathVariable UUID batchId) {
        List<Object[]> summary = issueRepository.countIssuesBySeverity(batchId);
        return ResponseEntity.ok(summary);
    }

    /**
     * Get all critical issues across all batches
     */
    @GetMapping("/issues/critical")
    public ResponseEntity<List<FileValidationIssue>> getCriticalIssues() {
        List<FileValidationIssue> issues = issueRepository.findBySeverity(FileValidationIssue.Severity.CRITICAL);
        return ResponseEntity.ok(issues);
    }
}
