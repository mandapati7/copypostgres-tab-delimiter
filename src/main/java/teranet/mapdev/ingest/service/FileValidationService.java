package teranet.mapdev.ingest.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import teranet.mapdev.ingest.model.FileValidationIssue;
import teranet.mapdev.ingest.model.FileValidationRule;
import teranet.mapdev.ingest.repository.FileValidationIssueRepository;
import teranet.mapdev.ingest.repository.FileValidationRuleRepository;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service for validating delimited files and tracking issues
 * 
 * This service:
 * - Validates tab counts per row based on configurable rules
 * - Automatically fixes excess tabs by converting to spaces
 * - Tracks all validation issues for reporting
 * - Generates validation reports for senders
 */
@Service
@Slf4j
public class FileValidationService {

    private final FileValidationRuleRepository ruleRepository;
    private final FileValidationIssueRepository issueRepository;

    public FileValidationService(
            FileValidationRuleRepository ruleRepository,
            FileValidationIssueRepository issueRepository) {
        this.ruleRepository = ruleRepository;
        this.issueRepository = issueRepository;
    }

    /**
     * Validate and optionally fix a file based on configured rules
     * 
     * @param inputStream Input file stream
     * @param fileName    Name of the file being validated
     * @param filePattern File pattern (e.g., "PM3", "IM2")
     * @param batchId     Batch ID for tracking
     * @return ValidationResult with fixed content and issues found
     */
    @Transactional
    public ValidationResult validateAndFix(
            InputStream inputStream,
            String fileName,
            String filePattern,
            UUID batchId) throws IOException {

        log.info("Starting validation for file: {} (pattern: {})", fileName, filePattern);

        // Load validation rule
        Optional<FileValidationRule> ruleOpt = ruleRepository.findByFilePattern(filePattern);
        if (ruleOpt.isEmpty() || !ruleOpt.get().getValidationEnabled()) {
            log.info("No validation rule found or validation disabled for pattern: {}", filePattern);
            return ValidationResult.noValidation(inputStream);
        }

        FileValidationRule rule = ruleOpt.get();
        log.info("Applying validation rule: expected {} tabs per row", rule.getExpectedTabCount());

        // Process file line by line
        List<FileValidationIssue> issues = new ArrayList<>();
        ByteArrayOutputStream fixedOutput = new ByteArrayOutputStream();
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fixedOutput, StandardCharsets.UTF_8));

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            long lineNumber = 0;
            boolean hasCriticalIssues = false;

            while ((line = reader.readLine()) != null) {
                lineNumber++;

                // Count tabs in the line
                int tabCount = countTabs(line);

                if (tabCount != rule.getExpectedTabCount()) {
                    // Create validation issue
                    FileValidationIssue issue = createIssue(
                            batchId, fileName, lineNumber, tabCount,
                            rule.getExpectedTabCount(), line);

                    if (tabCount > rule.getExpectedTabCount() && rule.getAutoFixEnabled()) {
                        // Fix excess tabs by converting extra tabs to spaces
                        String fixedLine = fixExcessTabs(line, rule.getExpectedTabCount());
                        issue.setAutoFixed(true);
                        issue.setCorrectedLine(fixedLine);
                        issue.setFixDescription(
                                String.format("Converted %d excess tabs to spaces",
                                        tabCount - rule.getExpectedTabCount()));
                        issue.setSeverity(FileValidationIssue.Severity.WARNING);

                        writer.write(fixedLine);
                        writer.newLine();
                    } else {
                        // Cannot auto-fix or insufficient tabs
                        issue.setSeverity(rule.getRejectOnViolation()
                                ? FileValidationIssue.Severity.CRITICAL
                                : FileValidationIssue.Severity.ERROR);
                        issue.setAutoFixed(false);

                        if (rule.getRejectOnViolation()) {
                            hasCriticalIssues = true;
                        }

                        writer.write(line);
                        writer.newLine();
                    }

                    issues.add(issue);
                } else {
                    // Line is valid
                    writer.write(line);
                    writer.newLine();
                }
            }

            writer.flush();

            // Save all issues to database
            if (!issues.isEmpty()) {
                issueRepository.saveAll(issues);
                log.info("Recorded {} validation issues for batch {}", issues.size(), batchId);
            }

            // Determine result
            if (hasCriticalIssues && rule.getRejectOnViolation()) {
                log.error("File {} has critical validation issues - rejecting", fileName);
                return ValidationResult.rejected(issues);
            }

            log.info("Validation completed: {} issues found, {} auto-fixed",
                    issues.size(),
                    issues.stream().filter(FileValidationIssue::getAutoFixed).count());

            return ValidationResult.success(
                    new ByteArrayInputStream(fixedOutput.toByteArray()),
                    issues);

        }
    }

    /**
     * Count the number of tabs in a line
     */
    private int countTabs(String line) {
        int count = 0;
        for (char c : line.toCharArray()) {
            if (c == '\t') {
                count++;
            }
        }
        return count;
    }

    /**
     * Fix excess tabs by converting extras to spaces
     * Keeps only the expected number of tabs
     */
    private String fixExcessTabs(String line, int expectedTabs) {
        StringBuilder result = new StringBuilder();
        int tabsSoFar = 0;

        for (char c : line.toCharArray()) {
            if (c == '\t') {
                if (tabsSoFar < expectedTabs) {
                    result.append('\t');
                    tabsSoFar++;
                } else {
                    // Convert excess tabs to single space
                    result.append(' ');
                }
            } else {
                result.append(c);
            }
        }

        return result.toString();
    }

    /**
     * Create a validation issue record
     */
    private FileValidationIssue createIssue(
            UUID batchId,
            String fileName,
            long lineNumber,
            int actualTabs,
            int expectedTabs,
            String originalLine) {

        FileValidationIssue issue = new FileValidationIssue();
        issue.setBatchId(batchId);
        issue.setFileName(fileName);
        issue.setLineNumber(lineNumber);
        issue.setIssueType(actualTabs > expectedTabs
                ? FileValidationIssue.IssueType.EXCESS_TABS
                : FileValidationIssue.IssueType.INSUFFICIENT_TABS);
        issue.setExpectedValue(expectedTabs + " tabs");
        issue.setActualValue(actualTabs + " tabs");
        issue.setDescription(String.format(
                "Line %d: Expected %d tabs but found %d tabs",
                lineNumber, expectedTabs, actualTabs));
        issue.setOriginalLine(truncate(originalLine, 500));

        return issue;
    }

    /**
     * Truncate string to max length
     */
    private String truncate(String str, int maxLength) {
        if (str == null)
            return null;
        return str.length() <= maxLength ? str : str.substring(0, maxLength) + "...";
    }

    /**
     * Generate a validation report for a batch
     */
    public ValidationReport generateReport(UUID batchId) {
        List<FileValidationIssue> issues = issueRepository.findByBatchIdOrderByLineNumber(batchId);

        ValidationReport report = new ValidationReport();
        report.setBatchId(batchId);
        report.setTotalIssues(issues.size());
        report.setAutoFixedCount((int) issues.stream().filter(FileValidationIssue::getAutoFixed).count());
        report.setIssues(issues);

        // Count by severity
        Map<FileValidationIssue.Severity, Long> severityCounts = new HashMap<>();
        for (FileValidationIssue issue : issues) {
            severityCounts.merge(issue.getSeverity(), 1L, Long::sum);
        }
        report.setSeverityCounts(severityCounts);

        return report;
    }

    /**
     * Result of validation operation
     */
    public static class ValidationResult {
        private final boolean validated;
        private final boolean rejected;
        private final InputStream fixedInputStream;
        private final List<FileValidationIssue> issues;

        private ValidationResult(boolean validated, boolean rejected,
                InputStream fixedInputStream,
                List<FileValidationIssue> issues) {
            this.validated = validated;
            this.rejected = rejected;
            this.fixedInputStream = fixedInputStream;
            this.issues = issues != null ? issues : Collections.emptyList();
        }

        public static ValidationResult noValidation(InputStream originalStream) {
            return new ValidationResult(false, false, originalStream, null);
        }

        public static ValidationResult success(InputStream fixedStream, List<FileValidationIssue> issues) {
            return new ValidationResult(true, false, fixedStream, issues);
        }

        public static ValidationResult rejected(List<FileValidationIssue> issues) {
            return new ValidationResult(true, true, null, issues);
        }

        public boolean isValidated() {
            return validated;
        }

        public boolean isRejected() {
            return rejected;
        }

        public boolean hasIssues() {
            return !issues.isEmpty();
        }

        public InputStream getFixedInputStream() {
            return fixedInputStream;
        }

        public List<FileValidationIssue> getIssues() {
            return issues;
        }
    }

    /**
     * Validation report DTO
     */
    public static class ValidationReport {
        private UUID batchId;
        private int totalIssues;
        private int autoFixedCount;
        private Map<FileValidationIssue.Severity, Long> severityCounts;
        private List<FileValidationIssue> issues;

        // Getters and setters
        public UUID getBatchId() {
            return batchId;
        }

        public void setBatchId(UUID batchId) {
            this.batchId = batchId;
        }

        public int getTotalIssues() {
            return totalIssues;
        }

        public void setTotalIssues(int totalIssues) {
            this.totalIssues = totalIssues;
        }

        public int getAutoFixedCount() {
            return autoFixedCount;
        }

        public void setAutoFixedCount(int autoFixedCount) {
            this.autoFixedCount = autoFixedCount;
        }

        public Map<FileValidationIssue.Severity, Long> getSeverityCounts() {
            return severityCounts;
        }

        public void setSeverityCounts(Map<FileValidationIssue.Severity, Long> severityCounts) {
            this.severityCounts = severityCounts;
        }

        public List<FileValidationIssue> getIssues() {
            return issues;
        }

        public void setIssues(List<FileValidationIssue> issues) {
            this.issues = issues;
        }
    }
}
