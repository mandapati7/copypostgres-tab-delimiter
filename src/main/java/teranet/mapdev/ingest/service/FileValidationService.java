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
 * - Cleans data by replacing control characters with asterisk (*)
 * - Replaces non-BASIC_LATIN characters with asterisk (*)
 * - Collapses consecutive replaced characters to single asterisk
 * - Tracks all validation issues for reporting
 * - Generates validation reports for senders
 */
@Service
@Slf4j
public class FileValidationService {

    private final FileValidationRuleRepository ruleRepository;
    private final FileValidationIssueRepository issueRepository;

    // Regex patterns for efficient character replacement
    // Control characters: 0x00-0x1F (except \t, \n, \r) and 0x7F (DEL)
    private static final Pattern CONTROL_CHAR_PATTERN = Pattern.compile(
            "[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F\\x7F]");

    // Non-BASIC_LATIN: any character with codepoint > 0x7F
    // Negative lookahead to preserve tabs and standard whitespace
    private static final Pattern NON_BASIC_LATIN_PATTERN = Pattern.compile(
            "[^\\x00-\\x7F]");

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

                // Store original line for reporting
                String originalLine = line;
                String processedLine = line;
                List<FileValidationIssue> lineIssues = new ArrayList<>();

                // Step 1: Apply data cleaning rules first
                if (rule.getReplaceControlChars() || rule.getReplaceNonLatinChars()
                        || rule.getCollapseConsecutiveReplaced()) {
                    DataCleaningResult cleaningResult = cleanLineData(
                            processedLine,
                            rule.getReplaceControlChars(),
                            rule.getReplaceNonLatinChars(),
                            rule.getCollapseConsecutiveReplaced());

                    processedLine = cleaningResult.cleanedLine;

                    // Record data cleaning issues
                    if (cleaningResult.controlCharsReplaced > 0) {
                        FileValidationIssue issue = createDataCleaningIssue(
                                batchId, fileName, lineNumber, originalLine, processedLine,
                                FileValidationIssue.IssueType.CONTROL_CHARACTERS,
                                cleaningResult.controlCharsReplaced,
                                "control character(s)");
                        lineIssues.add(issue);
                    }

                    if (cleaningResult.nonLatinCharsReplaced > 0) {
                        FileValidationIssue issue = createDataCleaningIssue(
                                batchId, fileName, lineNumber, originalLine, processedLine,
                                FileValidationIssue.IssueType.NON_LATIN_CHARACTERS,
                                cleaningResult.nonLatinCharsReplaced,
                                "non-BASIC_LATIN character(s)");
                        lineIssues.add(issue);
                    }

                    if (cleaningResult.consecutiveCollapsed > 0) {
                        FileValidationIssue issue = createDataCleaningIssue(
                                batchId, fileName, lineNumber, originalLine, processedLine,
                                FileValidationIssue.IssueType.CONSECUTIVE_REPLACED_CHARS,
                                cleaningResult.consecutiveCollapsed,
                                "consecutive replaced character(s) collapsed");
                        lineIssues.add(issue);
                    }
                }

                // Step 2: Count tabs in the (possibly cleaned) line
                int tabCount = countTabs(processedLine);

                if (tabCount != rule.getExpectedTabCount()) {
                    // Create tab validation issue
                    FileValidationIssue issue = createIssue(
                            batchId, fileName, lineNumber, tabCount,
                            rule.getExpectedTabCount(), processedLine);

                    if (tabCount > rule.getExpectedTabCount() && rule.getAutoFixEnabled()) {
                        // Fix excess tabs by converting extra tabs to spaces
                        processedLine = fixExcessTabs(processedLine, rule.getExpectedTabCount());
                        issue.setAutoFixed(true);
                        issue.setCorrectedLine(processedLine);
                        issue.setFixDescription(
                                String.format("Converted %d excess tabs to spaces",
                                        tabCount - rule.getExpectedTabCount()));
                        issue.setSeverity(FileValidationIssue.Severity.WARNING);

                        writer.write(processedLine);
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

                        writer.write(processedLine);
                        writer.newLine();
                    }

                    lineIssues.add(issue);
                } else {
                    // Line tab count is valid, write the processed line
                    writer.write(processedLine);
                    writer.newLine();
                }

                // Add all issues for this line
                issues.addAll(lineIssues);
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
        // Sanitize to remove NULL bytes before storing in PostgreSQL
        issue.setOriginalLine(truncate(sanitizeForPostgres(originalLine), 500));

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
     * Clean line data according to PTDDataCleaner rules:
     * 1. Replace control characters with asterisk (*)
     * 2. Replace non-BASIC_LATIN characters with asterisk (*)
     * 3. Collapse consecutive asterisks to single asterisk
     * 
     * @param line                 The line to clean
     * @param replaceControlChars  Whether to replace control characters
     * @param replaceNonLatinChars Whether to replace non-BASIC_LATIN characters
     * @param collapseConsecutive  Whether to collapse consecutive asterisks
     * @return DataCleaningResult containing cleaned line and statistics
     */
    private DataCleaningResult cleanLineData(
            String line,
            boolean replaceControlChars,
            boolean replaceNonLatinChars,
            boolean collapseConsecutive) {

        DataCleaningResult result = new DataCleaningResult();
        result.cleanedLine = line;

        if (line == null || line.isEmpty()) {
            return result;
        }

        String cleanedLine = line;

        // Step 1: Replace control characters with regex (more efficient)
        if (replaceControlChars) {
            // Pattern for control characters (0x00-0x1F except tab/newline/CR, and 0x7F)
            // Preserve \t (tab), \n (newline), \r (carriage return)
            String beforeReplace = cleanedLine;
            cleanedLine = CONTROL_CHAR_PATTERN.matcher(cleanedLine).replaceAll("*");

            // Count replacements by comparing string lengths and asterisk counts
            result.controlCharsReplaced = countReplacements(beforeReplace, cleanedLine);
        }

        // Step 2: Replace non-BASIC_LATIN characters with regex
        if (replaceNonLatinChars) {
            String beforeReplace = cleanedLine;
            // Replace any character with codepoint > 0x7F (outside BASIC_LATIN)
            // Preserve tabs and whitespace
            cleanedLine = NON_BASIC_LATIN_PATTERN.matcher(cleanedLine).replaceAll("*");

            result.nonLatinCharsReplaced = countReplacements(beforeReplace, cleanedLine);
        }

        result.cleanedLine = cleanedLine;
        result.controlCharsReplaced = result.controlCharsReplaced;
        result.nonLatinCharsReplaced = result.nonLatinCharsReplaced;

        // Step 3: Collapse consecutive asterisks if enabled
        if (collapseConsecutive && result.cleanedLine.contains("**")) {
            String beforeCollapse = result.cleanedLine;
            result.cleanedLine = result.cleanedLine.replaceAll("\\*{2,}", "*");
            // Count how many asterisks were removed
            int beforeCount = beforeCollapse.length() - beforeCollapse.replace("*", "").length();
            int afterCount = result.cleanedLine.length() - result.cleanedLine.replace("*", "").length();
            result.consecutiveCollapsed = beforeCount - afterCount;
        }

        return result;
    }

    /**
     * Count how many characters were replaced by comparing before and after strings
     * This counts the difference in length plus the number of asterisks added
     */
    private int countReplacements(String before, String after) {
        if (before.equals(after)) {
            return 0;
        }

        // Count asterisks in after that weren't in before
        int beforeAsterisks = countChar(before, '*');
        int afterAsterisks = countChar(after, '*');
        int newAsterisks = afterAsterisks - beforeAsterisks;

        // Calculate replacements: characters removed + new asterisks added
        int lengthDiff = before.length() - after.length();
        return lengthDiff + newAsterisks;
    }

    /**
     * Count occurrences of a character in a string
     */
    private int countChar(String str, char c) {
        int count = 0;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == c) {
                count++;
            }
        }
        return count;
    }

    /**
     * Sanitize string for PostgreSQL storage
     * Removes NULL bytes (\x00) which PostgreSQL cannot store in text fields
     */
    private String sanitizeForPostgres(String input) {
        if (input == null) {
            return null;
        }
        // Remove NULL bytes - PostgreSQL cannot store \x00 in text fields
        return input.replace("\u0000", "*");
    }

    /**
     * Create a data cleaning issue record
     */
    private FileValidationIssue createDataCleaningIssue(
            UUID batchId,
            String fileName,
            long lineNumber,
            String originalLine,
            String correctedLine,
            FileValidationIssue.IssueType issueType,
            int replacementCount,
            String replacementDescription) {

        FileValidationIssue issue = new FileValidationIssue();
        issue.setBatchId(batchId);
        issue.setFileName(fileName);
        issue.setLineNumber(lineNumber);
        issue.setIssueType(issueType);
        issue.setSeverity(FileValidationIssue.Severity.WARNING);
        issue.setAutoFixed(true);
        // Sanitize strings to remove NULL bytes before storing in PostgreSQL
        issue.setOriginalLine(truncate(sanitizeForPostgres(originalLine), 500));
        issue.setCorrectedLine(truncate(sanitizeForPostgres(correctedLine), 500));
        issue.setFixDescription(String.format("Replaced %d %s with asterisk (*)",
                replacementCount, replacementDescription));
        issue.setDescription(String.format(
                "Line %d: Found %d %s, replaced with asterisk",
                lineNumber, replacementCount, replacementDescription));

        return issue;
    }

    /**
     * Data cleaning result holder
     */
    private static class DataCleaningResult {
        String cleanedLine;
        int controlCharsReplaced = 0;
        int nonLatinCharsReplaced = 0;
        int consecutiveCollapsed = 0;
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
