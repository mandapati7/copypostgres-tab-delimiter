package teranet.mapdev.ingest.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import teranet.mapdev.ingest.model.FileValidationIssue;
import teranet.mapdev.ingest.model.FileValidationRule;
import teranet.mapdev.ingest.repository.FileValidationIssueRepository;
import teranet.mapdev.ingest.repository.FileValidationRuleRepository;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FileValidationService data cleaning features
 * Tests the PTDDataCleaner-style validations:
 * - Control character replacement
 * - Non-BASIC_LATIN character replacement
 * - Consecutive asterisk collapsing
 */
@ExtendWith(MockitoExtension.class)
class FileValidationServiceDataCleaningTest {

    @Mock
    private FileValidationRuleRepository ruleRepository;

    @Mock
    private FileValidationIssueRepository issueRepository;

    @InjectMocks
    private FileValidationService validationService;

    @Captor
    private ArgumentCaptor<List<FileValidationIssue>> issuesCaptor;

    private UUID testBatchId;
    private FileValidationRule testRule;

    @BeforeEach
    void setUp() {
        testBatchId = UUID.randomUUID();

        // Create a test rule with data cleaning enabled
        testRule = new FileValidationRule();
        testRule.setId(1L);
        testRule.setFilePattern("pm3");
        testRule.setTableName("staging_pm3");
        testRule.setExpectedTabCount(5);
        testRule.setValidationEnabled(true);
        testRule.setAutoFixEnabled(true);
        testRule.setRejectOnViolation(false);
        testRule.setReplaceControlChars(true);
        testRule.setReplaceNonLatinChars(true);
        testRule.setCollapseConsecutiveReplaced(true);
    }

    @Test
    void testValidateAndFix_ReplacesControlCharacters() throws IOException {
        // Given: line with control characters
        String inputData = "field1\u0001\u0002field2\tfield3\u0003\tfield4\tfield5\tfield6\n";
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));

        when(ruleRepository.findByFilePattern("pm3")).thenReturn(Optional.of(testRule));
        when(issueRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        // When: validate and fix
        FileValidationService.ValidationResult result = validationService.validateAndFix(
                inputStream, "test.pm3", "pm3", testBatchId);

        // Then: control characters should be replaced
        assertThat(result.isValidated()).isTrue();
        assertThat(result.isRejected()).isFalse();
        assertThat(result.hasIssues()).isTrue();

        // Verify issues were saved
        verify(issueRepository).saveAll(issuesCaptor.capture());
        List<FileValidationIssue> issues = issuesCaptor.getValue();

        // Should have control character issue
        assertThat(issues).anySatisfy(issue -> {
            assertThat(issue.getIssueType()).isEqualTo(FileValidationIssue.IssueType.CONTROL_CHARACTERS);
            assertThat(issue.getSeverity()).isEqualTo(FileValidationIssue.Severity.WARNING);
            assertThat(issue.getAutoFixed()).isTrue();
        });

        // Read the cleaned output
        String cleanedOutput = new String(result.getFixedInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(cleanedOutput).doesNotContain("\u0001", "\u0002", "\u0003");
        assertThat(cleanedOutput).contains("*"); // Control chars replaced with asterisk
    }

    @Test
    void testValidateAndFix_ReplacesNonBasicLatinCharacters() throws IOException {
        // Given: line with non-BASIC_LATIN characters (emoji, accented chars, etc.)
        String inputData = "café\t世界\tñoño\t☕\ttest\tfield6\n";
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));

        when(ruleRepository.findByFilePattern("pm3")).thenReturn(Optional.of(testRule));
        when(issueRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        // When: validate and fix
        FileValidationService.ValidationResult result = validationService.validateAndFix(
                inputStream, "test.pm3", "pm3", testBatchId);

        // Then: non-BASIC_LATIN characters should be replaced
        assertThat(result.isValidated()).isTrue();
        assertThat(result.hasIssues()).isTrue();

        // Verify issues were saved
        verify(issueRepository).saveAll(issuesCaptor.capture());
        List<FileValidationIssue> issues = issuesCaptor.getValue();

        // Should have non-BASIC_LATIN character issue
        assertThat(issues).anySatisfy(issue -> {
            assertThat(issue.getIssueType()).isEqualTo(FileValidationIssue.IssueType.NON_LATIN_CHARACTERS);
            assertThat(issue.getSeverity()).isEqualTo(FileValidationIssue.Severity.WARNING);
            assertThat(issue.getAutoFixed()).isTrue();
        });

        // Read the cleaned output
        String cleanedOutput = new String(result.getFixedInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(cleanedOutput).contains("caf*"); // é replaced
        assertThat(cleanedOutput).contains("*"); // Non-latin chars replaced
        assertThat(cleanedOutput).contains("test"); // ASCII text preserved
    }

    @Test
    void testValidateAndFix_CollapsesConsecutiveReplacedCharacters() throws IOException {
        // Given: line with multiple consecutive control/non-latin characters
        String inputData = "field1\u0001\u0002\u0003\tfield2世界日本\tfield3\tfield4\tfield5\tfield6\n";
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));

        when(ruleRepository.findByFilePattern("pm3")).thenReturn(Optional.of(testRule));
        when(issueRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        // When: validate and fix
        FileValidationService.ValidationResult result = validationService.validateAndFix(
                inputStream, "test.pm3", "pm3", testBatchId);

        // Then: consecutive asterisks should be collapsed
        assertThat(result.isValidated()).isTrue();
        assertThat(result.hasIssues()).isTrue();

        // Verify issues were saved
        verify(issueRepository).saveAll(issuesCaptor.capture());
        List<FileValidationIssue> issues = issuesCaptor.getValue();

        // Should have consecutive collapsed issue
        assertThat(issues).anySatisfy(issue -> {
            assertThat(issue.getIssueType()).isEqualTo(FileValidationIssue.IssueType.CONSECUTIVE_REPLACED_CHARS);
            assertThat(issue.getSeverity()).isEqualTo(FileValidationIssue.Severity.WARNING);
            assertThat(issue.getAutoFixed()).isTrue();
        });

        // Read the cleaned output
        String cleanedOutput = new String(result.getFixedInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(cleanedOutput).doesNotContain("**"); // No consecutive asterisks
        assertThat(cleanedOutput).contains("*"); // But has single asterisks
    }

    @Test
    void testValidateAndFix_DataCleaningDisabled_NoCleaningIssues() throws IOException {
        // Given: rule with data cleaning disabled
        testRule.setReplaceControlChars(false);
        testRule.setReplaceNonLatinChars(false);
        testRule.setCollapseConsecutiveReplaced(false);

        String inputData = "café\u0001世界\tfield2\tfield3\tfield4\tfield5\tfield6\n";
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));

        when(ruleRepository.findByFilePattern("pm3")).thenReturn(Optional.of(testRule));

        // When: validate and fix
        FileValidationService.ValidationResult result = validationService.validateAndFix(
                inputStream, "test.pm3", "pm3", testBatchId);

        // Then: no data cleaning issues should be created
        if (result.hasIssues()) {
            when(issueRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));
            verify(issueRepository).saveAll(issuesCaptor.capture());
            List<FileValidationIssue> issues = issuesCaptor.getValue();

            // Should NOT have data cleaning issues
            assertThat(issues)
                    .noneMatch(issue -> issue.getIssueType() == FileValidationIssue.IssueType.CONTROL_CHARACTERS ||
                            issue.getIssueType() == FileValidationIssue.IssueType.NON_LATIN_CHARACTERS ||
                            issue.getIssueType() == FileValidationIssue.IssueType.CONSECUTIVE_REPLACED_CHARS);
        }

        // Read the output - should be unchanged
        String output = new String(result.getFixedInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(output).contains("café\u0001世界"); // Original characters preserved
    }

    @Test
    void testValidateAndFix_CombinesDataCleaningAndTabValidation() throws IOException {
        // Given: line with both data issues and wrong tab count
        String inputData = "field1\u0001\tfield2\t\tfield3世界\tfield4\t\tfield5\t\tfield6\n"; // 8 tabs instead of 5
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));

        when(ruleRepository.findByFilePattern("pm3")).thenReturn(Optional.of(testRule));
        when(issueRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        // When: validate and fix
        FileValidationService.ValidationResult result = validationService.validateAndFix(
                inputStream, "test.pm3", "pm3", testBatchId);

        // Then: should have both types of issues
        assertThat(result.isValidated()).isTrue();
        assertThat(result.hasIssues()).isTrue();

        // Verify issues were saved
        verify(issueRepository).saveAll(issuesCaptor.capture());
        List<FileValidationIssue> issues = issuesCaptor.getValue();

        // Should have data cleaning issues
        assertThat(issues).anySatisfy(issue -> assertThat(issue.getIssueType()).isIn(
                FileValidationIssue.IssueType.CONTROL_CHARACTERS,
                FileValidationIssue.IssueType.NON_LATIN_CHARACTERS));

        // Should have tab count issue
        assertThat(issues).anySatisfy(
                issue -> assertThat(issue.getIssueType()).isEqualTo(FileValidationIssue.IssueType.EXCESS_TABS));
    }

    @Test
    void testValidateAndFix_PreservesTabsAndWhitespace() throws IOException {
        // Given: line with tabs and spaces that should be preserved
        String inputData = "field1 with spaces\tfield2  \tfield3\tfield4\tfield5\tfield6\n";
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));

        when(ruleRepository.findByFilePattern("pm3")).thenReturn(Optional.of(testRule));

        // When: validate and fix
        FileValidationService.ValidationResult result = validationService.validateAndFix(
                inputStream, "test.pm3", "pm3", testBatchId);

        // Then: tabs and spaces should be preserved
        String output = new String(result.getFixedInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(output).contains("field1 with spaces\t"); // Spaces preserved
        assertThat(output).contains("field2  \t"); // Multiple spaces preserved

        // Count tabs - should still have expected count
        long tabCount = output.chars().filter(ch -> ch == '\t').count();
        assertThat(tabCount).isEqualTo((long) testRule.getExpectedTabCount());
    }

    @Test
    void testValidateAndFix_HandlesEmptyLine() throws IOException {
        // Given: empty line
        String inputData = "\n";
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));

        when(ruleRepository.findByFilePattern("pm3")).thenReturn(Optional.of(testRule));
        when(issueRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        // When: validate and fix
        FileValidationService.ValidationResult result = validationService.validateAndFix(
                inputStream, "test.pm3", "pm3", testBatchId);

        // Then: should handle gracefully (will have tab count issue)
        assertThat(result.isValidated()).isTrue();

        // Verify issues were saved for tab count
        verify(issueRepository).saveAll(issuesCaptor.capture());
        List<FileValidationIssue> issues = issuesCaptor.getValue();
        assertThat(issues).anySatisfy(
                issue -> assertThat(issue.getIssueType()).isEqualTo(FileValidationIssue.IssueType.INSUFFICIENT_TABS));
    }

    @Test
    void testValidateAndFix_MultipleLines_TracksLineNumbers() throws IOException {
        // Given: multiple lines with various issues
        String inputData = "good\tline\there\twith\tfive\ttabs\n" +
                "bad\u0001line\twith\tcontrol\tchars\ttabs\n" +
                "another世界\tline\twith\tnon\tlatin\ttabs\n";
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));

        when(ruleRepository.findByFilePattern("pm3")).thenReturn(Optional.of(testRule));
        when(issueRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        // When: validate and fix
        FileValidationService.ValidationResult result = validationService.validateAndFix(
                inputStream, "test.pm3", "pm3", testBatchId);

        // Then: should track correct line numbers
        verify(issueRepository).saveAll(issuesCaptor.capture());
        List<FileValidationIssue> issues = issuesCaptor.getValue();

        // Line 1: should have no issues
        assertThat(issues).noneMatch(issue -> issue.getLineNumber() == 1);

        // Line 2: should have control character issue
        assertThat(issues).anySatisfy(issue -> {
            assertThat(issue.getLineNumber()).isEqualTo(2);
            assertThat(issue.getIssueType()).isEqualTo(FileValidationIssue.IssueType.CONTROL_CHARACTERS);
        });

        // Line 3: should have non-latin character issue
        assertThat(issues).anySatisfy(issue -> {
            assertThat(issue.getLineNumber()).isEqualTo(3);
            assertThat(issue.getIssueType()).isEqualTo(FileValidationIssue.IssueType.NON_LATIN_CHARACTERS);
        });
    }

    @Test
    void testValidateAndFix_OnlyControlCharsEnabled() throws IOException {
        // Given: only control character replacement enabled
        testRule.setReplaceControlChars(true);
        testRule.setReplaceNonLatinChars(false);
        testRule.setCollapseConsecutiveReplaced(false);

        String inputData = "field1\u0001\tcafé\t世界\tfield4\tfield5\tfield6\n";
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));

        when(ruleRepository.findByFilePattern("pm3")).thenReturn(Optional.of(testRule));
        when(issueRepository.saveAll(any())).thenAnswer(invocation -> invocation.getArgument(0));

        // When: validate and fix
        FileValidationService.ValidationResult result = validationService.validateAndFix(
                inputStream, "test.pm3", "pm3", testBatchId);

        // Then: only control chars should be replaced
        verify(issueRepository).saveAll(issuesCaptor.capture());
        List<FileValidationIssue> issues = issuesCaptor.getValue();

        // Should have control character issue
        assertThat(issues).anySatisfy(
                issue -> assertThat(issue.getIssueType()).isEqualTo(FileValidationIssue.IssueType.CONTROL_CHARACTERS));

        // Should NOT have non-latin character issue
        assertThat(issues)
                .noneMatch(issue -> issue.getIssueType() == FileValidationIssue.IssueType.NON_LATIN_CHARACTERS);

        // Read output - non-latin chars should be preserved
        String output = new String(result.getFixedInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(output).contains("café"); // Preserved
        assertThat(output).contains("世界"); // Preserved
        assertThat(output).doesNotContain("\u0001"); // Control char removed
    }
}
