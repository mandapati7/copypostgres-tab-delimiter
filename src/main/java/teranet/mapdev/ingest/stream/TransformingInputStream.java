package teranet.mapdev.ingest.stream;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import teranet.mapdev.ingest.transformer.DataTransformer;
import teranet.mapdev.ingest.model.FileValidationIssue;
import teranet.mapdev.ingest.repository.FileValidationIssueRepository;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * FilterInputStream that applies DataTransformer to each line.
 * 
 * This stream:
 * - Reads lines from the input stream
 * - Applies transformation via DataTransformer
 * - Writes transformed lines to an output stream
 * - Pipes the output back as readable input
 * 
 * The transformation happens in a background thread to avoid blocking.
 */
@Slf4j
public class TransformingInputStream extends FilterInputStream {
    
    private final PipedInputStream pipedInput;
    private final Thread transformThread;
    private volatile IOException transformException;
    private final List<TransformationRecord> transformations = new CopyOnWriteArrayList<>();
    private final String filePattern;
    private final UUID batchId;
    private final FileValidationIssueRepository issueRepository;
    
    /**
     * Create a transforming input stream.
     * 
     * @param in The original input stream
     * @param transformer The transformer to apply
     * @param filePattern The file pattern being processed
     * @param batchId The batch ID for tracking
     * @param issueRepository Repository to save transformation issues
     * @throws IOException If piping cannot be established
     */
    public TransformingInputStream(InputStream in, DataTransformer transformer, String filePattern,
                        UUID batchId, FileValidationIssueRepository issueRepository) throws IOException {
        super(in);
        
        this.filePattern = filePattern;
        this.batchId = batchId;
        this.issueRepository = issueRepository;
        
        PipedOutputStream pipedOutput = new PipedOutputStream();
        this.pipedInput = new PipedInputStream(pipedOutput, 65536); // 64KB buffer
        
        // Start transformation in background thread
        this.transformThread = new Thread(() -> transformData(in, pipedOutput, transformer));
        this.transformThread.setName("DataTransformer-" + transformer.getClass().getSimpleName());
        this.transformThread.setDaemon(true);
        this.transformThread.start();
    }
    
    /**
     * Transform data in background thread.
     */
    private void transformData(InputStream input, PipedOutputStream output, DataTransformer transformer) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8))) {
            
            String line;
            long lineNumber = 0;
            
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                
                try {
                    // Transform the line
                    String transformedLine = transformer.transformLine(line, lineNumber);
                    
                    // Skip line if transformer returns null
                    if (transformedLine == null) {
                        log.debug("Line {} skipped by transformer", lineNumber);
                        continue;
                    }
                    
                    // Track transformation if line was actually changed
                    if (!line.equals(transformedLine)) {
                        transformations.add(new TransformationRecord(lineNumber, line, transformedLine));
                    }
                    
                    // Write transformed line
                    writer.write(transformedLine);
                    writer.newLine();
                    
                } catch (Exception e) {
                    log.error("Error transforming line {}: {}", lineNumber, e.getMessage(), e);
                    // Write original line on error
                    writer.write(line);
                    writer.newLine();
                }
            }
            
            log.debug("Transformation completed. Processed {} lines, {} transformations applied", 
                     lineNumber, transformations.size());
            
            // Save transformations to database after processing completes
            if (!transformations.isEmpty() && issueRepository != null) {
                saveTransformationIssues();
            }
            
        } catch (IOException e) {
            log.error("Error during data transformation", e);
            this.transformException = e;
        }
    }
    
    /**
     * Save transformation issues to database
     */
    private void saveTransformationIssues() {
        try {
            // Use parallel stream with map to leverage multiple CPUs
            List<FileValidationIssue> issues = transformations.parallelStream()
                .map(record -> {
                    FileValidationIssue issue = new FileValidationIssue();
                    issue.setBatchId(batchId);
                    issue.setFileName(filePattern);
                    issue.setLineNumber(record.lineNumber);
                    issue.setIssueType(FileValidationIssue.IssueType.DATA_TRANSFORMATION);
                    issue.setSeverity(FileValidationIssue.Severity.WARNING);
                    issue.setAutoFixed(true);
                    issue.setOriginalLine(record.originalLine);
                    issue.setCorrectedLine(record.transformedLine);
                    issue.setFixDescription("Applied data transformation (empty PIN fix)");
                    issue.setDescription("Empty PIN value '0000' inserted");
                    return issue;
                })
                .toList();
            
            issueRepository.saveAll(issues);
            log.info("{} transformation issues saved for batch {}", issues.size(), batchId);
            
        } catch (Exception e) {
            log.error("Error saving transformation issues for batch {}", batchId, e);
            // Don't throw - transformation already happened, just logging failed
        }
    }
    
    @Override
    public int read() throws IOException {
        checkTransformException();
        return pipedInput.read();
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        checkTransformException();
        return pipedInput.read(b);
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkTransformException();
        return pipedInput.read(b, off, len);
    }
    
    @Override
    public int available() throws IOException {
        checkTransformException();
        return pipedInput.available();
    }
    
    @Override
    public void close() throws IOException {
        try {
            pipedInput.close();
        } finally {
            super.close();
            // Wait for transform thread to complete
            if (transformThread != null && transformThread.isAlive()) {
                try {
                    transformThread.join(5000); // Wait max 5 seconds
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted while waiting for transform thread to complete");
                }
            }
        }
    }
    
    /**
     * Check if transformation thread encountered an exception.
     */
    private void checkTransformException() throws IOException {
        if (transformException != null) {
            throw new IOException("Error during data transformation", transformException);
        }
    }

    /**
    * Record of a transformation that was applied
    */
    @AllArgsConstructor
    public static class TransformationRecord {
        public final long lineNumber;
        public final String originalLine;
        public final String transformedLine;
    }
    
}
