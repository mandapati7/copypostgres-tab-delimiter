package teranet.mapdev.ingest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Service responsible for file checksum calculation and compression handling.
 * 
 * Features:
 * - SHA-256 checksum calculation for idempotency
 * - Support for compressed files (.gz, .zip)
 * - Automatic decompression stream wrapping
 * 
 * This service is stateless and can be safely used concurrently.
 */
@Service
public class FileChecksumService {
    
    private static final Logger logger = LoggerFactory.getLogger(FileChecksumService.class);
    
    /**
     * Calculate SHA-256 checksum for file idempotency.
     * Automatically handles compressed files (.gz, .zip).
     * 
     * @param file the file to calculate checksum for
     * @return SHA-256 checksum as hexadecimal string
     * @throws IOException if file cannot be read
     * @throws NoSuchAlgorithmException if SHA-256 algorithm is not available
     */
    public String calculateFileChecksum(MultipartFile file) throws IOException, NoSuchAlgorithmException {
        logger.debug("Calculating SHA-256 checksum for file: {}", file.getOriginalFilename());
        
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        
        try (InputStream inputStream = file.getInputStream()) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
        }
        
        byte[] hashBytes = digest.digest();
        String checksum = bytesToHex(hashBytes);
        
        logger.debug("Calculated checksum for {}: {}", file.getOriginalFilename(), checksum);
        
        return checksum;
    }
    
    /**
     * Get decompressed input stream for file.
     * Automatically detects and handles:
     * - .gz files → GZIPInputStream
     * - .zip files → ZipInputStream (first CSV entry)
     * - plain files → original InputStream
     * 
     * @param file the file to decompress
     * @return decompressed InputStream (or original if not compressed)
     * @throws IOException if decompression fails
     */
    public InputStream getDecompressedInputStream(MultipartFile file) throws IOException {
        String filename = file.getOriginalFilename();
        InputStream inputStream = file.getInputStream();
        
        if (filename != null) {
            if (filename.endsWith(".gz")) {
                logger.debug("Decompressing GZIP file: {}", filename);
                return new GZIPInputStream(inputStream);
            } else if (filename.endsWith(".zip")) {
                logger.debug("Decompressing ZIP file: {}", filename);
                ZipInputStream zipStream = new ZipInputStream(inputStream);
                ZipEntry entry = zipStream.getNextEntry();
                if (entry != null && entry.getName().endsWith(".csv")) {
                    logger.debug("Found CSV entry in ZIP: {}", entry.getName());
                    return zipStream;
                }
                logger.warn("No CSV entry found in ZIP file: {}", filename);
            }
        }
        
        logger.debug("Using plain input stream for: {}", filename);
        return inputStream; // Plain CSV
    }
    
    /**
     * Convert byte array to hexadecimal string.
     * 
     * @param bytes the byte array to convert
     * @return hexadecimal string representation
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        
        return hexString.toString();
    }
}
