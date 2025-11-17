package teranet.mapdev.ingest.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("integration-test")
@TestPropertySource(locations = "classpath:application-integration-test.properties")
public class DelimitedFileControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testUploadCsvFile() throws Exception {
        // Create a sample CSV file with headers matching pm1 table columns
        String csvContent = "block_num,property_id_num,registration_system_code\n12345,6789,A\n54321,9876,B";
        MockMultipartFile file = new MockMultipartFile("file", "PM162.csv", "text/csv", csvContent.getBytes());

        mockMvc.perform(multipart("/api/v1/ingest/delimited/upload")
                .file(file)
                .param("format", "csv")
                .param("hasHeaders", "true")
                .param("routeByFilename", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"));
    }

    @Test
    public void testUploadTsvFile() throws Exception {
        // Create a sample TSV file without headers, data in im1 table column order
        // im1 columns: block_num, property_id_num, etc.
        String tsvContent = "12345\t6789\tA\n54321\t9876\tB";
        MockMultipartFile file = new MockMultipartFile("file", "IM162.tsv", "text/tab-separated-values", tsvContent.getBytes());

        mockMvc.perform(multipart("/api/v1/ingest/delimited/upload")
                .file(file)
                .param("format", "tsv")
                .param("hasHeaders", "false")
                .param("routeByFilename", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"));
    }

    @Test
    public void testUploadInvalidFile() throws Exception {
        // Empty file
        MockMultipartFile file = new MockMultipartFile("file", "empty.csv", "text/csv", "".getBytes());

        mockMvc.perform(multipart("/api/v1/ingest/delimited/upload")
                .file(file)
                .param("format", "csv"))
                .andExpect(status().isBadRequest());
    }
}