package teranet.mapdev.ingest.controller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("integration-test")
@TestPropertySource(locations = "classpath:application-integration-test.properties")
public class DelimitedFileControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    public void setUp() {
        // Clean the ingestion_manifest table before each test
        jdbcTemplate.execute("DELETE FROM title_d_app_int.ingestion_manifest");
    }

    @Test
    @Transactional
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
                .andExpect(content().json("{\"status\":\"COMPLETED\"}"));
    }

    @Test
    @Transactional
    public void testUploadTsvFile() throws Exception {
        // Create a sample TSV file without headers, data in im1 table column order
        // im1 columns: lro_num(2), instrument_num(10), registration_date,
        // interest_code(6), etc.
        String tsvContent = "12\tINST123456\t2023-01-01 00:00:00\tINT001\tQUAL01\n34\tINST789012\t2023-02-01 00:00:00\tINT002\tQUAL02";
        MockMultipartFile file = new MockMultipartFile("file", "IM163.tsv", "text/tab-separated-values",
                tsvContent.getBytes());

        mockMvc.perform(multipart("/api/v1/ingest/delimited/upload")
                .file(file)
                .param("format", "tsv")
                .param("hasHeaders", "false")
                .param("routeByFilename", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"));
    }

    @Test
    @Transactional
    public void testUploadInvalidFile() throws Exception {
        // Empty file
        MockMultipartFile file = new MockMultipartFile("file", "empty.csv", "text/csv", "".getBytes());

        mockMvc.perform(multipart("/api/v1/ingest/delimited/upload")
                .file(file)
                .param("format", "csv"))
                .andExpect(status().isBadRequest());
    }
}