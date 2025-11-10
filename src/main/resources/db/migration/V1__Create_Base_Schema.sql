-- PostgreSQL CSV Loader Database Schema
-- Migration: V1__Create_Base_Schema.sql

-- Create schemas for organization
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS title_d_app;

-- ================================
-- ENUMS AND TYPES
-- ================================

-- Create enum types
CREATE TYPE audit.ingestion_status AS ENUM ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELLED');
CREATE TYPE core.order_status AS ENUM ('NEW', 'PROCESSING', 'COMPLETED', 'CANCELLED');

-- ================================
-- AUDIT SCHEMA - Ingestion tracking
-- ================================

-- Table to track file ingestion attempts and metadata
CREATE TABLE audit.ingestion_manifest (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    file_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(500),
    file_size_bytes BIGINT NOT NULL,
    file_checksum VARCHAR(64) NOT NULL, -- SHA-256 hash for idempotency
    content_type VARCHAR(100),
    
    -- Processing metadata
    status audit.ingestion_status NOT NULL DEFAULT 'PENDING',
    total_records BIGINT DEFAULT 0,
    processed_records BIGINT DEFAULT 0,
    failed_records BIGINT DEFAULT 0,
    
    -- Timing information
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    processing_duration_ms BIGINT,
    
    -- Error handling
    error_message TEXT,
    error_details JSONB,
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system',
    
    -- Constraints
    CONSTRAINT chk_file_size CHECK (file_size_bytes >= 0),
    CONSTRAINT chk_records_non_negative CHECK (
        total_records >= 0 AND 
        processed_records >= 0 AND 
        failed_records >= 0 AND
        processed_records + failed_records <= total_records
    )
);

-- Indexes for performance
CREATE INDEX idx_ingestion_manifest_batch_id ON audit.ingestion_manifest(batch_id);
CREATE INDEX idx_ingestion_manifest_status ON audit.ingestion_manifest(status);
CREATE INDEX idx_ingestion_manifest_checksum ON audit.ingestion_manifest(file_checksum);
CREATE INDEX idx_ingestion_manifest_created_at ON audit.ingestion_manifest(created_at);
CREATE INDEX idx_ingestion_manifest_file_name ON audit.ingestion_manifest(file_name);

-- ================================
-- STAGING SCHEMA - Raw CSV data
-- ================================

-- Generic staging table for CSV data
-- This will hold raw CSV records before validation and transformation
CREATE TABLE staging.csv_raw_data (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    row_number INTEGER NOT NULL,
    raw_data JSONB NOT NULL, -- Store raw CSV row as JSON for flexibility
    
    -- Processing status per row
    is_processed BOOLEAN DEFAULT FALSE,
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT[],
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP WITH TIME ZONE,
    
    -- Foreign key to manifest
    CONSTRAINT fk_csv_raw_data_batch_id 
        FOREIGN KEY (batch_id) REFERENCES audit.ingestion_manifest(batch_id)
        ON DELETE CASCADE
);

-- Indexes for staging queries
CREATE INDEX idx_csv_raw_data_batch_id ON staging.csv_raw_data(batch_id);
CREATE INDEX idx_csv_raw_data_processed ON staging.csv_raw_data(is_processed, batch_id);
CREATE INDEX idx_csv_raw_data_valid ON staging.csv_raw_data(is_valid, batch_id);

-- ================================
-- CORE SCHEMA - Business entities
-- ================================

-- Example: Orders table (customize based on your CSV structure)
CREATE TABLE core.orders (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    order_date DATE NOT NULL,
    status core.order_status NOT NULL DEFAULT 'NEW',
    
    -- Audit trail - track which batch created this record
    source_batch_id UUID,
    source_row_number INTEGER,
    
    -- Standard audit fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_orders_quantity CHECK (quantity > 0),
    CONSTRAINT chk_orders_unit_price CHECK (unit_price >= 0),
    CONSTRAINT chk_orders_total_amount CHECK (total_amount >= 0),
    
    -- Foreign key to track source
    CONSTRAINT fk_orders_source_batch_id 
        FOREIGN KEY (source_batch_id) REFERENCES audit.ingestion_manifest(batch_id)
        ON DELETE SET NULL
);

-- Indexes for business queries
CREATE INDEX idx_orders_order_id ON core.orders(order_id);
CREATE INDEX idx_orders_customer_id ON core.orders(customer_id);
CREATE INDEX idx_orders_product_id ON core.orders(product_id);
CREATE INDEX idx_orders_order_date ON core.orders(order_date);
CREATE INDEX idx_orders_status ON core.orders(status);
CREATE INDEX idx_orders_source_batch ON core.orders(source_batch_id);

-- ================================
-- FUNCTIONS AND TRIGGERS
-- ================================

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for updated_at
CREATE TRIGGER trigger_update_ingestion_manifest_updated_at
    BEFORE UPDATE ON audit.ingestion_manifest
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_update_orders_updated_at
    BEFORE UPDATE ON core.orders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ================================
-- VIEWS FOR MONITORING
-- ================================

-- View for ingestion monitoring
CREATE OR REPLACE VIEW audit.ingestion_summary AS
SELECT 
    DATE(created_at) as ingestion_date,
    status,
    COUNT(*) as batch_count,
    SUM(total_records) as total_records,
    SUM(processed_records) as processed_records,
    SUM(failed_records) as failed_records,
    AVG(processing_duration_ms) as avg_processing_time_ms,
    SUM(file_size_bytes) as total_file_size_bytes
FROM audit.ingestion_manifest
GROUP BY DATE(created_at), status
ORDER BY ingestion_date DESC, status;

-- View for current processing status
CREATE OR REPLACE VIEW audit.active_batches AS
SELECT 
    batch_id,
    file_name,
    status,
    total_records,
    processed_records,
    (processed_records::FLOAT / NULLIF(total_records, 0) * 100)::DECIMAL(5,2) as completion_percentage,
    started_at,
    EXTRACT(EPOCH FROM (COALESCE(completed_at, CURRENT_TIMESTAMP) - started_at))::INTEGER as elapsed_seconds
FROM audit.ingestion_manifest
WHERE status IN ('PENDING', 'PROCESSING')
ORDER BY started_at;

-- Grant permissions (adjust as needed for your security model)
-- GRANT USAGE ON SCHEMA audit TO csv_loader_app;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA audit TO csv_loader_app;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA audit TO csv_loader_app;

-- GRANT USAGE ON SCHEMA staging TO csv_loader_app;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO csv_loader_app;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO csv_loader_app;

-- GRANT USAGE ON SCHEMA core TO csv_loader_app;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA core TO csv_loader_app;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA core TO csv_loader_app;