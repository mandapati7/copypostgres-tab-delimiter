-- Staging Tables for Title D App Data
-- These tables mirror the title_d_app tables with an added batch_id column for tracking uploads

-- Create the staging schema if needed
-- CREATE SCHEMA IF NOT EXISTS public;

-- ================================
-- STAGING PM TABLES
-- ================================

-- Staging PM1: Property Master Table
DROP TABLE IF EXISTS staging_pm1;
CREATE TABLE staging_pm1 (
    batch_id UUID NOT NULL,
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    registration_system_code VARCHAR(1) NULL,
    property_type_code VARCHAR(1) NULL,
    last_update_date TIMESTAMP(0) NULL,
    interest_code VARCHAR(6) NULL,
    qualifier_code VARCHAR(6) NULL,
    origin_code VARCHAR(1) NULL,
    status_code VARCHAR(1) NULL,
    parent_block VARCHAR(5) NULL,
    parent_property_id VARCHAR(4) NULL,
    parent_count NUMERIC NULL,
    child_from_block VARCHAR(5) NULL,
    child_from_property VARCHAR(4) NULL,
    child_to_block VARCHAR(5) NULL,
    child_to_property VARCHAR(4) NULL,
    polaris_restriction_ind VARCHAR(1) NULL
);

-- Staging PM2: Property-Instrument Relationship Table
DROP TABLE IF EXISTS staging_pm2;
CREATE TABLE staging_pm2 (
    batch_id UUID NOT NULL,
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    rule_out_ind VARCHAR(1) NULL,
    seq_id NUMERIC(38) NULL
);

-- Staging PM3: Alternative Party Information
DROP TABLE IF EXISTS staging_pm3;
CREATE TABLE staging_pm3 (
    batch_id UUID NOT NULL,
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    alt_party_id_code VARCHAR(6) NULL,
    alt_party_name VARCHAR(128) NULL,
    owner_capacity_code VARCHAR(6) NULL,
    ownership_share_desc VARCHAR(12) NULL,
    owner_name VARCHAR(128) NULL,
    alt_party_b_day TIMESTAMP(0) NULL
);

-- Staging PM4: Parent Property Relationships
DROP TABLE IF EXISTS staging_pm4;
CREATE TABLE staging_pm4 (
    batch_id UUID NOT NULL,
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    parent_block_num VARCHAR(5) NOT NULL,
    parent_property_id_num VARCHAR(4) NOT NULL
);

-- Staging PM5: Property Thumbnail/Description
DROP TABLE IF EXISTS staging_pm5;
CREATE TABLE staging_pm5 (
    batch_id UUID NOT NULL,
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    thumbnail_desc TEXT NULL
);

-- Staging PM6: Property Comments
DROP TABLE IF EXISTS staging_pm6;
CREATE TABLE staging_pm6 (
    batch_id UUID NOT NULL,
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    comment_desc VARCHAR(255) NULL
);

-- Staging PM7: Property Address Information
DROP TABLE IF EXISTS staging_pm7;
CREATE TABLE staging_pm7 (
    batch_id UUID NOT NULL,
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    assessment_num VARCHAR(15) NULL,
    municipality_name VARCHAR(24) NULL,
    postal_region_name VARCHAR(26) NULL,
    street_num NUMERIC NULL,
    street_suffix VARCHAR(6) NULL,
    street_name VARCHAR(34) NULL,
    unit_num VARCHAR(6) NULL,
    unit_type_code VARCHAR(6) NULL
);

-- ================================
-- STAGING IM TABLES
-- ================================

-- Staging IM1: Instrument Master Table
DROP TABLE IF EXISTS staging_im1;
CREATE TABLE staging_im1 (
    batch_id UUID NOT NULL,
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    registration_date TIMESTAMP(0) NULL,
    interest_code VARCHAR(6) NULL,
    qualifier_code VARCHAR(6) NULL,
    ref_charge_instr_num VARCHAR(10) NULL,
    ref_assignment_instr_num VARCHAR(10) NULL,
    consideration_amt NUMERIC NULL,
    last_update_date TIMESTAMP(0) NULL,
    instrument_selection_code VARCHAR(1) NULL,
    instrument_sub_type_code VARCHAR(8) NULL,
    origin_ind_code VARCHAR(1) NULL,
    instrument_registration_stage VARCHAR(1) NULL
);

-- Staging IM2: Instrument Party Information
DROP TABLE IF EXISTS staging_im2;
CREATE TABLE staging_im2 (
    batch_id UUID NOT NULL,
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    to_from_ind VARCHAR(1) NULL,
    qualifier_code VARCHAR(10) NULL,
    qualifier_name VARCHAR(128) NULL,
    capacity_code VARCHAR(6) NULL,
    im_share VARCHAR(12) NULL,
    "name" VARCHAR(128) NULL,
    party_b_day TIMESTAMP(0) NULL
);

-- Staging IM3: Instrument Comments
DROP TABLE IF EXISTS staging_im3;
CREATE TABLE staging_im3 (
    batch_id UUID NOT NULL,
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    comment_desc VARCHAR(255) NULL
);

-- Create indexes on batch_id for performance
CREATE INDEX idx_staging_pm1_batch ON staging_pm1(batch_id);
CREATE INDEX idx_staging_pm2_batch ON staging_pm2(batch_id);
CREATE INDEX idx_staging_pm3_batch ON staging_pm3(batch_id);
CREATE INDEX idx_staging_pm4_batch ON staging_pm4(batch_id);
CREATE INDEX idx_staging_pm5_batch ON staging_pm5(batch_id);
CREATE INDEX idx_staging_pm6_batch ON staging_pm6(batch_id);
CREATE INDEX idx_staging_pm7_batch ON staging_pm7(batch_id);
CREATE INDEX idx_staging_im1_batch ON staging_im1(batch_id);
CREATE INDEX idx_staging_im2_batch ON staging_im2(batch_id);
CREATE INDEX idx_staging_im3_batch ON staging_im3(batch_id);
