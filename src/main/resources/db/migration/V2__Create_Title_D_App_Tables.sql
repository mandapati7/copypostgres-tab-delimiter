-- Title D App Tables Migration
-- Migration: V2__Create_Title_D_App_Tables.sql
-- Description: Creates all title_d_app tables (pm1-pm7 and im1-im3) for staging property and instrument data

-- Set search path to title_d_app schema
-- CREATE SCHEMA IF NOT EXISTS title_d_app;
-- SET search_path TO title_d_app, public;

-- ================================
-- PM TABLES (Property Management)
-- ================================

-- PM1: Property Master Table
-- Contains core property information including registration, interest codes, and parent-child relationships
DROP TABLE IF EXISTS title_d_app.pm1;
CREATE TABLE title_d_app.pm1 (
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
CREATE UNIQUE INDEX pm1_blk_pid ON title_d_app.pm1 USING btree (block_num, property_id_num);

-- PM2: Property-Instrument Relationship Table
-- Links properties to land registration office (LRO) numbers and instrument numbers
DROP TABLE IF EXISTS title_d_app.pm2;
CREATE TABLE title_d_app.pm2 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    rule_out_ind VARCHAR(1) NULL,
    seq_id NUMERIC(38) NULL
);
CREATE UNIQUE INDEX pm2_seq_id_idx ON title_d_app.pm2 USING btree (seq_id);

-- PM3: Alternative Party Information
-- Stores alternate party identifiers, names, and ownership details
DROP TABLE IF EXISTS title_d_app.pm3;
CREATE TABLE title_d_app.pm3 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    alt_party_id_code VARCHAR(6) NULL,
    alt_party_name VARCHAR(128) NULL,
    owner_capacity_code VARCHAR(6) NULL,
    ownership_share_desc VARCHAR(12) NULL,
    owner_name VARCHAR(128) NULL,
    alt_party_b_day TIMESTAMP(0) NULL
);
CREATE INDEX pm3_blk_pid ON title_d_app.pm3 USING btree (block_num, property_id_num);

-- PM4: Parent Property Relationships
-- Tracks hierarchical property relationships (parent-child)
DROP TABLE IF EXISTS title_d_app.pm4;
CREATE TABLE title_d_app.pm4 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    parent_block_num VARCHAR(5) NOT NULL,
    parent_property_id_num VARCHAR(4) NOT NULL
);
CREATE INDEX pm4_blk_pid ON title_d_app.pm4 USING btree (block_num, property_id_num);

-- PM5: Property Thumbnail/Description
-- Stores property thumbnail images or text descriptions
DROP TABLE IF EXISTS title_d_app.pm5;
CREATE TABLE title_d_app.pm5 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    thumbnail_desc TEXT NULL
);
CREATE UNIQUE INDEX pm5_blk_pid ON title_d_app.pm5 USING btree (block_num, property_id_num);

-- PM6: Property Comments
-- Stores additional comments or notes related to properties
DROP TABLE IF EXISTS title_d_app.pm6;
CREATE TABLE title_d_app.pm6 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    comment_desc VARCHAR(255) NULL
);
CREATE UNIQUE INDEX pm6_blk_pid ON title_d_app.pm6 USING btree (block_num, property_id_num);

-- PM7: Property Address Information
-- Contains detailed property address including municipality, postal region, street details, and unit information
DROP TABLE IF EXISTS title_d_app.pm7;
CREATE TABLE title_d_app.pm7 (
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
CREATE UNIQUE INDEX pm7_blk_pid ON title_d_app.pm7 USING btree (block_num, property_id_num);

-- ================================
-- IM TABLES (Instrument Management)
-- ================================

-- IM1: Instrument Master Table
-- Contains core instrument information including registration dates, interest codes, and references
DROP TABLE IF EXISTS title_d_app.im1;
CREATE TABLE title_d_app.im1 (
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
CREATE UNIQUE INDEX im1_lro_instr ON title_d_app.im1 USING btree (instrument_num, lro_num);

-- IM2: Instrument Party Information
-- Links instruments to parties with qualifier codes, names, capacity codes, and dates
DROP TABLE IF EXISTS title_d_app.im2;
CREATE TABLE title_d_app.im2 (
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    to_from_ind VARCHAR(1) NULL,
    qualifier_code VARCHAR(6) NULL,
    qualifier_name VARCHAR(128) NULL,
    capacity_code VARCHAR(6) NULL,
    im_share VARCHAR(12) NULL,
    "name" VARCHAR(128) NULL,
    party_b_day TIMESTAMP(0) NULL
);
CREATE INDEX im2_lro_instr ON title_d_app.im2 USING btree (instrument_num, lro_num);

-- IM3: Instrument Comments
-- Stores comments or additional notes related to instruments
DROP TABLE IF EXISTS title_d_app.im3;
CREATE TABLE title_d_app.im3 (
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    comment_desc VARCHAR(255) NULL
);
CREATE UNIQUE INDEX im3_lro_instr ON title_d_app.im3 USING btree (instrument_num, lro_num);

-- ================================
-- GRANT PERMISSIONS
-- ================================

-- Grant necessary permissions to application user (adjust username as needed)
-- GRANT USAGE ON SCHEMA title_d_app TO your_app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA title_d_app TO your_app_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA title_d_app TO your_app_user;

-- ================================
-- COMMENTS FOR DOCUMENTATION
-- ================================

COMMENT ON TABLE title_d_app.pm1 IS 'Property Master Table - Core property information';
COMMENT ON TABLE title_d_app.pm2 IS 'Property-Instrument Relationship - Links properties to instruments';
COMMENT ON TABLE title_d_app.pm3 IS 'Alternative Party Information - Alternate party details and ownership';
COMMENT ON TABLE title_d_app.pm4 IS 'Parent Property Relationships - Hierarchical property structure';
COMMENT ON TABLE title_d_app.pm5 IS 'Property Thumbnails - Visual or text descriptions';
COMMENT ON TABLE title_d_app.pm6 IS 'Property Comments - Additional property notes';
COMMENT ON TABLE title_d_app.pm7 IS 'Property Address Information - Detailed location data';

COMMENT ON TABLE title_d_app.im1 IS 'Instrument Master Table - Core instrument registration data';
COMMENT ON TABLE title_d_app.im2 IS 'Instrument Party Information - Party relationships and details';
COMMENT ON TABLE title_d_app.im3 IS 'Instrument Comments - Additional instrument notes';
