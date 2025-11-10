-- Title D App Tables - Direct SQL Statements
-- Execute these statements directly in your PostgreSQL database

-- Create the schema first if it doesn't exist
CREATE SCHEMA IF NOT EXISTS title_d_app;

-- ================================
-- PM TABLES (Property Management)
-- ================================

-- PM1: Property Master Table
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
DROP TABLE IF EXISTS title_d_app.pm4;
CREATE TABLE title_d_app.pm4 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    parent_block_num VARCHAR(5) NOT NULL,
    parent_property_id_num VARCHAR(4) NOT NULL
);
CREATE INDEX pm4_blk_pid ON title_d_app.pm4 USING btree (block_num, property_id_num);

-- PM5: Property Thumbnail/Description
DROP TABLE IF EXISTS title_d_app.pm5;
CREATE TABLE title_d_app.pm5 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    thumbnail_desc TEXT NULL
);
CREATE UNIQUE INDEX pm5_blk_pid ON title_d_app.pm5 USING btree (block_num, property_id_num);

-- PM6: Property Comments
DROP TABLE IF EXISTS title_d_app.pm6;
CREATE TABLE title_d_app.pm6 (
    block_num VARCHAR(5) NOT NULL,
    property_id_num VARCHAR(4) NOT NULL,
    comment_desc VARCHAR(255) NULL
);
CREATE UNIQUE INDEX pm6_blk_pid ON title_d_app.pm6 USING btree (block_num, property_id_num);

-- PM7: Property Address Information
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
DROP TABLE IF EXISTS title_d_app.im3;
CREATE TABLE title_d_app.im3 (
    lro_num VARCHAR(2) NOT NULL,
    instrument_num VARCHAR(10) NOT NULL,
    comment_desc VARCHAR(255) NULL
);
CREATE UNIQUE INDEX im3_lro_instr ON title_d_app.im3 USING btree (instrument_num, lro_num);
