-- ==============================
-- MySQL Database & Tables Setup
-- ==============================

-- Tạo database
CREATE DATABASE IF NOT EXISTS etl_db;
USE etl_db;

-- Bảng tracking_events (rút gọn, các cột quan trọng)
CREATE TABLE IF NOT EXISTS tracking_events (
    uuid VARCHAR(50) PRIMARY KEY,
    create_time VARCHAR(50),
    job_id VARCHAR(50),
    custom_track VARCHAR(50),
    bid VARCHAR(50),
    campaign_id VARCHAR(50),
    group_id VARCHAR(50),
    publisher_id VARCHAR(50),
    ev VARCHAR(50),
    ts VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng search_by_jobid
CREATE TABLE IF NOT EXISTS search_by_jobid (
    job_id VARCHAR(50) PRIMARY KEY,
    company_name VARCHAR(255),
    title VARCHAR(255),
    city_name VARCHAR(255),
    state VARCHAR(255),
    major_category VARCHAR(255),
    minor_category VARCHAR(255),
    pay_from FLOAT,
    pay_to FLOAT,
    pay_type VARCHAR(50),
    work_schedule VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
