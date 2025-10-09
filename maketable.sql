CREATE DATABASE IF NOT EXISTS electricity_db DEFAULT CHARSET utf8mb4;
USE electricity_db;

CREATE TABLE electricity_db.current_node_electricity_price (
    id INT AUTO_INCREMENT PRIMARY KEY,
    record_date DATE,
    record_time TIME,
    type VARCHAR(50),
    channel_name VARCHAR(50),
    value DECIMAL(10, 2),
    created_at DATETIME,
    sheet_name VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE electricity_db.realtime_node_electricity_price (
    id INT AUTO_INCREMENT PRIMARY KEY,
    record_date DATE,
    record_time TIME,
    type VARCHAR(50),
    channel_name VARCHAR(50),
    value DECIMAL(10, 2),
    created_at DATETIME,
    sheet_name VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
