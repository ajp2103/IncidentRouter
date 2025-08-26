CREATE TABLE MEMBER_DATA (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  assignment_group_sys_id VARCHAR(64) NOT NULL,
  member_sys_id VARCHAR(64) NOT NULL,
  member_name VARCHAR(200),
  role ENUM('L1','L2','L3','SME') DEFAULT 'L2',
  shift_start_time TIME NOT NULL,
  shift_end_time TIME NOT NULL,
  shift_days VARCHAR(64) NOT NULL,
  weekend_shift_flag TINYINT(1) DEFAULT 0,
  active TINYINT(1) DEFAULT 1,
  weight_modifier DECIMAL(5,3) DEFAULT 1.000,
  last_manual_update_by VARCHAR(200),
  last_manual_update_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY ux_member_group (assignment_group_sys_id, member_sys_id)
);

CREATE TABLE ASSIGNMENT_HISTORY (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  incident_sys_id VARCHAR(64),
  incident_number VARCHAR(50),
  assigned_to_member_sys_id VARCHAR(64),
  assignment_timestamp DATETIME,
  algorithm_snapshot JSON,
  success TINYINT(1) DEFAULT 1,
  created_by VARCHAR(64),
  INDEX idx_member_time (assigned_to_member_sys_id, assignment_timestamp)
);