-- Create database and schema
CREATE OR REPLACE DATABASE spy_agency;
CREATE OR REPLACE SCHEMA spy_agency.intel;
CREATE OR REPLACE WAREHOUSE spy_agency_wh;
CREATE OR REPLACE STAGE spy_agency.intel.stage
Directory = (ENABLE = True);

-- Create table for intercepted spy messages
CREATE or Replace TABLE spy_missions (
    mission_id VARCHAR,
    message_text TEXT,
    sender_code VARCHAR,
    receiver_code VARCHAR,
    mission_location VARCHAR,
    transmission_date TIMESTAMP,
    encryption_level VARCHAR
);

-- Create table for mission success reports
CREATE or REPLACE TABLE spy_reports (
    report_id VARCHAR,
    agent_code VARCHAR,
    mission_id VARCHAR,
    mission_outcome VARCHAR,
    suspected_double_agent BOOLEAN,
    last_known_location VARCHAR,
    failure_reason TEXT
);

INSERT INTO spy_missions (mission_id, message_text, sender_code, receiver_code, mission_location, transmission_date, encryption_level) VALUES
('M001', 'Package secured. Rendezvous at midnight. Codeword: Shadow.', 'X7A', 'K9Q', 'Berlin', '2025-03-10 23:45:00', 'HIGH'),
('M002', 'New orders received. Proceed with caution.', 'B3Z', 'X7A', 'London', '2025-03-11 04:30:00', 'MEDIUM'),
('M003', 'Possible breach. Suspect a mole in the network.', 'K9Q', 'B3Z', 'Paris', '2025-03-09 18:20:00', 'HIGH'),
('M004', 'Intercepted message decoded. Proceed to fallback point.', 'A1D', 'T2M', 'Rome', '2025-03-08 12:15:00', 'LOW'),
('M005', 'Meet at the abandoned warehouse. No electronic devices.', 'F7X', 'L3P', 'Moscow', '2025-03-07 22:00:00', 'HIGH'),
('M006', 'The raven flies at dawn. Extract the target.', 'T2M', 'F7X', 'Madrid', '2025-03-06 06:30:00', 'MEDIUM'),
('M007', 'Radio silence until further notice. Burn after reading.', 'L3P', 'A1D', 'New York', '2025-03-05 19:45:00', 'HIGH'),
('M008', 'The safe house is compromised. Move to Sector 9.', 'K9Q', 'T2M', 'Tokyo', '2025-03-04 14:20:00', 'HIGH'),
('M009', 'Retrieve the documents from the courier. Passphrase: Blue Sky.', 'B3Z', 'X7A', 'Los Angeles', '2025-03-03 09:10:00', 'LOW'),
('M010', 'Under surveillance. Abort mission immediately.', 'F7X', 'L3P', 'Sydney', '2025-03-02 21:30:00', 'HIGH'),
('M011', 'Safe passage secured. Travel via Route 17.', 'X7A', 'B3Z', 'Dubai', '2025-03-01 15:00:00', 'MEDIUM'),
('M012', 'Decoy deployed. Confuse the trackers. Keyphrase: Silver Wave.', 'A1D', 'K9Q', 'Cape Town', '2025-02-28 08:55:00', 'LOW'),
('M013', 'Final instructions in the briefcase. Retrieve immediately. Keyphrase: Red Falcon.', 'L3P', 'T2M', 'Toronto', '2025-02-27 11:40:00', 'HIGH'),
('M014', 'A storm is coming. Prepare accordingly.', 'B3Z', 'X7A', 'Mexico City', '2025-02-26 17:10:00', 'MEDIUM'),
('M015', 'Meet the contact in the underground station.', 'K9Q', 'L3P', 'Hong Kong', '2025-02-25 10:05:00', 'HIGH'),
('M016', 'Secure the perimeter. No one gets in or out.', 'T2M', 'A1D', 'Istanbul', '2025-02-24 13:20:00', 'HIGH'),
('M017', 'Last transmission compromised. Use alternate channel.', 'L3P', 'B3Z', 'Bangkok', '2025-02-23 23:50:00', 'MEDIUM'),
('M018', 'The eagle has landed. Proceed as planned.', 'F7X', 'K9Q', 'Seoul', '2025-02-22 07:15:00', 'LOW'),
('M019', 'The package is in the vault. Keyphrase: Golden Dawn.', 'A1D', 'X7A', 'Athens', '2025-02-21 16:35:00', 'HIGH'),
('M020', 'Do not engage until further notice.', 'B3Z', 'T2M', 'Stockholm', '2025-02-20 05:50:00', 'MEDIUM');


INSERT INTO spy_reports (report_id, agent_code, mission_id, mission_outcome, suspected_double_agent, last_known_location, failure_reason) VALUES
('R001', 'X7A', 'M001', 'Success', FALSE, 'Berlin', 'N/A'),
('R002', 'B3Z', 'M002', 'Compromised', TRUE, 'London', 'Intel leak'),
('R003', 'K9Q', 'M003', 'Failed', TRUE, 'Paris', 'Double agent interference'),
('R004', 'A1D', 'M004', 'Success', FALSE, 'Rome', 'N/A'),
('R005', 'F7X', 'M005', 'Success', FALSE, 'Moscow', 'N/A'),
('R006', 'T2M', 'M006', 'Success', FALSE, 'Madrid', 'N/A'),
('R007', 'L3P', 'M007', 'Failed', FALSE, 'New York', 'Communication breakdown'),
('R008', 'K9Q', 'M008', 'Compromised', TRUE, 'Tokyo', 'Safe house compromised'),
('R009', 'B3Z', 'M009', 'Success', FALSE, 'Los Angeles', 'N/A'),
('R010', 'F7X', 'M010', 'Compromised', FALSE, 'Sydney', 'Surveillance detected'),
('R011', 'X7A', 'M011', 'Success', FALSE, 'Dubai', 'N/A'),
('R012', 'A1D', 'M012', 'Success', FALSE, 'Cape Town', 'N/A'),
('R013', 'L3P', 'M013', 'Compromised', FALSE, 'Toronto', 'Intercepted courier'),
('R014', 'B3Z', 'M014', 'Failed', FALSE, 'Mexico City', 'Equipment malfunction'),
('R015', 'K9Q', 'M015', 'Success', FALSE, 'Hong Kong', 'N/A'),
('R016', 'T2M', 'M016', 'Compromised', FALSE, 'Istanbul', 'Security breach'),
('R017', 'L3P', 'M017', 'Failed', FALSE, 'Bangkok', 'Intercepted transmission'),
('R018', 'F7X', 'M018', 'Success', FALSE, 'Seoul', 'N/A'),
('R019', 'A1D', 'M019', 'Compromised', FALSE, 'Athens', 'Unauthorized access'),
('R020', 'B3Z', 'M020', 'Success', FALSE, 'Stockholm', 'N/A');

CREATE OR REPLACE CORTEX SEARCH SERVICE spy_mission_search
  ON message_text
  ATTRIBUTES mission_id, sender_code, receiver_code, mission_location, transmission_date, encryption_level
  WAREHOUSE = spy_agency_wh
  TARGET_LAG = '1 minute'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
  AS (
    SELECT
        mission_id, 
        message_text, 
        sender_code, 
        receiver_code, 
        mission_location, 
        transmission_date, 
        encryption_level
    FROM spy_missions
);