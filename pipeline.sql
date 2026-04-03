### Pipeline Script: Automates classification of appointments
### Uses Streams for change tracking and Tasks for scheduled processing


-- Create a stream to track changes in appointments table for attended patients
CREATE OR REPLACE STREAM attended_stream
ON TABLE appointments;

-- Create another stream to track changes for missed patients
CREATE OR REPLACE STREAM missed_stream
ON TABLE appointments;

------------------------------------------------------------

-- Create a task to process attended patients automatically
CREATE OR REPLACE TASK process_attended_task
WAREHOUSE = COMPUTE_WH              -- Compute resource used to run the task
SCHEDULE = '1 MINUTE'               -- Task runs every 1 minute
AS

-- Merge new/updated attended records into attended_patients table
MERGE INTO attended_patients t
USING (
    SELECT * FROM attended_stream WHERE status = 'Attended'   -- Get only attended records from stream
) s
ON t.patient_id = s.patient_id     -- Match records using patient_id

-- If record already exists, update it
WHEN MATCHED THEN UPDATE SET
    t.patient_name = s.patient_name,
    t.appointment_date = s.appointment_date,
    t.appointment_time = s.appointment_time,
    t.status = s.status

-- If record does not exist, insert new row
WHEN NOT MATCHED THEN
INSERT VALUES (
    s.patient_id,
    s.patient_name,
    s.appointment_date,
    s.appointment_time,
    s.status
);

------------------------------------------------------------

-- Create a task to process missed patients automatically
CREATE OR REPLACE TASK process_missed_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS

-- Merge new/updated missed records into missed_patients table
MERGE INTO missed_patients t
USING (
    SELECT * FROM missed_stream WHERE status = 'Missed'   -- Filter only missed records
) s
ON t.patient_id = s.patient_id

-- Update existing records
WHEN MATCHED THEN UPDATE SET
    t.patient_name = s.patient_name,
    t.appointment_date = s.appointment_date,
    t.appointment_time = s.appointment_time,
    t.status = s.status

-- Insert new records if not present
WHEN NOT MATCHED THEN
INSERT VALUES (
    s.patient_id,
    s.patient_name,
    s.appointment_date,
    s.appointment_time,
    s.status
);

------------------------------------------------------------

-- Start (resume) both tasks so automation begins
ALTER TASK process_attended_task RESUME;
ALTER TASK process_missed_task RESUME;

------------------------------------------------------------

-- Insert new sample data into appointments table
INSERT INTO appointments VALUES
('101', 'Olivia', '2026-04-21', '13:30', 'Attended'),
('102', 'David', '2026-04-21', '10:30', 'Missed');

-- View data in all tables
SELECT * FROM appointments;
SELECT * FROM attended_patients;
SELECT * FROM missed_patients;

------------------------------------------------------------

-- Backfill: include old data into attended and missed tables (one-time operation)
TRUNCATE TABLE attended_patients;   -- Clear existing data
TRUNCATE TABLE missed_patients;

-- Insert all attended records from appointments table
INSERT INTO attended_patients
SELECT *
FROM appointments
WHERE status = 'Attended';

-- Insert all missed records from appointments table
INSERT INTO missed_patients
SELECT *
FROM appointments
WHERE status = 'Missed';

------------------------------------------------------------

-- Insert additional test data to validate automation
INSERT INTO appointments VALUES
('103', 'Ruth', '2026-04-21', '9:00', 'Attended'),
('104', 'James', '2026-04-21', '15:00', 'Missed'),
('105', 'Benji', '2026-04-21', '14:30', 'Attended'),
('106', 'Joe', '2026-04-22', '8:30', 'Missed');

-- Check updated tables
SELECT * FROM appointments;
SELECT * FROM attended_patients;
SELECT * FROM missed_patients;

------------------------------------------------------------

-- Insert more records to test real-time pipeline
INSERT INTO appointments VALUES
('107', 'Roger', '2026-04-22', '11:00', 'Attended'),
('108', 'Precious', '2026-04-22', '12:00', 'Missed');
