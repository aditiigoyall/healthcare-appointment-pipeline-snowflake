# 🏥 Healthcare Appointment Automation Pipeline using Snowflake

## 📌 Overview

This project implements an automated healthcare appointment tracking system using Snowflake. It eliminates manual effort by automatically classifying patient appointments into attended and missed categories in near real-time.

---

## 🎯 Problem Statement

Healthcare systems often rely on manual tracking of patient appointments, leading to:

* Inefficiencies
* Human errors
* Lack of real-time visibility

---

## 💡 Solution

This project builds a data pipeline in Snowflake that:

* Ingests appointment data
* Tracks changes using Streams
* Automates classification using Tasks
* Updates attended and missed patient records in real time

---

## 🏗️ Architecture

```
CSV Data → Snowflake Table (appointments)
         → Streams (change tracking)
         → Tasks (automation)
         → Output Tables:
             ├── attended_patients
             └── missed_patients
```

---

## ⚙️ Technologies Used

* Snowflake
* SQL
* Snowflake Streams
* Snowflake Tasks

---

## 🚀 Key Features

* ✅ Automated data pipeline using Snowflake
* ✅ Real-time classification of patient appointments
* ✅ Stream-based change tracking
* ✅ Task-based scheduling (runs every minute)
* ✅ Duplicate handling using MERGE logic

---

## 📂 Project Structure

```
healthcare-appointment-pipeline-snowflake/
│
├── setup.sql        # Database, schema, tables, stage
├── pipeline.sql     # Streams, tasks, automation logic
├── README.md        # Project documentation
```

---

## 🔄 Workflow

1. Upload CSV data to Snowflake internal stage
2. Load data into `appointments` table using `COPY INTO`
3. Streams capture incremental changes in the table
4. Tasks run automatically at scheduled intervals
5. Data is processed and classified into:

   * `attended_patients`
   * `missed_patients`


---

## 🧠 Key Learnings

* Implemented real-time data pipelines using Snowflake
* Understood stream consumption behavior
* Designed automation using scheduled tasks
* Applied MERGE for deduplication and data integrity

---

## 📈 Future Enhancements

* Add analytics layer for trend analysis
* Integrate dashboard (Power BI/Tableau)
* Implement ML model to predict no-shows


Feel free to connect for collaboration or feedback!
