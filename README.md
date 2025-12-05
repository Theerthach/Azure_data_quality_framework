# Enterprise Data Quality Check Framework  
### *(Azure ADF · Databricks · Delta Lake · Synapse · Power BI)*

This project demonstrates an end-to-end Data Quality Monitoring Framework using **Azure Data Factory**, **Azure Databricks**, **Delta Lake**, **Synapse Serverless SQL**, and **Power BI**.  
The purpose is to **automate data validation**, **store DQ metrics**, and **enable observability dashboards** for continuous data reliability.

---

## ⭐ High-Level Architecture

### Flow Summary

1. **Source → ADLS Gen2**  
   Raw data is landed in Azure Data Lake.

2. **ADLS → ADF**  
   ADF ingests the file and performs basic assertions (null checks, type checks, row counts).

3. **ADF → ADLS**  
   ADF writes validated/flagged data back to ADLS.

4. **ADLS → Databricks**  
   Databricks notebook runs advanced DQ checks (duplicates, business rules, date validations, etc.).

5. **Databricks → ADLS**  
   Results are stored as a Delta table (`dq_results`).

6. **Synapse Serverless SQL**  
   Synapse reads the Delta table using `OPENROWSET` and exposes an external view.

7. **Power BI Dashboard**  
   Power BI visualizes DQ metrics, trends, and rule-level failures.
<img width="911" height="541" alt="Flow_Diagram drawio" src="https://github.com/user-attachments/assets/93615035-ee0f-4a30-b4b7-108b05843010" />

---

## 🧪 Data Quality Checks

### ✔ Basic Checks (ADF)
- Null validations  
- Data type checks  
- Format validations  
- Row-level error tagging  

### ✔ Advanced Checks (Databricks)
- Duplicate key detection  
- Invalid numeric values  
- Invalid/incorrect dates  
- Out-of-range values  
- Business rule validations  
- Error row counts  
- Timestamped rule-level metrics

## 💡 Power BI Dashboard KPIs

- Overall Data Quality Score  
- Total Failed Checks  
- Duplicate Records  
- Invalid Data Counts  
- Error Row %  
- Trend of DQ Failures  
- Rule-Level Heatmaps  

---

## 🎯 Future Enhancements

- Email/Teams alerts for DQ threshold breaches  
- Azure DevOps CI/CD automation  

---
