# Climate-Data-Analysis-and-Prediciton-System

# Kenya County-Level Rainfall Onset Advisory Dashboard
## SDS 2412 — Analysis of Large Datasets
## GROUP TWO

### Project Overview
The Kenya Seasonal Rainfall Onset Advisory Dashboard is a cloud‑native, large‑scale data system that ingests 60+ years of historical climate observations and real‑time forecast streams to deliver county‑level planting advisories for rain‑fed agriculture. Built on Google Cloud Platform using a Lambda Architecture (Apache Spark batch processing and OpenMeteo/Pub/Sub streaming), the system applies machine learning to generate daily onset probability scores. The final output is an interactive web dashboard that visualizes county risk maps and historical onset trends, enabling agricultural officers and farmers to make timely, climate‑smart planting decisions.

### Team
| Role | Name | Responsibility |
|------|------|----------------|
| R01  | [Dennis Gitau] | Data & Infrastructure Lead [RO1] |
| R02  | [Ashley Otieno] | Distributed Processing Engineer [R02] |
| R03  | [Alexander Kihoi] | Streaming & Real-Time Engineer [R03] |
| R04  | [Eric Mugo] | ML & Analytics Engineer [R04] |
| R05  | [Faith Gichuru] | DevOps, Deployment & Reporting Lead [R05] |


## USE BRANCHES TO ENSURE WE DO NOT OVERRIDE/OVERWRITE EACH OTHERS WORK
main branch - Role5 only (Faith)
feature/r01-ingest - Role1 only (Gitau)
feature/r02-scalability -Role2 only (Ash)
feature/r03-openmeteo -Role 3 only (Alex)
feature/r05-setup -Role5