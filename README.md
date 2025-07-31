# 🏥💰🔷 Healthcare AR management using Azure Databricks
The Project `Healthcare AR management using Azure Databricks` aims for good cash flow in the Healthcare sector, by contributing to AR management with valuable data, which can be used to take better actions

## 📄Project Description:

In the Healthcare, **Account Receivable** (amount owed by patients or insurance company) should be managed properly.Because of the modern Low-payment Insurance policy, there are high deductibles and copay(amount paid by the patient) which leads to delay in payment or sometimes most likely not pay the amount, that directly affects the Heathcare organization. 

To avoid this, to have a good cash flow and to reduce the collection period, Account Receivable Management should be Implemented in the healthcare Organizations. For the effective AR management, it needs valuable data. This project focus to provide the quality and valueble data to the `AR Management team`. 

Thus,
- Collecting the data from the different sources
- By following the medallion Architecture, Cleaning & Enriching the data and implementing the **Common Data Model**  
- Finally, loading the data to the destination(`Datawarehouse`).

On top of this, we can perform analysis and get insights like patient with long due,patients who's not likely to pay based on the patterns. By understanding these things we can take actions like

- Appointing staff for:
   - Frequent follow-ups 🔁
   - Reminders ⏰
   - To guide and create awareness among patients 🧑‍⚕️📞

- Build models🤖 etc...

These measures will maintain a good cash flow in the organization, so that they invest in things that needed.

---

## 🧰 Tech Stack used

- **🔷 Azure Databricks** – for transformation
- **🔗 Azure Data Factory (ADF)** – for building pipelines and orchestration 
- **☁️ Azure Data Lake Storage** – Used for storage purpose 
- **🐙 GitHub** – Integrated with ADF for version control, collaboration, and as a source location
- **🤖 AskYourDatabase** - for conversational querying
- **🛢️ Azure SQL DB** - for storing source data

---
## Getting started 

Steps to intiate the project

### 🚀 Step 1: Create the required accounts
- 🌐 Create and Azure account and the following services:
   - Azure Cloud – for storage (Storage Account)
   - Azure Databricks
   - Azure Data Factory
   - Azure SQL DB (2)
   - Azure KeyVault

- Create an AskYourDatabase account – for text-to-SQL querying on top of Snowflake
<img src="ScreenShots/resouce_grp&services_ss.png" alt="services" width="500">





