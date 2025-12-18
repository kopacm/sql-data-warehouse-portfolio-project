# SQL Data warehouse portfolio project 

With this project I built data warehouse with SQL Server, including ETL processes, data modeling

My approach:
- I watched YouTube tutorials from the channel Data with Baraa and took notes.
- I practiced on my own to gain not only theoretical knowledge but also practical experience.
- While I practiced I created a reusable template to support future projects.

![Overview](/pics/Data_warehouse_overview.png)

### 🔵 Requirements Analysis

#### Project Requirements -  Building the Data Warehouse (Data Engineering)

**Objective**

Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

**Specifications**

- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.

- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.

- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.

- **Scope**: Focus on the latest dataset only; historization of data is not required.

- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

## My Approach

###  Design Data Architecture

First I needed to make sure how I will approach the Data Managment. I decided for Data Warehouse - Medallion Architecture.
I created definition for each layer and all lower mentioned parameters. 
<table>
  <tr>
    <td>
       <img src="/pics/Data_Management_Approach.png" alt="Data_Management_Approach" width="400"/>
    </td>
    <td>
      <img src="/pics/Design_layers.png" alt="Data_layers" width="400" />
    </td>
  </tr>
</table>


###  Project Initialization 
I created Detailed Project Tasks in my Obsidian vault. 

For purpose of establishing consistency in all objects (schemas, tables, columns, folders, stored procedures). I used this [Naming Conventions](/docs/Naming_Conventions_Guide.pdf) and I chose **Snake_case** code writting style.

### Build Bronze Layer
### Build Silver Layer
### Build Gold Layer

## 🟢 Protocols for each layer
<table>
  <tr>
    <td align="center">
      <img src="/pics/bronze_layer.png" alt="bronze" width="400"/><br/>
      <a href="/docs/Protocol–Bronze_Layer_Build_Database.pdf">Extended Bronze layer protocol</a>
    </td>
    <td align="center">
      <img src="/pics/silver_layer.png" alt="silver" width="400" /><br/>
      <a href="/docs/Protocol–Silver_Layer_Build_Database.pdf">Extended Silver layer protocol</a>
    </td>
  </tr>
</table>


