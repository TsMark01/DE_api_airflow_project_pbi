# DE API Airflow Project with Power BI Integration

## 📋 Project Overview

This is my first **Data Engineering (DE)** project, focused on building an ETL (Extract, Transform, Load) pipeline. It fetches data from a non-public medical information system API, processes it using **Apache Airflow** for task orchestration, stores it in a database, and visualizes insights in **Power BI**. The project demonstrates core DE skills like workflow automation, data processing, and reporting while ensuring no sensitive data is exposed.

### 🎯 Key Objectives
- Extract data from a secure, non-public medical information system API.
- Orchestrate ETL tasks using Airflow DAGs with error handling and scheduling.
- Store processed data securely in a database.
- Create visualizations in Power BI for data insights.

## 🛠️ Tech Stack

| Category          | Tools/Technologies       | Purpose |
|-------------------|--------------------------|---------|
| **Orchestration** | Apache Airflow          | Workflow scheduling |
| **Data Extraction**| Python (Requests)       | Fetch API data |
| **Transformation**| Python (Pandas)         | Data cleaning |
| **Storage**       | PostgreSQL              | Data storage |
| **Visualization** | Power BI                | Dashboards |
| **Version Control**| Git/GitHub             | Code management |

## 🏗️ Architecture

1. **Extract**: Pull data from a non-public medical information system API.
2. **Transform**: Clean and format data using Pandas, ensuring compliance with data sensitivity requirements.
3. **Load**: Store data in PostgreSQL with appropriate security measures.
4. **Orchestrate**: Airflow schedules and manages tasks.
5. **Visualize**: Power BI creates dashboards from the database for insights.

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- PostgreSQL
- Apache Airflow
- Power BI Desktop
- Access credentials for the medical information system API (not included in the repository)

### Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone https://github.com/TsMark01/DE_api_airflow_project_pbi.git
   cd DE_api_airflow_project_pbi
   ```

2. **Set Up Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Installing Airflow on Linux**
   Follow these steps to install and configure Apache Airflow on Linux. Ensure you have administrative privileges (use `sudo` where necessary).

   #### Uninstall Previous Airflow (if installed)
   ```bash
   pip uninstall apache-airflow -y
   pip freeze | grep apache-airflow | xargs pip uninstall -y
   rm -rf ~/airflow
   sudo systemctl stop airflow-scheduler
   sudo systemctl stop airflow-webserver
   sudo systemctl disable airflow-scheduler
   sudo systemctl disable airflow-webserver
   sudo rm /etc/systemd/system/airflow-*.service
   sudo rm -rf /etc/airflow
   ```

   #### Create Airflow Directory in Root
   ```bash
   cd /
   mkdir airflow
   chmod 777 airflow
   ```

   #### Check Airflow Config
   ```bash
   airflow config list
   ```

   #### Set AIRFLOW_HOME Environment Variable
   **Important:** Set this variable before proceeding and verify it at each step.
   ```bash
   export AIRFLOW_HOME=/airflow
   printenv | grep AIRFLOW_HOME  # Verify
   ```

   #### Edit Airflow Configuration
   Edit the Airflow config file (usually at `$AIRFLOW_HOME/airflow.cfg` after init) to set:
   ```
   executor = LocalExecutor
   sql_alchemy_conn = postgresql+psycopg2://airflowuser:password@localhost/airflow_metadata
   ```
   Replace `airflowuser:password` with your actual PostgreSQL credentials.

   #### Install Airflow and Dependencies
   ```bash
   pip3 install apache-airflow[postgresql,kubernetes]==2.7.3
   pip3 install psycopg2-binary
   pip3 install Flask-Session==0.5.0
   ```

   #### Create Required Folders and Set Permissions
   ```bash
   export AIRFLOW_HOME=/airflow  # Verify with printenv
   mkdir dags
   mkdir plugins
   mkdir scripts
   chmod 777 -R dags
   chmod 777 -R plugins
   chmod 777 -R scripts
   ```

   #### Initialize Database and Create User
   ```bash
   airflow db init
   airflow users create --username AirflowAdmin --firstname name1 --lastname name2 --role Admin --email airflow@airflow.com --password qwerty
   ```

   #### Set Up PostgreSQL Database
   ```bash
   sudo -u postgres psql
   ```
   Inside psql:
   ```
   CREATE DATABASE airflow_metadata;
   CREATE USER airflow1 WITH PASSWORD 'qwerty12345';
   GRANT ALL PRIVILEGES ON DATABASE airflow_metadata TO airflow1;
   \q
   ```

   #### Start Airflow Services
   ```bash
   airflow scheduler &
   airflow webserver &
   ```

   #### Check and Kill Conflicting Ports (if needed)
   If port 8793 or 8080 is in use:
   ```bash
   sudo netstat -tulnp | grep 8793
   sudo kill <PID>
   sudo kill -9 <PID>  # If necessary
   ```

   Access the Airflow UI at `http://<your-ip>:8080`.

4. **Configure API Access**
   - Securely store API credentials (e.g., in environment variables or a secrets manager). No credentials are included in this repository for security.
   - Update the DAG to include API authentication parameters specific to the medical information system.

5. **Run the Pipeline**
   - Start Airflow and trigger the DAG via the UI (http://localhost:8080).

6. **Power BI**
   - Connect Power BI to PostgreSQL and load the dashboard, ensuring compliance with data privacy regulations.

## 📝 License

This project is licensed under the [MIT License](LICENSE). The repository contains no sensitive data, API keys, or passwords, ensuring secure sharing of the code. The non-public medical information system API requires authorized access, which is not included in this project.