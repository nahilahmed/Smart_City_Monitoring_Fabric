
# Smart City Monitoring with Microsoft Fabric

Welcome to the Smart City Monitoring repository! This project leverages Microsoft Fabric to provide real-time analytics for urban infrastructure monitoring, focusing on waste management and water quality management as initial use cases.

## Use Cases

### 1. Waste Management
The waste management solution aims to optimize waste collection routes, monitor service levels, and support long-term urban planning. This is achieved by leveraging IoT sensors in waste bins and collection trucks, with data stored in Kusto DB. The analytics provide actionable insights for city administrators, waste operations analysts, and urban planners.

#### Architecture Overview

**Data Pipeline:**
- **Data Sources**: IoT sensors in waste bins and trucks, along with city reference data (e.g., zoning and weather).
- **Ingestion**: Data flows through pipelines and dataflows to Kusto DB for storage.
- **Storage**: Real-time data is stored in **Kusto DB**, enabling efficient querying and analytics.
- **Exposure & Consumption**:
  - **Power BI**: Offers dashboards for city administrators to monitor collection status and service levels.
  - **Kusto Query**: Provides detailed query access for waste operations analysts to optimize collection routes and schedules.
  - **Notebooks**: Used by urban planners for predictive analysis, helping forecast future waste management needs.

### 2. Water Quality Management 
The water quality management use case aims to provide real-time monitoring of water quality indicators across the city, ensuring regulatory compliance and public safety. This module will use a similar architecture, with data ingestion, storage, and exposure via Kusto DB, Power BI, and notebooks.

## Repository Structure
- **`/src`**: Contains source code for data ingestion, transformations, and analytics.
- **`/notebooks`**: Jupyter Notebooks used for advanced analytics and predictive modeling.
- **`/dashboards`**: Power BI files and configurations for data visualization.
- **`/docs`**: Documentation related to the project, including setup and user guides.

## Getting Started
1. **Prerequisites**:
   - Access to Microsoft Fabric workspace.
   - A GitHub Personal Access Token (classic) to connect to the Fabric workspace repository.

2. **Setup**:
   - Clone this repository.
   - Follow the instructions in `/docs/setup.md` to set up your local environment.
   - Use the `.env.template` file for required environment variables.

3. **Deployment**:
   - After setup, use Microsoft Fabric’s pipelines to automate data ingestion and processing.
   - Deploy Power BI dashboards for waste management and configure Kusto DB for real-time analytics.

## Future Work
- **Enhancements** to the waste management analytics, such as incorporating predictive models for demand forecasting.
- **Expansion** into additional smart city use cases, starting with water quality management.
  
## Contributing
We welcome contributions! Please read the [Contributing Guide](/docs/CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
