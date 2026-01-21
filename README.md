# Financial Transaction Analysis and Fraud Detection

## Project Overview
This project analyzes comprehensive banking data to derive insights into customer behavior, financial risk, and fraudulent activities. By integrating customer profiles, card specifications, and transaction history, we aim to visualize the financial ecosystem and build predictive models for credit scoring and fraud detection.

The analysis focuses on processing raw transaction data to support strategic decision-making regarding risk management and customer segmentation.

## Dataset
The dataset consists of integrated banking records, including:
* **Customer Data:** Demographics, income levels, and credit scores.
* **Card Information:** Card types, limits, and expiration details.
* **Transaction History:** Timestamps, merchant locations, amounts, and transaction categories.

*Source: Kaggle - Transactions Fraud Datasets*

## Key Analysis Areas

### 1. Customer Segmentation and Profiling
We applied RFM (Recency, Frequency, Monetary) analysis to categorize customers based on their interaction with the bank. Key metrics include:
* **Age & Spending:** Relationship between age groups and specific spending categories.
* **Income Distribution:** Analysis of spending power across different income groups.
* **Customer Loyalty:** Evaluation of active engagement duration per segment.

### 2. Financial Risk and Credit Management
This section evaluates the financial health of the customer base:
* **Limit Utilization:** Analysis of credit limit usage rates across different segments.
* **Debt-to-Income Ratio:** Monitoring changes in financial leverage and borrowing capacity.
* **Error Analysis:** Correlating transaction errors with specific card types or spending behaviors.

### 3. Card Usage Efficiency
This section focuses on the operational security and lifecycle of customer cards:
* **Physical Card Security:** Analyzed the recency of PIN code changes to identify high-risk customers and potential security vulnerabilities.
* **Card Renewal Lifecycle:** Evaluated customer engagement and renewal activities relative to card expiration dates to predict churn or inactivity.

### 4. Geographic and Logistic Insights
* **Local vs. Global Spending:** Comparison of registered residence addresses versus transaction locations.
* **E-Commerce Adoption:** Tracking the rate of digital transformation among customers.
* **Sectoral Heatmaps:** Identifying dominant spending sectors across different cities.

## Machine Learning Models

### Personalized Credit Score Predictor
We developed a regression model to estimate credit scores based on financial indicators.
* **Inputs:** Annual income, total debt, per capita income, and number of active cards.
* **Objective:** Determine key parameters influencing creditworthiness and identify thresholds where credit scores decline.

### Fraud Detection System
A classification approach to identify suspicious activity based on anomaly detection rules:
* **Transaction Speed:** Detecting physically impossible transactions (e.g., transactions in different cities within a short timeframe).
* **Location Analysis:** Flagging transactions significantly distant from the user's registered address.
* **Limit Anomalies:** High-value transactions exceeding normal usage patterns.
* **Temporal Anomalies:** Identification of unusual spending times (e.g., high-volume transactions at 03:00 AM).

## Tech Stack
The project utilizes a modern data stack for end-to-end processing:

* **Google Colab (Python):** Used as the primary Cloud IDE for data analysis, data cleaning, and training machine learning models.
* **Google BigQuery:** Serves as the Data Warehouse for storing and querying large-scale transaction data.
* **dbt (Data Build Tool):** Applied for data transformation, testing, and documentation within the warehouse.
* **Microsoft Power BI:** Used to build interactive dashboards and visualize KPIs for business intelligence.
* **GitHub:** Version control and project collaboration.

## Team
* Ahsen Toker
* Dicle Dodurga
* Elif Çal
* Mehmet Çelik
