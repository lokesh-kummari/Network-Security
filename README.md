# Network Security Data Science Project

## Project Structure Setup with Environment

This project is an end-to-end **data science project focused on a cybersecurity problem**. The setup involves creating a structured folder, environment, and essential files.

---

### Creating the Project Folder
- Create a folder named network_security inside the Udemy directory on the E drive.
- Open the folder in **VSCode** using:

---

### Environment Setup
- Deactivate any active environment:

- Create a new environment:

- Activate the environment:

---

### Essential Project Files
- **requirements.txt** → List of required libraries.
- **.gitignore** → Exclude unnecessary files (e.g., venv/).
- **README.md** → Project documentation.
- **.env** → Environment variables (API keys, credentials).

---

### GitHub Workflow Setup

---

### Project Structure

---

### Installing Dependencies
- Add libraries in requirements.txt (example):

- Install them:

---

### Packaging the Project (setup.py)

---

### Logging Setup

---

### Exception Handling

---

## Key Takeaways
- Modular project structure for **cybersecurity data science** workflows.
- Proper **environment setup** with Anaconda & VSCode integration.
- Essential files for **project management & security** (requirements.txt, .gitignore, .env, setup.py).
- **GitHub workflows** & **Dockerfile** for deployment.
- Integrated **logging & exception handling** for debugging and reliability.

---

# Introduction to ETL Pipelines & MongoDB Atlas Setup

## Introduction to Project Architecture
The project follows a modular architecture with the following flow:
- **MongoDB Database** → Data Ingestion → Data Validation → Data Transformation → Model Training → Model Evaluation → Deployment to Cloud (AWS/Azure/etc.)

Before data ingestion, data must enter MongoDB via an **ETL pipeline**.

---

## What is an ETL Pipeline?
**ETL** = **Extract, Transform, Load**  
- **Extract**: Retrieve data from the source (e.g., CSV, API, S3, database).  
- **Transform**: Clean, preprocess, and convert the data (e.g., convert CSV → JSON).  
- **Load**: Store the transformed data into the destination (e.g., MongoDB).  

In this project:
- **Source** → Local CSV file (fishing_data.csv)  
- **Transformation** → Cleaning & conversion to JSON  
- **Destination** → MongoDB Atlas database  

---

## Importance of ETL Pipelines
- Real-world projects use multiple data sources: APIs, databases, S3 buckets.  
- ETL pipelines combine & transform heterogeneous data into a unified format.  
- Data engineers typically build ETL pipelines, but **data scientists should understand them**.

---

## Setting Up MongoDB Atlas
1. Create a free MongoDB Atlas account.  
2. Deploy a **free M0 cluster** (default name: Cluster0).  
3. Create a **database user** (username/password).  
4. Install pymongo and add to requirements.txt.  
5. Test connection using Python:

---

## Securing Credentials
- Use a .env file to store sensitive information:  

- Load it in Python using:

---

## ETL Pipeline with Python

### CSV to JSON Conversion

### Insert into MongoDB

### Running the ETL Pipeline

---

## Verification
- Successfully inserted **11,055 records** into MongoDB.  
- Verified via MongoDB Atlas cluster.  
- Committed changes to Git:  

---

## Key Takeaways
- Implemented ETL pipeline (**CSV → JSON → MongoDB**).  
- Used .env for secure connection strings.  
- Leveraged **exception handling & logging** for robust execution.  
- Verified & version-controlled pipeline using **GitHub**.  

---

# Data Ingestion Architecture

## Introduction
Previously, we created a basic ETL pipeline and loaded data into **MongoDB Atlas**.  
The next step is implementing the **Data Ingestion Component**, which prepares data for model training.

---

## Overview of Data Ingestion Component
The data ingestion module consists of three main parts:
1. **Data Ingestion Config** → Stores file paths & split settings  
2. **Data Ingestion Component** → Reads data from MongoDB & processes it  
3. **Data Ingestion Artifact** → Stores raw, train, and test datasets  

---

## Data Ingestion Workflow
1. **Read data from MongoDB**  
2. **Save raw data** → raw.csv in feature store  
3. **Split data** → Train/Test sets  
4. **Save train/test files** → Stored in ingested folder  

---

## Data Ingestion Config
Defines where data is stored and how it’s split:
- Data ingestion directory  
- Feature store path  
- Train file path  
- Test file path  
- Train-test split ratio  
- MongoDB collection & database name  

Example configuration:

---

## Data Ingestion Component
Handles reading from MongoDB, storing feature data, and splitting train/test sets.

### Export collection as DataFrame

### Save raw data

### Train-Test Split

---

## Data Ingestion Artifact
Encapsulates the output of ingestion (train & test file paths):

---

## Orchestration – Initiating Data Ingestion

---

## Testing the Ingestion Pipeline

✅ This will generate:
- raw.csv → Complete dataset  
- train.csv → Training dataset  
- test.csv → Testing dataset  

---

## Key Takeaways
- Data ingestion fetches data from **MongoDB Atlas**.  
- Configuration ensures all paths & settings are modular.  
- Feature store & ingested folders maintain raw, train & test datasets.  
- Artifacts make the ingestion **reproducible & scalable**.  
- The next step is **Data Validation Component** implementation.  

---

# Data Ingestion Architecture

## Introduction
Building on the ETL and MongoDB Atlas setup, this section describes the **Data Ingestion** layer that reads data from MongoDB, persists a raw snapshot, and produces train/test splits for downstream modeling.

## Overview
Data Ingestion has three parts:
1. **Data Ingestion Config** – where and how to save raw/train/test files.
2. **Data Ingestion Component** – logic that reads from MongoDB and writes CSVs.
3. **Data Ingestion Artifact** – return object with paths to outputs for the next pipeline stages.

**High-level flow:**
- Read all records from MongoDB (database: e.g., crochet, collection: e.g., network_data).
- Drop unnecessary columns (e.g., Mongo _id), normalize missing values.
- Write a **raw snapshot** (e.g., raw.csv) into a **feature store** folder.
- Split into **train.csv** and **test.csv** and write into an **ingested** folder.

---

## Configuration Entities & Constants

Create entity/config_entity.py for pipeline-wide configs and constant/training_pipeline/__init__.py for constants.

**Examples of constants** (in network_security/constant/training_pipeline/__init__.py):

**Training pipeline config** (in entity/config_entity.py):

**Data ingestion config** (in entity/config_entity.py):

---

## Data Ingestion Component

Create components/data_ingestion.py with the logic to export data from MongoDB, write the raw snapshot, and produce train/test splits.

**Example runner (main.py)**

---

## Notes & Good Practices
- Add **.env** to .gitignore and never commit secrets.
- For development you may whitelist 0.0.0.0/0 in MongoDB Atlas **Network Access**; for production, lock this down to known IPs/VPC peering.
- Prefer **timestamps** in artifact directories to keep runs isolated and reproducible.
- Log at each step (export, snapshot, split) to simplify debugging.

---

## Key Takeaways
- **Config-driven** ingestion ensures reproducibility and simple reconfiguration.
- A clean **artifact structure** (raw → ingested/train,test) standardizes downstream pipelines.
- The **DataIngestionArtifact** provides a clear contract to subsequent stages (validation, transformation, training).

---

# Data Validation Module

## Overview
The **Data Validation** component ensures the data used in model training and evaluation maintains integrity and consistency.  
It verifies schema consistency, checks numerical columns, and detects **data drift** between training and test datasets.

### Key Steps
1. **Schema Validation**
   - Columns and data types must match the expected schema defined in schema.yaml.
   - Ensures numerical columns are present.
2. **Data Drift Detection**
   - Compares distributions of train vs test datasets using **Kolmogorov-Smirnov (ks_2samp)** test.
   - Generates a report.yaml summarizing drift per feature.
3. **Artifacts**
   - Validated train and test datasets.
   - Drift report path.

### Data Validation Config
Defines paths for:
- Data validation directory
- Valid/invalid data directories
- Drift report file path

### Data Validation Artifact
Holds output values:
- validation_status
- valid_train_file_path
- valid_test_file_path
- drift_report_file_path

### Example Utility (read_yaml_file)

---

# Data Validation Implementation

---

# Data Transformation Module

## Overview
The **Data Transformation** component prepares validated data for modeling. It reads train/test datasets, applies preprocessing, and saves transformed arrays + pipeline objects.

### Process
1. Read validated train & test datasets.
2. Split features (X) and target (y) using the target column.
3. Apply preprocessing pipeline (e.g., **KNN Imputer**, scaling).
4. Fit pipeline on train, transform both train & test.
5. Save transformed arrays and preprocessing pipeline.

### Configuration
Paths include:
- Data transformation directory inside artifacts/
- Transformed train/test arrays (.npy)
- Preprocessing pipeline pickle file

### Utility Functions

### Example Transformation Pipeline

### Output Artifacts
- train.npy and test.npy – transformed feature + target arrays
- preprocessor.pkl – saved preprocessing pipeline

---

## Key Takeaways
- **Data Validation** ensures schema compliance and checks for data drift.  
- **Data Transformation** prepares the datasets with imputation, scaling, and preprocessing pipelines.  
- Both modules generate reusable **artifacts** stored under the artifacts/ directory for downstream modeling.

---

# Data Validation — Part 1

## Overview
After **Data Ingestion**, the **Data Validation** module verifies schema consistency and checks for **data drift** between train and test (and future incoming) datasets.

### Inputs & Outputs
- **Input**: DataIngestionArtifact (paths to train.csv, test.csv, plus raw snapshot).
- **Output**: DataValidationArtifact (validation status, validated file paths, and a drift report path).

### Key Checks
- Schema matches expected columns and dtypes.
- Numerical columns exist.
- Consistent number of columns in train/test.
- Data drift detection between base (train) and current (test/new) data.

---

## Configuration & Directories
Create a **Data Validation Config** with directories like:
- data_validation_dir/
- valid/ and invalid/ subfolders
- drift_report/ with report.yaml

Example (constants in network_security/constant/training_pipeline/__init__.py):

**Config entity** (in entity/config_entity.py):

---

## Schema Definition (YAML)
Create data_schema/schema.yaml:

Utility to read YAML (network_security/utils/utils.py):

---

## Data Validation Component (Part 1)
components/data_validation.py:

---

# Data Validation — Part 2

## Drift Detection & Artifact Generation
Extend DataValidation with drift detection using **Kolmogorov–Smirnov** test:

Example usage:

---

# Data Transformation Architecture

## Overview
The **Data Transformation** module consumes validated datasets and outputs **transformed arrays** plus a **preprocessing pipeline** for consistent inference.

### Steps
1. Read validated train/test from DataValidationArtifact.
2. Split into **X (features)** and **y (target)** using TARGET_COLUMN from constants.
3. Build a preprocessing pipeline (e.g., **KNNImputer**, **RobustScaler**).
4. fit on train features; transform train and test (avoid leakage).
5. Persist transformed arrays (.npy) and the fitted pipeline (.pkl).

## Config & Artifacts
Constants (examples):

Config entity:

Artifacts & utils:

Component (components/data_transformation.py):

## Key Points
- Use fit on train only; transform on test to **avoid leakage**.
- Keep the preprocessing pipeline versioned—save with artifacts for reproducibility.
- Add class balancing (e.g., **SMOTE Tomek**) if needed later.

---

# Data Transformation Implementation

## Class Definition

## Method: initiate_data_transformation

## Method: get_data_transformer_object

**Explanation**
- Drops target column, isolates features and target.
- Converts target values (-1 → 0).
- Uses **KNNImputer** pipeline for missing values.
- Saves numpy arrays and preprocessor pickle.
- Returns a DataTransformationArtifact.

---

# Model Trainer — Part 1

## Overview
The **Model Trainer** consumes transformed data and trains multiple models, selecting the best.

**Config includes**:
- Trainer directory
- Trained model file path
- Expected accuracy (e.g., 0.6)
- Threshold for model difference (e.g., 0.05)

**Artifacts include**:
- ClassificationMetricArtifact (F1, precision, recall)
- ModelTrainerArtifact (trained model path, metrics)

**Utilities**:
- save_object, load_object
- load_numpy_array_data

**Classification Metrics Utility** (ml_utils/metric.py):

---

# Model Trainer & Evaluation with Hyperparameter Tuning

## Training Flow
- Load arrays → split into features and targets.
- Initialize models: RandomForest, DecisionTree, GradientBoosting, LogisticRegression, AdaBoost.
- Define parameter grids for each model.
- Use **GridSearchCV** for hyperparameter tuning.

## Evaluate Models Function

## Model Selection
- Pick model with **highest test score**.
- Compute classification metrics for train/test with get_classification_score.
- Wrap preprocessor + model into NetworkModel class for easy prediction.
- Save as pickle.

---

# MLflow Experiment Tracking

## Why MLflow?
- Track metrics (F1, precision, recall).
- Log models for reuse.
- Compare multiple runs.

## Function Example

## Running & Viewing
- Run pipeline: python main.py
- Start UI: mlflow ui
- View experiments at http://127.0.0.1:5000

**Best Practices**
- Exclude mlruns/ from Git via .gitignore.
- Use consistent naming for experiments.

---

## Key Takeaways
- Data Transformation implemented with **KNN Imputer** and artifact saving.
- Model Trainer selects best model via **GridSearchCV** hyperparameter tuning.
- Metrics (F1, precision, recall) logged for both train/test.
- **MLflow** integrated for experiment tracking, visualization, and reproducibility.

---

# Data Transformation — Implementation

## Overview
Implements the full transformation pipeline: read validated data → split features/target → impute missing values with **KNNImputer** → persist transformed arrays and the preprocessor object.

**What it does**
- Reads validated train/test CSVs.
- Drops TARGET_COLUMN from features.
- Maps target -1 → 0 for binary classification.
- Applies **KNNImputer** via a scikit-learn Pipeline.
- Saves transformed arrays and the preprocessor .pkl.
- Returns a DataTransformationArtifact with output paths.

---

# Model Trainer — Part 1 (Architecture & Setup)

## Purpose
Consumes **DataTransformationArtifact** and a **ModelTrainerConfig** to train multiple models, evaluate, select the best, and persist it.

### Typical Config Fields
- model_trainer_directory
- train_model_file_path
- expected_accuracy (e.g., 0.60)
- model_config_file_path (optional)
- threshold for minimum improvement

### Artifacts
- ClassificationMetricArtifact: F1, Precision, Recall
- ModelTrainerArtifact: trained model path + train/test metrics

### Utilities
- save_object(obj, path) / load_object(path)
- load_numpy_array_data(path)

### Wrapper for Inference
A NetworkModel(processor, model) class combines preprocessing and prediction.

---

# Model Trainer — Training & Evaluation with Hyperparameter Tuning

## Approach
Use GridSearchCV across several algorithms, evaluate on train & test, and pick the best test performer.

### Models (examples)
- RandomForestClassifier(verbose=1)
- DecisionTreeClassifier
- GradientBoostingClassifier
- LogisticRegression
- AdaBoostClassifier

### Skeleton

### Persist Combined Model

### Artifact
Includes the trained model path and classified metrics (train/test).

---

# Model Experiment Tracking with MLflow

## Why MLflow
Track metrics & models, compare runs, and manage experiment artifacts.

### Install

### Minimal Tracker

### View UI

- Compare runs, view metrics & artifacts.
- **Tip**: add mlruns/ to .gitignore.

---

---

# MLflow Experiment Tracking with Remote Repository (DAGsHub)

## Introduction
Instead of logging MLflow runs locally (mlruns/), you can configure tracking to a remote repo such as **DAGsHub**, which provides experiment versioning + collaboration.

## Setup
1. Create / connect a **GitHub repo** with **DAGsHub** (sign in with GitHub/GitLab/Bitbucket).
2. Install dagshub package:

3. Initialize inside your code:

4. Run training as usual:

- Now metrics and artifacts sync to DAGsHub instead of local mlruns/.

## Viewing Results
- Experiments appear in DAGsHub → *Experiments* section.
- Each run shows metrics (F1, precision, recall), artifacts (pickles), and a link to MLflow UI.
- Shareable URLs make collaboration easy.

## Extra Features
- DAGsHub also integrates with **DVC** for dataset versioning.
- Recommended: add mlruns/ to .gitignore to avoid clutter.

---

# Model Pusher Implementation

## Goal
Consolidate best model + preprocessor pickle into a **final_models/** folder for prediction or deployment.

## Process
- Copy trained model pickle (model.pkl) from model trainer artifacts.
- Copy preprocessing pickle (preprocessor.pkl) from transformation artifacts.
- Save into:

- Optional: extend to upload these files to **S3** or cloud storage.

**Example:**

---

# Training Pipeline Implementation

## Modular Pipeline
Implements a sequential flow: **Ingestion → Validation → Transformation → Training → Pusher**.

### TrainingPipeline Class

### FastAPI Integration
Expose training trigger via API:

### Run

- Open http://localhost:8000/docs to hit /train and trigger full pipeline.

## Key Takeaways
- DAGsHub allows **remote MLflow tracking**.
- Model pusher centralizes final artifacts in final_models/.
- Training pipeline orchestrates all components end-to-end.
- FastAPI provides an interface to trigger training with a REST call.

---

---

# Final Model & Artifacts Pusher to AWS S3

## Why S3?
- Local storage (final_models/) is not scalable for large or versioned models.  
- AWS S3 provides durable, versioned, and shareable storage for **artifacts** + **models**.

## Implementation
Create cloud/s3_sync.py with:

Use inside pipeline:

## Setup
1. Install & configure AWS CLI:

- Provide AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, region, output format.

2. Ensure IAM user has S3 permissions.

3. Run pipeline → artifacts & models sync to bucket.

---

# Building Docker Image & GitHub Actions

## Dockerfile Example

## GitHub Actions Workflow (main.yaml)

Add required GitHub Secrets:
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_REGION
- AWS_ECR_LOGIN_URI
- ECR_REPOSITORY_NAME

---

# Deployment to AWS EC2 Instance

## Steps
1. Launch EC2 instance (t2.medium suggested).  
2. SSH or use EC2 Instance Connect.  
3. Install Docker + setup non-sudo permissions:

4. Configure **GitHub self-hosted runner** on EC2:  

5. Extend workflow with deployment job (runs on self-hosted EC2 runner).

## Deployment Job (YAML)

## Security Group
- Open inbound rule: TCP 8080 (or your app port).  
- Access via http://<EC2_PUBLIC_IP>:8080.

---

# Key Takeaways
- **S3** stores artifacts & final models securely with versioning.  
- **Docker + ECR** provide a portable container image.  
- **GitHub Actions** automates CI/CD.  
- **EC2** self-hosted runner enables continuous deployment.  
- Full pipeline: Local dev → GitHub Actions → ECR → EC2 Deployment.  

---

---

# Final Model & Artifacts Pusher to AWS S3 Buckets

## Why push to S3?
Local storage for large models/artifacts doesn’t scale. **Amazon S3** gives durable, cheap, versioned storage and makes downstream deployment simpler.

## High-level
- **sync_artifact_directory_to_s3** → uploads the entire artifacts/ tree
- **sync_final_model_directory_to_s3** → uploads final_models/ (e.g., model.pkl, preprocessing.pkl)

## Use AWS CLI (simple & robust)
Install and configure **AWS CLI**, then sync folders:

## S3Sync helper (cloud/s3_sync.py)

### Integrate in pipeline

### AWS prerequisites
- IAM user with S3 permissions (e.g., s3:PutObject, s3:ListBucket).
- aws configure set on the machine/runner.

---

# Building Docker Image & GitHub Actions (CI overview)

## Dockerfile (example)

## Basic CI workflow (/.github/workflows/main.yaml)

---

# GitHub Action — Docker Image Push to AWS ECR (CD to registry)

## Create ECR repository
- AWS Console → **ECR** → *Create repository* (private) → e.g., network-security
- Note **repository URI** (e.g., 123456789012.dkr.ecr.us-east-1.amazonaws.com)

## Add secrets in GitHub → Settings → Secrets and variables → Actions
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_REGION (e.g., us-east-1)
- AWS_ECR_LOGIN_URI (e.g., 123456789012.dkr.ecr.us-east-1.amazonaws.com)
- ECR_REPOSITORY_NAME (e.g., network-security)

## Extend workflow

---

# Final Deployment to AWS EC2 (CD — run container)

## EC2 prep
- Launch Ubuntu instance (e.g., t2.medium), enable inbound **TCP 8080**
- Install Docker & set permissions:

## Self-hosted GitHub Runner on EC2
- Repo → **Settings** → **Runners** → *New self-hosted runner* (Linux)
- Follow the generated commands on EC2:

## Deployment job (runs-on: self-hosted)

### Verify
- Open http://<EC2_PUBLIC_IP>:8080 (ensure security group allows 8080)
- Swagger at /docs if using FastAPI example

---

## Key Takeaways
- **S3 sync** keeps your artifacts/ and final_models/ durable & versioned.
- **ECR** stores Docker images privately inside AWS.
- **EC2 + self-hosted runner** enables full CD: pull image and run container on pushes.
- Keep credentials in **GitHub Secrets**; never commit them.
