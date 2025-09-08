# Network Security Data Science Project

## Project Structure Setup with Environment

This project is an end-to-end **data science project focused on a cybersecurity problem**. The setup involves creating a structured folder, environment, and essential files.

---

### Creating the Project Folder
- Create a folder named `network_security` inside the `Udemy` directory on the E drive.
- Open the folder in **VSCode** using:
```bash
code .
```

---

### Environment Setup
- Deactivate any active environment:
```bash
conda deactivate
```
- Create a new environment:
```bash
conda create -p venv python=3.10
```
- Activate the environment:
```bash
conda activate venv
```

---

### Essential Project Files
- **requirements.txt** → List of required libraries.
- **.gitignore** → Exclude unnecessary files (e.g., `venv/`).
- **README.md** → Project documentation.
- **.env** → Environment variables (API keys, credentials).

---

### GitHub Workflow Setup
```
.github/workflows/
    main.yaml   # GitHub Actions config
```

---

### Project Structure
```
network_data/         # Dataset storage
notebooks/            # Jupyter notebooks
network_security/     # Main project package
    __init__.py
    components/
    constants/
    entity/
    exception/
    logging/
    pipeline/
    utils/
    cloud/
Dockerfile            # Docker image setup
setup.py              # Packaging configuration
```

---

### Installing Dependencies
- Add libraries in `requirements.txt` (example):
```
python-dotenv
pandas
numpy
pymongo
```
- Install them:
```bash
pip install -r requirements.txt
```

---

### Packaging the Project (`setup.py`)
```python
from setuptools import find_packages, setup
from typing import List

def get_requirements() -> List[str]:
    requirements_list = []
    try:
        with open('requirements.txt', 'r') as file:
            lines = file.readlines()
            for line in lines:
                requirement = line.strip()
                if requirement and requirement != '-e .':
                    requirements_list.append(requirement)
    except FileNotFoundError:
        print('requirements.txt file not found.')
    return requirements_list

setup(
    name='network_security',
    version='0.0.1',
    author='Kreshnik',
    author_email='KrishnaX06@gmail.com',
    packages=find_packages(),
    install_requires=get_requirements(),
)
```

---

### Logging Setup
```python
import logging, os
from datetime import datetime

log_file = f"{datetime.now():%m_%d_%Y_%H_%M_%S}.log"
logs_path = os.path.join(os.getcwd(), "logs")
os.makedirs(logs_path, exist_ok=True)
log_file_path = os.path.join(logs_path, log_file)

logging.basicConfig(
    filename=log_file_path,
    format='%(asctime)s - %(lineno)d - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
```

---

### Exception Handling
```python
import sys

class NetworkSecurityException(Exception):
    def __init__(self, error_message, error_detail: sys):
        super().__init__(error_message)
        self.error_message = error_message
        self.error_detail = error_detail
        self.line_number = error_detail.exc_info()[2].tb_lineno
        self.file_name = error_detail.exc_info()[2].tb_frame.f_code.co_filename

    def __str__(self):
        return f"Error occurred in {self.file_name} at line {self.line_number}: {self.error_message}"
```

---

## Key Takeaways
- Modular project structure for **cybersecurity data science** workflows.
- Proper **environment setup** with Anaconda & VSCode integration.
- Essential files for **project management & security** (`requirements.txt`, `.gitignore`, `.env`, `setup.py`).
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
- **Source** → Local CSV file (`fishing_data.csv`)  
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
2. Deploy a **free M0 cluster** (default name: `Cluster0`).  
3. Create a **database user** (username/password).  
4. Install `pymongo` and add to `requirements.txt`.  
5. Test connection using Python:

```python
from pymongo import MongoClient

client = MongoClient("mongodb+srv://username:<password>@cluster0.mongodb.net/?retryWrites=true&w=majority")

try:
    client.admin.command('ping')
    print("Ping your deployment. You're successfully connected to MongoDB.")
except Exception as e:
    print(e)
```

---

## Securing Credentials
- Use a `.env` file to store sensitive information:  
```
MONGODB_URL=mongodb+srv://username:password@cluster0.mongodb.net/?retryWrites=true&w=majority
```

- Load it in Python using:
```python
from dotenv import load_dotenv
import os

load_dotenv()
MONGODB_URL = os.getenv('MONGODB_URL')
```

---

## ETL Pipeline with Python

### CSV to JSON Conversion
```python
import pandas as pd, json

def csv_to_json_converter(file_path):
    data = pd.read_csv(file_path)
    data.reset_index(drop=True, inplace=True)
    records = json.loads(data.T.to_json()).values()
    return list(records)
```

### Insert into MongoDB
```python
import pymongo

def insert_data_mongodb(records, database, collection):
    mongo_client = pymongo.MongoClient(MONGODB_URL)
    db = mongo_client[database]
    col = db[collection]
    col.insert_many(records)
    return len(records)
```

### Running the ETL Pipeline
```python
if __name__ == '__main__':
    file_path = './network_data/fishing_data.csv'
    database = 'crash_i'
    collection = 'network_data'

    records = csv_to_json_converter(file_path)
    inserted = insert_data_mongodb(records, database, collection)
    print(f'Number of records inserted: {inserted}')
```

---

## Verification
- Successfully inserted **11,055 records** into MongoDB.  
- Verified via MongoDB Atlas cluster.  
- Committed changes to Git:  
```bash
git add .
git commit -m "ETL pipeline completed"
git push -u origin main
```

---

## Key Takeaways
- Implemented ETL pipeline (**CSV → JSON → MongoDB**).  
- Used `.env` for secure connection strings.  
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
2. **Save raw data** → `raw.csv` in feature store  
3. **Split data** → Train/Test sets  
4. **Save train/test files** → Stored in `ingested` folder  

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
```python
class DataIngestionConfig:
    def __init__(self, training_pipeline_config):
        self.data_ingestion_dir = os.path.join(
            training_pipeline_config.artifact_dir,
            training_pipeline.DATA_INGESTION_DIR_NAME
        )
        self.feature_store_file_path = os.path.join(
            self.data_ingestion_dir,
            training_pipeline.DATA_INGESTION_FEATURE_STORE_DIR,
            training_pipeline.FILE_NAME
        )
        self.train_file_path = os.path.join(
            self.data_ingestion_dir,
            training_pipeline.DATA_INGESTION_INGESTED_DIR,
            training_pipeline.TRAIN_FILE_NAME
        )
        self.test_file_path = os.path.join(
            self.data_ingestion_dir,
            training_pipeline.DATA_INGESTION_INGESTED_DIR,
            training_pipeline.TEST_FILE_NAME
        )
        self.train_test_split_ratio = training_pipeline.DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO
        self.collection_name = training_pipeline.DATA_INGESTION_COLLECTION_NAME
        self.database_name = training_pipeline.DATA_INGESTION_DATABASE_NAME
```

---

## Data Ingestion Component
Handles reading from MongoDB, storing feature data, and splitting train/test sets.

### Export collection as DataFrame
```python
def export_collection_as_dataframe(self) -> pd.DataFrame:
    mongo_client = pymongo.MongoClient(self.mongo_db_url)
    collection = mongo_client[self.data_ingestion_config.database_name][self.data_ingestion_config.collection_name]
    df = pd.DataFrame(list(collection.find()))
    if '_id' in df.columns:
        df.drop(columns=['_id'], inplace=True)
    df.replace(to_replace='na', value=np.nan, inplace=True)
    return df
```

### Save raw data
```python
def export_data_to_feature_store(self, df: pd.DataFrame) -> pd.DataFrame:
    feature_store_dir = self.data_ingestion_config.feature_store_file_path
    os.makedirs(feature_store_dir, exist_ok=True)
    file_path = os.path.join(feature_store_dir, 'fishing_data.csv')
    df.to_csv(file_path, index=False, header=True)
    return df
```

### Train-Test Split
```python
def split_data_as_train_test(self, df: pd.DataFrame):
    train_set, test_set = train_test_split(
        df,
        test_size=self.data_ingestion_config.train_test_split_ratio
    )
    os.makedirs(os.path.dirname(self.data_ingestion_config.training_file_path), exist_ok=True)
    train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
    test_set.to_csv(self.data_ingestion_config.test_file_path, index=False, header=True)
```

---

## Data Ingestion Artifact
Encapsulates the output of ingestion (train & test file paths):
```python
@dataclass
class DataIngestionArtifact:
    train_file_path: str
    test_file_path: str
```

---

## Orchestration – Initiating Data Ingestion
```python
def initiate_data_ingestion(self) -> DataIngestionArtifact:
    df = self.export_collection_as_dataframe()
    self.export_data_to_feature_store(df)
    self.split_data_as_train_test(df)
    artifact = DataIngestionArtifact(
        train_file_path=self.data_ingestion_config.training_file_path,
        test_file_path=self.data_ingestion_config.test_file_path
    )
    return artifact
```

---

## Testing the Ingestion Pipeline
```python
if __name__ == '__main__':
    data_ingestion_config = DataIngestionConfig()
    data_ingestion = DataIngestion(data_ingestion_config)
    artifact = data_ingestion.initiate_data_ingestion()
    print(artifact)
```

✅ This will generate:
- `raw.csv` → Complete dataset  
- `train.csv` → Training dataset  
- `test.csv` → Testing dataset  

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
- Read all records from MongoDB (database: e.g., `crochet`, collection: e.g., `network_data`).
- Drop unnecessary columns (e.g., Mongo `_id`), normalize missing values.
- Write a **raw snapshot** (e.g., `raw.csv`) into a **feature store** folder.
- Split into **train.csv** and **test.csv** and write into an **ingested** folder.

---

## Configuration Entities & Constants

Create `entity/config_entity.py` for pipeline-wide configs and `constant/training_pipeline/__init__.py` for constants.

**Examples of constants** (in `network_security/constant/training_pipeline/__init__.py`):
```python
PIPELINE_NAME = "network_security"
ARTIFACT_DIR = "artifacts"

DATA_INGESTION_DIR_NAME = "data_ingestion"
DATA_INGESTION_FEATURE_STORE_DIR = "feature_store"
DATA_INGESTION_INGESTED_DIR = "ingested"

FILE_NAME = "fishing_data.csv"   # raw snapshot name
TRAIN_FILE_NAME = "train.csv"
TEST_FILE_NAME = "test.csv"

DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO = 0.2
DATA_INGESTION_COLLECTION_NAME = "network_data"
DATA_INGESTION_DATABASE_NAME = "crochet"

TARGET_COLUMN = "result"
```

**Training pipeline config** (in `entity/config_entity.py`):
```python
from datetime import datetime
import os
from network_security.constant import training_pipeline

class TrainingPipelineConfig:
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        self.pipeline_name = training_pipeline.PIPELINE_NAME
        # artifacts/<timestamp>
        self.artifact_dir = os.path.join(training_pipeline.ARTIFACT_DIR, self.timestamp)
```

**Data ingestion config** (in `entity/config_entity.py`):
```python
class DataIngestionConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        self.data_ingestion_dir = os.path.join(
            training_pipeline_config.artifact_dir,
            training_pipeline.DATA_INGESTION_DIR_NAME
        )
        self.feature_store_file_path = os.path.join(
            self.data_ingestion_dir,
            training_pipeline.DATA_INGESTION_FEATURE_STORE_DIR,
            training_pipeline.FILE_NAME
        )
        self.training_file_path = os.path.join(
            self.data_ingestion_dir,
            training_pipeline.DATA_INGESTION_INGESTED_DIR,
            training_pipeline.TRAIN_FILE_NAME
        )
        self.test_file_path = os.path.join(
            self.data_ingestion_dir,
            training_pipeline.DATA_INGESTION_INGESTED_DIR,
            training_pipeline.TEST_FILE_NAME
        )
        self.train_test_split_ratio = training_pipeline.DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO
        self.collection_name = training_pipeline.DATA_INGESTION_COLLECTION_NAME
        self.database_name = training_pipeline.DATA_INGESTION_DATABASE_NAME
```

---

## Data Ingestion Component

Create `components/data_ingestion.py` with the logic to export data from MongoDB, write the raw snapshot, and produce train/test splits.

```python
import os, pymongo
import pandas as pd
import numpy as np
from typing import Tuple
from dataclasses import dataclass
from sklearn.model_selection import train_test_split

from network_security.entity.config_entity import DataIngestionConfig
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging

import os
from dotenv import load_dotenv
load_dotenv()
MONGODB_URL = os.getenv("MONGODB_URL")

@dataclass
class DataIngestionArtifact:
    train_file_path: str
    test_file_path: str

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            self.config = data_ingestion_config
            self.mongo_db_url = MONGODB_URL
        except Exception as e:
            raise NetworkSecurityException(e)

    def export_collection_as_dataframe(self) -> pd.DataFrame:
        try:
            client = pymongo.MongoClient(self.mongo_db_url)
            collection = client[self.config.database_name][self.config.collection_name]
            df = pd.DataFrame(list(collection.find()))
            if "_id" in df.columns:
                df.drop(columns=["_id"], inplace=True)
            df.replace(to_replace="na", value=np.nan, inplace=True)
            logging.info(f"Exported {len(df)} rows from MongoDB")
            return df
        except Exception as e:
            raise NetworkSecurityException(e)

    def export_data_to_feature_store(self, df: pd.DataFrame) -> str:
        try:
            # ensure parent dir exists
            os.makedirs(os.path.dirname(self.config.feature_store_file_path), exist_ok=True)
            df.to_csv(self.config.feature_store_file_path, index=False, header=True)
            logging.info(f"Wrote raw snapshot: {self.config.feature_store_file_path}")
            return self.config.feature_store_file_path
        except Exception as e:
            raise NetworkSecurityException(e)

    def split_data_as_train_test(self, df: pd.DataFrame) -> Tuple[str, str]:
        try:
            train_df, test_df = train_test_split(df, test_size=self.config.train_test_split_ratio, random_state=42)
            os.makedirs(os.path.dirname(self.config.training_file_path), exist_ok=True)
            train_df.to_csv(self.config.training_file_path, index=False, header=True)
            test_df.to_csv(self.config.test_file_path, index=False, header=True)
            logging.info(f"Train/Test written: {self.config.training_file_path}, {self.config.test_file_path}")
            return self.config.training_file_path, self.config.test_file_path
        except Exception as e:
            raise NetworkSecurityException(e)

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            df = self.export_collection_as_dataframe()
            self.export_data_to_feature_store(df)
            self.split_data_as_train_test(df)
            return DataIngestionArtifact(
                train_file_path=self.config.training_file_path,
                test_file_path=self.config.test_file_path
            )
        except Exception as e:
            raise NetworkSecurityException(e)
```

**Example runner (`main.py`)**
```python
from network_security.entity.config_entity import TrainingPipelineConfig, DataIngestionConfig
from network_security.components.data_ingestion import DataIngestion

if __name__ == "__main__":
    tpc = TrainingPipelineConfig()
    di_config = DataIngestionConfig(tpc)
    ingestion = DataIngestion(di_config)
    artifact = ingestion.initiate_data_ingestion()
    print(artifact)
```

---

## Notes & Good Practices
- Add **`.env`** to `.gitignore` and never commit secrets.
- For development you may whitelist `0.0.0.0/0` in MongoDB Atlas **Network Access**; for production, lock this down to known IPs/VPC peering.
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
   - Columns and data types must match the expected schema defined in `schema.yaml`.
   - Ensures numerical columns are present.
2. **Data Drift Detection**
   - Compares distributions of train vs test datasets using **Kolmogorov-Smirnov (ks_2samp)** test.
   - Generates a `report.yaml` summarizing drift per feature.
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
- `validation_status`
- `valid_train_file_path`
- `valid_test_file_path`
- `drift_report_file_path`

### Example Utility (read_yaml_file)
```python
import yaml

def read_yaml_file(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)
```

---

# Data Validation Implementation

```python
class DataValidation:
    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e)

    def validate_number_of_columns(self, df: pd.DataFrame) -> bool:
        required_cols = len(self.schema_config)
        return len(df.columns) == required_cols

    def detect_data_set_drift(self, base_df, current_df, threshold: float = 0.05) -> bool:
        from scipy.stats import ks_2samp
        report, status = {}, True
        for column in base_df.columns:
            ks_test = ks_2samp(base_df[column], current_df[column])
            p_value = ks_test.pvalue
            same = p_value > threshold
            report[column] = {"p_value": float(p_value), "same_distribution": same}
            if not same:
                status = False
        write_yaml(file_path=self.config.drift_report_file_path, content=report)
        return status
```

---

# Data Transformation Module

## Overview
The **Data Transformation** component prepares validated data for modeling. It reads train/test datasets, applies preprocessing, and saves transformed arrays + pipeline objects.

### Process
1. Read validated train & test datasets.
2. Split features (`X`) and target (`y`) using the target column.
3. Apply preprocessing pipeline (e.g., **KNN Imputer**, scaling).
4. Fit pipeline on train, transform both train & test.
5. Save transformed arrays and preprocessing pipeline.

### Configuration
Paths include:
- Data transformation directory inside `artifacts/`
- Transformed train/test arrays (.npy)
- Preprocessing pipeline pickle file

### Utility Functions
```python
import numpy as np, pickle, os

def save_numpy_array(file_path, array: np.ndarray):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    np.save(file_path, array)

def save_object(file_path, obj):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as f:
        pickle.dump(obj, f)
```

### Example Transformation Pipeline
```python
from sklearn.pipeline import Pipeline
from sklearn.impute import KNNImputer
from sklearn.preprocessing import RobustScaler

pipeline = Pipeline(steps=[
    ("imputer", KNNImputer(n_neighbors=3)),
    ("scaler", RobustScaler())
])
```

### Output Artifacts
- `train.npy` and `test.npy` – transformed feature + target arrays
- `preprocessor.pkl` – saved preprocessing pipeline

---

## Key Takeaways
- **Data Validation** ensures schema compliance and checks for data drift.  
- **Data Transformation** prepares the datasets with imputation, scaling, and preprocessing pipelines.  
- Both modules generate reusable **artifacts** stored under the `artifacts/` directory for downstream modeling.


---

# Data Validation — Part 1

## Overview
After **Data Ingestion**, the **Data Validation** module verifies schema consistency and checks for **data drift** between train and test (and future incoming) datasets.

### Inputs & Outputs
- **Input**: `DataIngestionArtifact` (paths to `train.csv`, `test.csv`, plus raw snapshot).
- **Output**: `DataValidationArtifact` (validation status, validated file paths, and a drift report path).

### Key Checks
- Schema matches expected columns and dtypes.
- Numerical columns exist.
- Consistent number of columns in train/test.
- Data drift detection between base (train) and current (test/new) data.

---

## Configuration & Directories
Create a **Data Validation Config** with directories like:
- `data_validation_dir/`
- `valid/` and `invalid/` subfolders
- `drift_report/` with `report.yaml`

Example (constants in `network_security/constant/training_pipeline/__init__.py`):
```python
DATA_VALIDATION_DIR_NAME = "data_validation"
DATA_VALIDATION_VALID_DIR = "valid"
DATA_VALIDATION_INVALID_DIR = "invalid"
DATA_VALIDATION_DRIFT_REPORT_DIR = "drift_report"
DATA_VALIDATION_DRIFT_REPORT_FILE_NAME = "report.yaml"
NUMERICAL_COLUMNS = ["col1", "col2", "col3"]  # example
```

**Config entity** (in `entity/config_entity.py`):
```python
class DataValidationConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        base_dir = os.path.join(
            training_pipeline_config.artifact_dir,
            training_pipeline.DATA_VALIDATION_DIR_NAME
        )
        self.valid_data_dir = os.path.join(base_dir, training_pipeline.DATA_VALIDATION_VALID_DIR)
        self.invalid_data_dir = os.path.join(base_dir, training_pipeline.DATA_VALIDATION_INVALID_DIR)
        self.valid_train_file_path = os.path.join(self.valid_data_dir, training_pipeline.TRAIN_FILE_NAME)
        self.valid_test_file_path  = os.path.join(self.valid_data_dir,  training_pipeline.TEST_FILE_NAME)
        self.invalid_train_file_path = os.path.join(self.invalid_data_dir, training_pipeline.TRAIN_FILE_NAME)
        self.invalid_test_file_path  = os.path.join(self.invalid_data_dir,  training_pipeline.TEST_FILE_NAME)
        self.drift_report_file_path = os.path.join(
            base_dir,
            training_pipeline.DATA_VALIDATION_DRIFT_REPORT_DIR,
            training_pipeline.DATA_VALIDATION_DRIFT_REPORT_FILE_NAME
        )
        self.schema_file_path = os.path.join("data_schema", "schema.yaml")
```

---

## Schema Definition (YAML)
Create `data_schema/schema.yaml`:
```yaml
columns:
  - name: feature1
    dtype: float
  - name: feature2
    dtype: int
  - name: feature3
    dtype: object
target_column: result
numerical_columns: [feature1, feature2]
```

Utility to read YAML (`network_security/utils/utils.py`):
```python
import yaml
def read_yaml_file(file_path: str) -> dict:
    with open(file_path, "r") as f:
        return yaml.safe_load(f)
```

---

## Data Validation Component (Part 1)
`components/data_validation.py`:
```python
import os, sys
import pandas as pd
from dataclasses import dataclass
from network_security.utils.utils import read_yaml_file
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging
from network_security.entity.config_entity import DataValidationConfig
from network_security.components.data_ingestion import DataIngestionArtifact

@dataclass
class DataValidationArtifact:
    validation_status: bool
    valid_train_file_path: str
    valid_test_file_path: str
    drift_report_file_path: str

class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.config = data_validation_config
            self.schema_config = read_yaml_file(self.config.schema_file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_number_of_columns(self, df: pd.DataFrame) -> bool:
        try:
            required_cols = [c["name"] for c in self.schema_config["columns"]]
            logging.info(f"Required columns: {len(required_cols)}, DF columns: {len(df.columns)}")
            return len(df.columns) == len(required_cols)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_numerical_columns_exist(self, df: pd.DataFrame) -> bool:
        try:
            numerical_cols = self.schema_config.get("numerical_columns", [])
            missing = [c for c in numerical_cols if c not in df.columns]
            if missing:
                logging.info(f"Missing numerical cols: {missing}")
                return False
            return True
        except Exception as e:
            raise NetworkSecurityException(e, sys)
```

---

# Data Validation — Part 2

## Drift Detection & Artifact Generation
Extend `DataValidation` with drift detection using **Kolmogorov–Smirnov** test:

```python
import numpy as np, os
from scipy.stats import ks_2samp

def write_yaml(file_path: str, content: dict):
    import yaml
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        yaml.safe_dump(content, f)

class DataValidation(DataValidation):  # extend
    def detect_data_set_drift(self, base_df: pd.DataFrame, current_df: pd.DataFrame, threshold: float = 0.05) -> bool:
        try:
            status = True
            report = {}
            for col in base_df.columns:
                if col not in current_df.columns:
                    report[col] = {"error": "missing in current_df"}
                    status = False
                    continue
                d1, d2 = base_df[col].dropna(), current_df[col].dropna()
                # Only run KS on numeric columns
                if np.issubdtype(d1.dtype, np.number) and np.issubdtype(d2.dtype, np.number):
                    p = ks_2samp(d1, d2).pvalue
                    same = p > threshold
                    status = status and same
                    report[col] = {"p_value": float(p), "same_distribution": bool(same)}
                else:
                    report[col] = {"skipped": "non-numeric"}
            write_yaml(self.config.drift_report_file_path, report)
            return status
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_fp = self.data_ingestion_artifact.train_file_path
            test_fp  = self.data_ingestion_artifact.test_file_path

            train_df = self.read_data(train_fp)
            test_df  = self.read_data(test_fp)

            status = True
            for df, name in [(train_df, "train"), (test_df, "test")]:
                if not self.validate_number_of_columns(df):
                    logging.info(f"{name} does not contain all required columns")
                    status = False
                if not self.validate_numerical_columns_exist(df):
                    logging.info(f"{name} missing required numerical columns")
                    status = False

            # drift between train (base) and test (current)
            if status:
                status = self.detect_data_set_drift(train_df, test_df)

            # write validated copies if OK
            if status:
                os.makedirs(os.path.dirname(self.config.valid_train_file_path), exist_ok=True)
                train_df.to_csv(self.config.valid_train_file_path, index=False)
                test_df.to_csv(self.config.valid_test_file_path, index=False)
            else:
                os.makedirs(os.path.dirname(self.config.invalid_train_file_path), exist_ok=True)
                train_df.to_csv(self.config.invalid_train_file_path, index=False)
                test_df.to_csv(self.config.invalid_test_file_path, index=False)

            return DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.config.valid_train_file_path if status else self.config.invalid_train_file_path,
                valid_test_file_path=self.config.valid_test_file_path if status else self.config.invalid_test_file_path,
                drift_report_file_path=self.config.drift_report_file_path
            )
        except Exception as e:
            raise NetworkSecurityException(e, sys)
```

Example usage:
```python
from network_security.components.data_validation import DataValidation
# data_ingestion_artifact, data_validation_config must be created earlier
validator = DataValidation(data_ingestion_artifact, data_validation_config)
artifact = validator.initiate_data_validation()
print(artifact)
```

---

# Data Transformation Architecture

## Overview
The **Data Transformation** module consumes validated datasets and outputs **transformed arrays** plus a **preprocessing pipeline** for consistent inference.

### Steps
1. Read validated train/test from `DataValidationArtifact`.
2. Split into **X (features)** and **y (target)** using `TARGET_COLUMN` from constants.
3. Build a preprocessing pipeline (e.g., **KNNImputer**, **RobustScaler**).
4. `fit` on train features; `transform` train and test (avoid leakage).
5. Persist transformed arrays (`.npy`) and the fitted pipeline (`.pkl`).

## Config & Artifacts
Constants (examples):
```python
DATA_TRANSFORMATION_DIR_NAME = "data_transformation"
DATA_TRANSFORMATION_TRANSFORMED_DIR = "transformed"
DATA_TRANSFORMATION_TRANSFORMED_TRAIN_FILE_NAME = "train.npy"
DATA_TRANSFORMATION_TRANSFORMED_TEST_FILE_NAME = "test.npy"
DATA_TRANSFORMATION_PREPROCESSOR_DIR = "preprocessor"
DATA_TRANSFORMATION_PREPROCESSOR_FILE_NAME = "preprocessor.pkl"
```

Config entity:
```python
class DataTransformationConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        base = os.path.join(training_pipeline_config.artifact_dir, training_pipeline.DATA_TRANSFORMATION_DIR_NAME)
        self.transformed_train_file_path = os.path.join(base, training_pipeline.DATA_TRANSFORMATION_TRANSFORMED_DIR, training_pipeline.DATA_TRANSFORMATION_TRANSFORMED_TRAIN_FILE_NAME)
        self.transformed_test_file_path  = os.path.join(base, training_pipeline.DATA_TRANSFORMATION_TRANSFORMED_DIR, training_pipeline.DATA_TRANSFORMATION_TRANSFORMED_TEST_FILE_NAME)
        self.preprocessor_file_path = os.path.join(base, training_pipeline.DATA_TRANSFORMATION_PREPROCESSOR_DIR, training_pipeline.DATA_TRANSFORMATION_PREPROCESSOR_FILE_NAME)
```

Artifacts & utils:
```python
from dataclasses import dataclass
import numpy as np, pickle, os

@dataclass
class DataTransformationArtifact:
    transformed_train_file_path: str
    transformed_test_file_path: str
    preprocessor_file_path: str

def save_numpy_array(file_path: str, array: np.ndarray):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    np.save(file_path, array)

def save_object(file_path: str, obj):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as f:
        pickle.dump(obj, f)
```

Component (`components/data_transformation.py`):
```python
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.impute import KNNImputer
from sklearn.preprocessing import RobustScaler

from network_security.constant import training_pipeline as C

class DataTransformation:
    def __init__(self, config: DataTransformationConfig, validation_artifact: DataValidationArtifact):
        self.config = config
        self.validation_artifact = validation_artifact

    def get_preprocessor(self) -> Pipeline:
        return Pipeline(steps=[
            ("imputer", KNNImputer(n_neighbors=5)),
            ("scaler", RobustScaler(with_centering=True, with_scaling=True))
        ])

    def initiate_data_transformation(self) -> DataTransformationArtifact:
        train_df = pd.read_csv(self.validation_artifact.valid_train_file_path)
        test_df  = pd.read_csv(self.validation_artifact.valid_test_file_path)

        X_train, y_train = train_df.drop(C.TARGET_COLUMN, axis=1), train_df[C.TARGET_COLUMN]
        X_test,  y_test  = test_df.drop(C.TARGET_COLUMN, axis=1),  test_df[C.TARGET_COLUMN]

        preprocessor = self.get_preprocessor()
        X_train_t = preprocessor.fit_transform(X_train)
        X_test_t  = preprocessor.transform(X_test)

        # concatenate target as last column for convenience
        train_arr = np.c_[X_train_t, y_train.to_numpy()]
        test_arr  = np.c_[X_test_t,  y_test.to_numpy()]

        save_numpy_array(self.config.transformed_train_file_path, train_arr)
        save_numpy_array(self.config.transformed_test_file_path,  test_arr)
        save_object(self.config.preprocessor_file_path, preprocessor)

        return DataTransformationArtifact(
            transformed_train_file_path=self.config.transformed_train_file_path,
            transformed_test_file_path=self.config.transformed_test_file_path,
            preprocessor_file_path=self.config.preprocessor_file_path
        )
```

## Key Points
- Use `fit` on train only; `transform` on test to **avoid leakage**.
- Keep the preprocessing pipeline versioned—save with artifacts for reproducibility.
- Add class balancing (e.g., **SMOTE Tomek**) if needed later.



---

# Data Transformation Implementation

## Class Definition
```python
class DataTransformation:
    def __init__(self, data_validation_artifact: DataValidationArtifact, data_transformation_config: DataTransformationConfig):
        try:
            self.data_validation_artifact = data_validation_artifact
            self.data_transformation_config = data_transformation_config
        except Exception as e:
            raise NetworkSecurityException(e)
```

## Method: initiate_data_transformation
```python
def initiate_data_transformation(self) -> DataTransformationArtifact:
    try:
        logging.info("Starting data transformation")

        train_df = DataTransformation.read_data(self.data_validation_artifact.valid_train_file_path)
        test_df = DataTransformation.read_data(self.data_validation_artifact.valid_test_file_path)

        # Features vs Target
        X_train = train_df.drop(columns=TARGET_COLUMN, axis=1)
        y_train = train_df[TARGET_COLUMN].replace(-1, 0)
        X_test = test_df.drop(columns=TARGET_COLUMN, axis=1)
        y_test = test_df[TARGET_COLUMN].replace(-1, 0)

        preprocessor = self.get_data_transformer_object()
        X_train_arr = preprocessor.fit_transform(X_train)
        X_test_arr = preprocessor.transform(X_test)

        train_arr = np.c_[X_train_arr, y_train]
        test_arr  = np.c_[X_test_arr, y_test]

        save_numpy_array(self.data_transformation_config.transform_train_file_path, train_arr)
        save_numpy_array(self.data_transformation_config.transform_test_file_path, test_arr)
        save_object(self.data_transformation_config.transformed_object_file_path, preprocessor)

        return DataTransformationArtifact(
            transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
            transformed_train_file_path=self.data_transformation_config.transform_train_file_path,
            transformed_test_file_path=self.data_transformation_config.transform_test_file_path
        )
    except Exception as e:
        raise NetworkSecurityException(e)
```

## Method: get_data_transformer_object
```python
@staticmethod
def get_data_transformer_object() -> Pipeline:
    logging.info("Entered get_data_transformer_object")
    try:
        knn_imputer = KNNImputer(**DATA_TRANSFORMATION_IMPUTER_PARAMS)
        processor = Pipeline(steps=[("imputer", knn_imputer)])
        return processor
    except Exception as e:
        raise NetworkSecurityException(e)
```

**Explanation**
- Drops target column, isolates features and target.
- Converts target values (-1 → 0).
- Uses **KNNImputer** pipeline for missing values.
- Saves numpy arrays and preprocessor pickle.
- Returns a `DataTransformationArtifact`.

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
- `ClassificationMetricArtifact` (F1, precision, recall)
- `ModelTrainerArtifact` (trained model path, metrics)

**Utilities**:
- `save_object`, `load_object`
- `load_numpy_array_data`

**Classification Metrics Utility** (`ml_utils/metric.py`):
```python
from sklearn.metrics import f1_score, precision_score, recall_score
@dataclass
class ClassificationMetricArtifact:
    f1_score: float
    precision_score: float
    recall_score: float

def get_classification_score(y_true, y_pred) -> ClassificationMetricArtifact:
    return ClassificationMetricArtifact(
        f1_score=f1_score(y_true, y_pred),
        precision_score=precision_score(y_true, y_pred),
        recall_score=recall_score(y_true, y_pred)
    )
```

---

# Model Trainer & Evaluation with Hyperparameter Tuning

## Training Flow
- Load arrays → split into features and targets.
- Initialize models: RandomForest, DecisionTree, GradientBoosting, LogisticRegression, AdaBoost.
- Define parameter grids for each model.
- Use **GridSearchCV** for hyperparameter tuning.

## Evaluate Models Function
```python
def evaluate_models(X_train, y_train, X_test, y_test, models: dict, params: dict):
    from sklearn.model_selection import GridSearchCV
    from sklearn.metrics import r2_score

    report = {}
    for name, model in models.items():
        gs = GridSearchCV(model, params[name], cv=3)
        gs.fit(X_train, y_train)
        model = gs.best_estimator_
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        train_score = r2_score(y_train, y_train_pred)
        test_score  = r2_score(y_test, y_test_pred)
        report[name] = {"train_score": train_score, "test_score": test_score, "best_model": model}
    return report
```

## Model Selection
- Pick model with **highest test score**.
- Compute classification metrics for train/test with `get_classification_score`.
- Wrap preprocessor + model into `NetworkModel` class for easy prediction.
- Save as pickle.

---

# MLflow Experiment Tracking

## Why MLflow?
- Track metrics (F1, precision, recall).
- Log models for reuse.
- Compare multiple runs.

## Function Example
```python
import mlflow, mlflow.sklearn

def track_mlflow(keyword, best_model, classification_metrics):
    with mlflow.start_run():
        mlflow.log_metric("f1_score", classification_metrics.f1_score)
        mlflow.log_metric("precision_score", classification_metrics.precision_score)
        mlflow.log_metric("recall_score", classification_metrics.recall_score)
        mlflow.sklearn.log_model(best_model, "model")
```

## Running & Viewing
- Run pipeline: `python main.py`
- Start UI: `mlflow ui`
- View experiments at http://127.0.0.1:5000

**Best Practices**
- Exclude `mlruns/` from Git via `.gitignore`.
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

```python
class DataTransformation:
    def __init__(self, data_validation_artifact: DataValidationArtifact, data_transformation_config: DataTransformationConfig):
        try:
            self.data_validation_artifact = data_validation_artifact
            self.data_transformation_config = data_transformation_config
        except Exception as e:
            raise NetworkSecurityException(e)

    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        return pd.read_csv(file_path)

    @staticmethod
    def get_data_transformer_object() -> Pipeline:
        logging.info("Entered get_data_transformer_object method of DataTransformation class")
        try:
            knn_imputer = KNNImputer(**DATA_TRANSFORMATION_IMPUTER_PARAMS)
            logging.info(f"Initialized KNN Imputer with parameters: {DATA_TRANSFORMATION_IMPUTER_PARAMS}")
            processor = Pipeline(steps=[('imputer', knn_imputer)])
            return processor
        except Exception as e:
            raise NetworkSecurityException(e)

    def initiate_data_transformation(self) -> DataTransformationArtifact:
        try:
            logging.info("Starting data transformation")

            # Reading train and test data
            train_df = DataTransformation.read_data(self.data_validation_artifact.valid_train_file_path)
            test_df = DataTransformation.read_data(self.data_validation_artifact.valid_test_file_path)

            # Removing target column
            input_feature_train_df = train_df.drop(columns=TARGET_COLUMN, axis=1)
            target_feature_train_df = train_df[TARGET_COLUMN]

            # Converting target values from -1 to 0
            target_feature_train_df.replace(-1, 0, inplace=True)

            input_feature_test_df = test_df.drop(columns=TARGET_COLUMN, axis=1)
            target_feature_test_df = test_df[TARGET_COLUMN]
            target_feature_test_df.replace(-1, 0, inplace=True)

            # Applying KNN Imputer
            preprocessor = self.get_data_transformer_object()
            input_feature_train_arr = preprocessor.fit_transform(input_feature_train_df)
            input_feature_test_arr = preprocessor.transform(input_feature_test_df)

            # Combining transformed features with target
            train_arr = np.c_[input_feature_train_arr, target_feature_train_df]
            test_arr = np.c_[input_feature_test_arr, target_feature_test_df]

            # Saving numpy arrays
            save_numpy_array(self.data_transformation_config.transform_train_file_path, train_arr)
            save_numpy_array(self.data_transformation_config.transform_test_file_path, test_arr)

            # Saving preprocessor object
            save_object(self.data_transformation_config.transformed_object_file_path, preprocessor)

            # Preparing artifact
            data_transformation_artifact = DataTransformationArtifact(
                transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
                transformed_train_file_path=self.data_transformation_config.transform_train_file_path,
                transformed_test_file_path=self.data_transformation_config.transform_test_file_path
            )

            logging.info("Data transformation completed successfully")
            return data_transformation_artifact

        except Exception as e:
            raise NetworkSecurityException(e)
```

**What it does**
- Reads validated train/test CSVs.
- Drops `TARGET_COLUMN` from features.
- Maps target `-1 → 0` for binary classification.
- Applies **KNNImputer** via a scikit-learn `Pipeline`.
- Saves transformed arrays and the preprocessor `.pkl`.
- Returns a `DataTransformationArtifact` with output paths.

---

# Model Trainer — Part 1 (Architecture & Setup)

## Purpose
Consumes **DataTransformationArtifact** and a **ModelTrainerConfig** to train multiple models, evaluate, select the best, and persist it.

### Typical Config Fields
- `model_trainer_directory`
- `train_model_file_path`
- `expected_accuracy` (e.g., 0.60)
- `model_config_file_path` (optional)
- `threshold` for minimum improvement

### Artifacts
- `ClassificationMetricArtifact`: F1, Precision, Recall
- `ModelTrainerArtifact`: trained model path + train/test metrics

### Utilities
- `save_object(obj, path)` / `load_object(path)`
- `load_numpy_array_data(path)`

### Wrapper for Inference
A `NetworkModel(processor, model)` class combines preprocessing and prediction.

---

# Model Trainer — Training & Evaluation with Hyperparameter Tuning

## Approach
Use `GridSearchCV` across several algorithms, evaluate on train & test, and pick the best test performer.

### Models (examples)
- RandomForestClassifier(verbose=1)
- DecisionTreeClassifier
- GradientBoostingClassifier
- LogisticRegression
- AdaBoostClassifier

### Skeleton
```python
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import f1_score, precision_score, recall_score

def evaluate_models(X_train, y_train, X_test, y_test, models: dict, params: dict) -> dict:
    report = {}
    for name, model in models.items():
        gs = GridSearchCV(model, params.get(name, {}), cv=3, n_jobs=-1, verbose=1)
        gs.fit(X_train, y_train)
        best_model = gs.best_estimator_
        y_tr_pred = best_model.predict(X_train)
        y_te_pred = best_model.predict(X_test)
        report[name] = {
            "f1_train": f1_score(y_train, y_tr_pred),
            "f1_test":  f1_score(y_test,  y_te_pred),
            "precision_test": precision_score(y_test, y_te_pred),
            "recall_test":    recall_score(y_test, y_te_pred),
            "estimator": best_model
        }
    return report

# select best by f1_test
report = evaluate_models(X_train, y_train, X_test, y_test, models, params)
best_name = max(report, key=lambda k: report[k]["f1_test"])
best = report[best_name]["estimator"]
```

### Persist Combined Model
```python
preprocessor = load_object(preprocessor_path)
network_model = NetworkModel(processor=preprocessor, model=best)
save_object(train_model_file_path, network_model)
```

### Artifact
Includes the trained model path and classified metrics (train/test).

---

# Model Experiment Tracking with MLflow

## Why MLflow
Track metrics & models, compare runs, and manage experiment artifacts.

### Install
```
pip install mlflow
```

### Minimal Tracker
```python
import mlflow, mlflow.sklearn

def track_mlflow(keyword, best_model, classification_metrics):
    with mlflow.start_run(run_name=str(keyword)):
        mlflow.log_metric("f1_score", classification_metrics.f1_score)
        mlflow.log_metric("precision_score", classification_metrics.precision_score)
        mlflow.log_metric("recall_score", classification_metrics.recall_score)
        mlflow.sklearn.log_model(best_model, "model")
```

### View UI
```
mlflow ui
```
- Compare runs, view metrics & artifacts.
- **Tip**: add `mlruns/` to `.gitignore`.

---



---

# MLflow Experiment Tracking with Remote Repository (DAGsHub)

## Introduction
Instead of logging MLflow runs locally (`mlruns/`), you can configure tracking to a remote repo such as **DAGsHub**, which provides experiment versioning + collaboration.

## Setup
1. Create / connect a **GitHub repo** with **DAGsHub** (sign in with GitHub/GitLab/Bitbucket).
2. Install `dagshub` package:
```bash
pip install dagshub
```
3. Initialize inside your code:
```python
from dagshub import DAGsHubLogger
DAGsHubLogger(repo_owner="Krishna06", repo_name="network-security", mlflow=True)
```
4. Run training as usual:
```bash
python main.py
```
- Now metrics and artifacts sync to DAGsHub instead of local `mlruns/`.

## Viewing Results
- Experiments appear in DAGsHub → *Experiments* section.
- Each run shows metrics (F1, precision, recall), artifacts (pickles), and a link to MLflow UI.
- Shareable URLs make collaboration easy.

## Extra Features
- DAGsHub also integrates with **DVC** for dataset versioning.
- Recommended: add `mlruns/` to `.gitignore` to avoid clutter.

---

# Model Pusher Implementation

## Goal
Consolidate best model + preprocessor pickle into a **final_models/** folder for prediction or deployment.

## Process
- Copy trained model pickle (`model.pkl`) from model trainer artifacts.
- Copy preprocessing pickle (`preprocessor.pkl`) from transformation artifacts.
- Save into:
```
final_models/
    model.pkl
    preprocessing.pkl
```
- Optional: extend to upload these files to **S3** or cloud storage.

**Example:**
```python
final_model_dir = "final_models"
os.makedirs(final_model_dir, exist_ok=True)
save_object(os.path.join(final_model_dir, "model.pkl"), best_model)
save_object(os.path.join(final_model_dir, "preprocessing.pkl"), preprocessor)
```

---

# Training Pipeline Implementation

## Modular Pipeline
Implements a sequential flow: **Ingestion → Validation → Transformation → Training → Pusher**.

### TrainingPipeline Class
```python
class TrainingPipeline:
    def __init__(self):
        self.training_pipeline_config = TrainingPipelineConfig()

    def start_data_ingestion(self): ...
    def start_data_validation(self, ingestion_artifact): ...
    def start_data_transformation(self, validation_artifact): ...
    def start_model_trainer(self, transformation_artifact): ...
    def run_pipeline(self):
        ingestion_artifact = self.start_data_ingestion()
        validation_artifact = self.start_data_validation(ingestion_artifact)
        transformation_artifact = self.start_data_transformation(validation_artifact)
        trainer_artifact = self.start_model_trainer(transformation_artifact)
        return trainer_artifact
```

### FastAPI Integration
Expose training trigger via API:
```python
from fastapi import FastAPI
import uvicorn
from training_pipeline import TrainingPipeline

app = FastAPI()

@app.get("/train")
async def train():
    pipeline = TrainingPipeline()
    pipeline.run_pipeline()
    return {"message": "Training successful"}

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
```

### Run
```bash
uvicorn app:app --reload
```
- Open http://localhost:8000/docs to hit `/train` and trigger full pipeline.

## Key Takeaways
- DAGsHub allows **remote MLflow tracking**.
- Model pusher centralizes final artifacts in `final_models/`.
- Training pipeline orchestrates all components end-to-end.
- FastAPI provides an interface to trigger training with a REST call.

---



---

# Final Model & Artifacts Pusher to AWS S3

## Why S3?
- Local storage (`final_models/`) is not scalable for large or versioned models.  
- AWS S3 provides durable, versioned, and shareable storage for **artifacts** + **models**.

## Implementation
Create `cloud/s3_sync.py` with:
```python
import os

class S3Sync:
    def sync_folder_to_s3(self, folder_name, aws_bucket_url):
        command = f"aws s3 sync {folder_name} {aws_bucket_url}"
        os.system(command)

    def sync_folder_from_s3(self, aws_bucket_url, folder_name):
        command = f"aws s3 sync {aws_bucket_url} {folder_name}"
        os.system(command)
```

Use inside pipeline:
```python
s3 = S3Sync()
s3.sync_folder_to_s3("artifacts/", "s3://mybucket/artifacts")
s3.sync_folder_to_s3("final_models/", "s3://mybucket/final_models")
```

## Setup
1. Install & configure AWS CLI:
```bash
aws configure
```
- Provide `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `region`, output format.

2. Ensure IAM user has S3 permissions.

3. Run pipeline → artifacts & models sync to bucket.

---

# Building Docker Image & GitHub Actions

## Dockerfile Example
```dockerfile
FROM python:3.10-slim-buster

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# install awscli if needed
RUN apt-get update && apt-get install -y awscli

CMD ["python", "app.py"]
```

## GitHub Actions Workflow (main.yaml)
```yaml
name: CI/CD Pipeline

on:
  push:
    branches: [ main ]
    paths-ignore: [ "README.md" ]

jobs:
  integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - run: echo "Linting & Tests here..."

  build_and_push_ecr_image:
    needs: integration
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: aws-actions/configure-aws-credentials@v1
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ secrets.AWS_REGION }}
      - uses: aws-actions/amazon-ecr-login@v1
      - run: |
          docker build -t ${{ secrets.ECR_REPOSITORY_NAME }}:latest .
          docker tag ${{ secrets.ECR_REPOSITORY_NAME }}:latest ${{ secrets.AWS_ECR_LOGIN_URI }}/${{ secrets.ECR_REPOSITORY_NAME }}:latest
          docker push ${{ secrets.AWS_ECR_LOGIN_URI }}/${{ secrets.ECR_REPOSITORY_NAME }}:latest
```

Add required GitHub Secrets:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_ECR_LOGIN_URI`
- `ECR_REPOSITORY_NAME`

---

# Deployment to AWS EC2 Instance

## Steps
1. Launch EC2 instance (`t2.medium` suggested).  
2. SSH or use EC2 Instance Connect.  
3. Install Docker + setup non-sudo permissions:
```bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install docker.io -y
sudo usermod -aG docker ubuntu
```

4. Configure **GitHub self-hosted runner** on EC2:  
```bash
mkdir actions-runner && cd actions-runner
curl -o actions-runner-linux-x64.tar.gz -L https://github.com/actions/runner/releases/download/v2.277.1/actions-runner-linux-x64-2.277.1.tar.gz
tar xzf ./actions-runner-linux-x64.tar.gz
./config.sh --url <repo-url> --token <token>
./run.sh
```

5. Extend workflow with deployment job (runs on self-hosted EC2 runner).

## Deployment Job (YAML)
```yaml
deploy:
  needs: build_and_push_ecr_image
  runs-on: self-hosted
  steps:
    - uses: actions/checkout@v2
    - uses: aws-actions/configure-aws-credentials@v1
      with:
        aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
        aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        aws-region: ${{ secrets.AWS_REGION }}
    - uses: aws-actions/amazon-ecr-login@v1
    - run: |
        docker pull ${{ secrets.AWS_ECR_LOGIN_URI }}/${{ secrets.ECR_REPOSITORY_NAME }}:latest
        docker stop network_security || true && docker rm network_security || true
        docker run -d -p 8080:8080 --name network_security ${{ secrets.AWS_ECR_LOGIN_URI }}/${{ secrets.ECR_REPOSITORY_NAME }}:latest
```

## Security Group
- Open inbound rule: TCP 8080 (or your app port).  
- Access via `http://<EC2_PUBLIC_IP>:8080`.

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
- **sync_artifact_directory_to_s3** → uploads the entire `artifacts/` tree
- **sync_final_model_directory_to_s3** → uploads `final_models/` (e.g., `model.pkl`, `preprocessing.pkl`)

## Use AWS CLI (simple & robust)
Install and configure **AWS CLI**, then `sync` folders:
```bash
aws configure  # supply ACCESS_KEY, SECRET_KEY, region, output=json
aws s3 sync <local_folder> <s3://bucket/path>
```

## S3Sync helper (cloud/s3_sync.py)
```python
import os

class S3Sync:
    def sync_folder_to_s3(self, folder_name: str, aws_bucket_url: str):
        os.system(f"aws s3 sync {folder_name} {aws_bucket_url}")

    def sync_folder_from_s3(self, aws_bucket_url: str, folder_name: str):
        os.system(f"aws s3 sync {aws_bucket_url} {folder_name}")
```

### Integrate in pipeline
```python
# example usage
self.s3_sync.sync_folder_to_s3(self.artifact_directory, self.aws_bucket_url_artifacts)
self.s3_sync.sync_folder_to_s3(self.final_model_directory, self.aws_bucket_url_final_models)
```

### AWS prerequisites
- IAM user with S3 permissions (e.g., `s3:PutObject`, `s3:ListBucket`).
- `aws configure` set on the machine/runner.

---

# Building Docker Image & GitHub Actions (CI overview)

## Dockerfile (example)
```dockerfile
FROM python:3.10-slim-buster

WORKDIR /app
COPY . /app

# System deps (optional) + AWS CLI if needed in container
RUN pip install --upgrade pip &&     pip install awscli &&     pip install -r requirements.txt

# Expose app
EXPOSE 8080

# Start app (uvicorn started inside app.py or here directly)
CMD ["python", "app.py"]
```

## Basic CI workflow (/.github/workflows/main.yaml)
```yaml
name: CI-CD

on:
  push:
    branches: [ "main" ]
    paths-ignore:
      - "README.md"

jobs:
  integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Lint & Test
        run: |
          echo "Run flake8/pytest here"
```

---

# GitHub Action — Docker Image Push to AWS ECR (CD to registry)

## Create ECR repository
- AWS Console → **ECR** → *Create repository* (private) → e.g., `network-security`
- Note **repository URI** (e.g., `123456789012.dkr.ecr.us-east-1.amazonaws.com`)

## Add secrets in GitHub → Settings → Secrets and variables → Actions
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION` (e.g., `us-east-1`)
- `AWS_ECR_LOGIN_URI` (e.g., `123456789012.dkr.ecr.us-east-1.amazonaws.com`)
- `ECR_REPOSITORY_NAME` (e.g., `network-security`)

## Extend workflow
```yaml
  build_and_push_ecr_image:
    needs: integration
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Login to Amazon ECR
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build, tag, and push
        env:
          ECR: ${{ secrets.AWS_ECR_LOGIN_URI }}
          REPO: ${{ secrets.ECR_REPOSITORY_NAME }}
        run: |
          docker build -t $REPO:latest .
          docker tag $REPO:latest $ECR/$REPO:latest
          docker push $ECR/$REPO:latest
```

---

# Final Deployment to AWS EC2 (CD — run container)

## EC2 prep
- Launch Ubuntu instance (e.g., t2.medium), enable inbound **TCP 8080**
- Install Docker & set permissions:
```bash
sudo apt-get update -y && sudo apt-get upgrade -y
sudo apt-get install -y docker.io
sudo usermod -aG docker ubuntu
```

## Self-hosted GitHub Runner on EC2
- Repo → **Settings** → **Runners** → *New self-hosted runner* (Linux)
- Follow the generated commands on EC2:
```bash
mkdir actions-runner && cd actions-runner
curl -o actions-runner-linux-x64.tar.gz -L https://github.com/actions/runner/releases/download/v2.317.0/actions-runner-linux-x64-2.317.0.tar.gz
tar xzf actions-runner-linux-x64.tar.gz
./config.sh --url <REPO_URL> --token <TOKEN>
./run.sh
```

## Deployment job (runs-on: self-hosted)
```yaml
  deploy_to_ec2:
    needs: build_and_push_ecr_image
    runs-on: self-hosted
    steps:
      - uses: actions/checkout@v3

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Login to Amazon ECR
        uses: aws-actions/amazon-ecr-login@v2

      - name: Pull & run container
        env:
          ECR: ${{ secrets.AWS_ECR_LOGIN_URI }}
          REPO: ${{ secrets.ECR_REPOSITORY_NAME }}
        run: |
          docker pull $ECR/$REPO:latest
          # stop/remove if exists
          docker rm -f network-security || true
          docker run -d --name network-security -p 8080:8080 $ECR/$REPO:latest
```

### Verify
- Open `http://<EC2_PUBLIC_IP>:8080` (ensure security group allows 8080)
- Swagger at `/docs` if using FastAPI example

---

## Key Takeaways
- **S3 sync** keeps your `artifacts/` and `final_models/` durable & versioned.
- **ECR** stores Docker images privately inside AWS.
- **EC2 + self-hosted runner** enables full CD: pull image and run container on pushes.
- Keep credentials in **GitHub Secrets**; never commit them.
