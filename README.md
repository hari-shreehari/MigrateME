<div align="center">
  <h1 align="center">MigrateME</h1>
</div>

MigrateME is a flexible, self-hostable and distributed data migration tool designed to seamlessly transfer data between various file formats and databases.

## Key Features

- **Versatile Data Source Support:**
  - **Files:** CSV, JSON, XLSX, XLS, ODS, Avro, Parquet, ORC
  - **Databases:** PostgreSQL, MongoDB, SQLite
  - **Folders:** Process entire folders of files at once.

- **RESTful API:** Easy-to-use API for initiating and managing data migrations.

- **PySpark:** Utilizes the power of Spark for efficient and distributed data processing.

## Setup and Usage

### 1. Environment Setup

**Using `pip`:**

Install the required dependencies from the `requirements.txt` file:

```bash
pip install -r requirements.txt
```

Or you can go ahead with conda environment utilizing the yml file provided 


### 2. Running the Application

To start the backend server, run the following command from the root directory:

```bash
uvicorn BackEnd.main:app --reload
```

### 3. Using the API

You can interact with the API by using the provided Postman collections over your prefered API testing platform.
`convert` - To convert the files on the system/server directly.
`upload` - For users to upload files from their device to the server for conversion. Uses the same logic as convert with added functionality for file transfer.

#### Postman Collections

The `Postman_Collections` directory contains collections for the `upload` and `convert` endpoints. You can import these into Postman to easily test the API.

- `Upload.postman_collection.json`
- `Convert.postman_collection.json`

## API Examples

Here are some examples of how to use the API for `/convert`:

### Example 1: Convert a CSV file to another CSV file

**URL:** `http://127.0.0.1:8000/convert`

**Body:**
```json
{
  "input": {
    "type": "csv",
    "path": "./TestFiles/airports.csv"
  },
  "output": {
    "type": "csv",
    "path": "./Output/airports_new.csv"
  }
}
```

### Example 2: Convert PostgreSQL table to a CSV file

**URL:** `http://127.0.0.1:8000/convert`

**Body:**
```json
{
  "input": {
    "type": "postgresql",
    "url": "jdbc:postgresql://your_db_host:port/your_db",
    "properties": {
        "user": "your_username",
        "password": "your_password",
        "driver": "org.postgresql.Driver"
    },
    "table": "your_table_name"
  },
  "output": {
   "type": "csv",
   "path": "./Output/from_postgres"
   }
}
```

### Example 3: Upload a folder of CSVs to a PostgreSQL database

**URL:** `http://127.0.0.1:8000/convert`

**Body:**
```json
{
  "input": {
    "type": "folder",
    "path": "./TestFiles/Group"
  },
  "output": {
    "type": "postgresql",
    "url": "jdbc:postgresql://your_db_host:port/your_db",
    "properties": {
        "user": "your_username",
        "password": "your_password",
        "driver": "org.postgresql.Driver"
    }
  }
}
```

For `/upload` use form data 

Key 1 -> Request : Has a json with conversion format 
```
{"input": {"type": "json"}, "output": {"type": "avro", "path": "json-avro"}}
```
Key 2 -> File : Upload the file 
