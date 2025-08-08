# Stage 1: Data Extraction with Athena

## Objective
Extract key facility metrics from JSON data in S3 using Amazon Athena SQL queries.

## Implementation Steps

### 1. S3 Setup
- Created bucket: `healthcare-facility-data`
- Uploaded JSON data in JSON Lines format
- Set up folder structure: `input/`, `athena-results/`

### 2. Athena Configuration
- Created database: `healthcare_analytics`
- Configured query result location in S3
- Set up external table with proper SerDe

### 3. Table Schema Design
- Mapped complex nested JSON structure
- Used appropriate Hive data types (struct, array)
- Handled JSON SerDe configuration

### 4. Query Development
- Main extraction query with UNNEST for accreditations
- Date parsing and MIN aggregation
- Proper ordering and formatting

## Key Technical Challenges

### JSON Format Conversion
- **Challenge**: Original data was JSON array format
- **Solution**: Converted to JSON Lines (one object per line)
- **Reason**: Athena SerDe requires newline-delimited JSON

### Complex Nested Data
- **Challenge**: Extracting minimum date from array of accreditations
- **Solution**: Used UNNEST() and MIN() aggregation
- **Query Logic**: 
  ```sql
  (SELECT MIN(date_parse(acc.valid_until, '%Y-%m-%d'))
   FROM UNNEST(accreditations) as t(acc))
   

# Stage 2: Automated Data Processing with Python & AWS SDK

## Objective
Develop a Python application using AWS SDK (boto3) to automatically filter healthcare facilities with accreditations expiring within 6 months and store results back to S3.

## Implementation Steps

### 1. Python Environment Setup
- Created virtual environment with required dependencies
- Installed boto3, python-dateutil for AWS integration and date handling
- Configured AWS credentials for S3 access
- Set up logging infrastructure for monitoring and debugging

### 2. S3 Integration Architecture
- Input bucket: `healthcare-facility-data/input/`
- Output bucket: `healthcare-facility-data/filtered/`

### 3. Core Application Design
- Object-oriented design with `HealthcareFacilityProcessor` class
- Modular methods for reading, processing, and writing data
- Comprehensive error handling and logging
- Support for both JSON array and JSON Lines formats

### 4. Business Logic Implementation
```python
def is_expiring_soon(self, expiry_date_str, months_threshold=6):
    expiry_date = parse(expiry_date_str).date()
    current_date = datetime.now().date()
    threshold_date = (datetime.now() + timedelta(days=months_threshold * 30)).date()
    
    
    return current_date <= expiry_date <= threshold_date
```
### Metadata Enhancement
- **Challenge**: Provide processing context for filtered results
- **Solution**: Added `_processing_metadata` to each filtered facility
- **Includes**:
  - Processing timestamp
  - List of expiring accreditations with details
  - Total accreditation count for context