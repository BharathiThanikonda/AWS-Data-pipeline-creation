import os
from datetime import datetime

# S3 Configuration
S3_CONFIG = {
    'input_bucket': os.getenv('INPUT_BUCKET', 'healthcare-facility-data'),
    'output_bucket': os.getenv('OUTPUT_BUCKET', 'healthcare-facility-data'),
    'input_prefix': 'input/',
    'output_prefix': 'filtered/',
    'region': os.getenv('AWS_REGION', 'us-east-1')
}

# Processing Configuration
PROCESSING_CONFIG = {
    'expiry_threshold_months': 6,
    'batch_size': 100,  # For future batching if needed
    'max_retries': 3
}

# Logging Configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'log_file': f'facility_processing_{datetime.now().strftime("%Y%m%d")}.log',
    'max_log_size': 10 * 1024 * 1024,  # 10MB
    'backup_count': 5
}