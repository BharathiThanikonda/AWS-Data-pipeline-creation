import boto3
import json
import logging
from datetime import datetime, timedelta
from dateutil.parser import parse
from botocore.exceptions import ClientError, NoCredentialsError
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('facility_processor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class HealthcareFacilityProcessor:
    def __init__(self, input_bucket, output_bucket, input_key='input/', output_key='filtered/'):
        """
        Initialize the processor with S3 bucket configurations
        
        Args:
            input_bucket (str): Name of the input S3 bucket
            output_bucket (str): Name of the output S3 bucket (can be same as input)
            input_key (str): S3 key/folder for input files
            output_key (str): S3 key/folder for output files
        """
        try:
            self.s3_client = boto3.client('s3')
            self.input_bucket = input_bucket
            self.output_bucket = output_bucket
            self.input_key = input_key
            self.output_key = output_key
            
            # Test AWS connection
            self.s3_client.head_bucket(Bucket=input_bucket)
            logger.info("Successfully connected to AWS S3")
            
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure your credentials.")
            raise
        except ClientError as e:
            logger.error(f"Error connecting to S3: {e}")
            raise

    def read_json_from_s3(self, file_key):
        """
        Read JSON data from S3
        
        Args:
            file_key (str): S3 key for the file to read
            
        Returns:
            list: List of facility records
        """
        try:
            logger.info(f"Reading file from s3://{self.input_bucket}/{file_key}")
            
            response = self.s3_client.get_object(
                Bucket=self.input_bucket,
                Key=file_key
            )
            
            content = response['Body'].read().decode('utf-8')
            
           
            facilities = []
            
           
            try:
                facilities = json.loads(content)
                logger.info(f"Successfully parsed JSON array with {len(facilities)} facilities")
            except json.JSONDecodeError:
                # Try JSON Lines format
                logger.info("JSON array parsing failed, trying JSON Lines format")
                for line_num, line in enumerate(content.strip().split('\n'), 1):
                    if line.strip():
                        try:
                            facility = json.loads(line)
                            facilities.append(facility)
                        except json.JSONDecodeError as e:
                            logger.warning(f"Skipping invalid JSON on line {line_num}: {e}")
                
                logger.info(f"Successfully parsed JSON Lines with {len(facilities)} facilities")
            
            return facilities
            
        except ClientError as e:
            logger.error(f"Error reading from S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error reading JSON: {e}")
            raise

    def is_expiring_soon(self, expiry_date_str, months_threshold=6):
        """
        Check if an accreditation is expiring within the threshold period
        
        Args:
            expiry_date_str (str): Date string in format 'YYYY-MM-DD'
            months_threshold (int): Number of months to check ahead
            
        Returns:
            bool: True if expiring soon, False otherwise
        """
        try:
            expiry_date = parse(expiry_date_str).date()
            current_date = datetime.now().date()
            threshold_date = (datetime.now() + timedelta(days=months_threshold * 30)).date()
            
            
            return current_date <= expiry_date <= threshold_date
            
        except Exception as e:
            logger.warning(f"Error parsing date '{expiry_date_str}': {e}")
            return False

    def filter_expiring_facilities(self, facilities):
        """
        Filter facilities with accreditations expiring within 6 months
        
        Args:
            facilities (list): List of facility records
            
        Returns:
            list: Filtered list of facilities with expiring accreditations
        """
        filtered_facilities = []
        
        for facility in facilities:
            try:
                facility_id = facility.get('facility_id', 'Unknown')
                facility_name = facility.get('facility_name', 'Unknown')
                accreditations = facility.get('accreditations', [])
                
                if not accreditations:
                    logger.info(f"Facility {facility_id} has no accreditations, skipping")
                    continue
                
                has_expiring_accreditation = False
                expiring_accreditations = []
                
                for accreditation in accreditations:
                    expiry_date = accreditation.get('valid_until', '')
                    if self.is_expiring_soon(expiry_date):
                        has_expiring_accreditation = True
                        expiring_accreditations.append({
                            'body': accreditation.get('accreditation_body', ''),
                            'expiry': expiry_date
                        })
                
                if has_expiring_accreditation:
                   
                    facility_copy = facility.copy()
                    facility_copy['_processing_metadata'] = {
                        'processed_date': datetime.now().isoformat(),
                        'expiring_accreditations': expiring_accreditations,
                        'total_accreditations': len(accreditations)
                    }
                    
                    filtered_facilities.append(facility_copy)
                    logger.info(f"Facility {facility_id} ({facility_name}) has {len(expiring_accreditations)} expiring accreditation(s)")
                
            except Exception as e:
                logger.error(f"Error processing facility {facility.get('facility_id', 'Unknown')}: {e}")
                continue
        
        logger.info(f"Filtered {len(filtered_facilities)} facilities with expiring accreditations out of {len(facilities)} total")
        return filtered_facilities

    def write_json_to_s3(self, data, output_filename):
        """
        Write JSON data to S3
        
        Args:
            data (list): List of facility records to write
            output_filename (str): Name of the output file
        """
        try:
            output_key = f"{self.output_key}{output_filename}"
            
          
            json_content = json.dumps(data, indent=2, ensure_ascii=False)
            
          
            self.s3_client.put_object(
                Bucket=self.output_bucket,
                Key=output_key,
                Body=json_content.encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"Successfully wrote {len(data)} filtered records to s3://{self.output_bucket}/{output_key}")
            
            
            summary = {
                'processing_date': datetime.now().isoformat(),
                'total_facilities_processed': len(data),
                'output_location': f"s3://{self.output_bucket}/{output_key}",
                'filter_criteria': 'Accreditations expiring within 6 months',
                'facilities_summary': [
                    {
                        'facility_id': facility['facility_id'],
                        'facility_name': facility['facility_name'],
                        'expiring_count': len(facility.get('_processing_metadata', {}).get('expiring_accreditations', []))
                    }
                    for facility in data
                ]
            }
            
            summary_key = f"{self.output_key}processing_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            self.s3_client.put_object(
                Bucket=self.output_bucket,
                Key=summary_key,
                Body=json.dumps(summary, indent=2).encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"Created processing summary at s3://{self.output_bucket}/{summary_key}")
            
        except ClientError as e:
            logger.error(f"Error writing to S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error writing JSON: {e}")
            raise

    def list_input_files(self):
        """
        List all JSON files in the input S3 location
        
        Returns:
            list: List of S3 keys for JSON files
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.input_bucket,
                Prefix=self.input_key
            )
            
            json_files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.endswith('.json') and key != self.input_key:
                        json_files.append(key)
            
            logger.info(f"Found {len(json_files)} JSON files in input location")
            return json_files
            
        except ClientError as e:
            logger.error(f"Error listing files: {e}")
            raise

    def process_all_files(self):
        """
        Main processing method - processes all JSON files in input location
        """
        try:
            logger.info("Starting healthcare facility processing")
            
           
            input_files = self.list_input_files()
            
            if not input_files:
                logger.warning("No JSON files found in input location")
                return
            
            all_filtered_facilities = []
            
          
            for file_key in input_files:
                try:
                    logger.info(f"Processing file: {file_key}")
                    
                    
                    facilities = self.read_json_from_s3(file_key)
                    
                  
                    filtered = self.filter_expiring_facilities(facilities)
                    
                    all_filtered_facilities.extend(filtered)
                    
                except Exception as e:
                    logger.error(f"Error processing file {file_key}: {e}")
                    continue
            
            if all_filtered_facilities:
               
                output_filename = f"expiring_facilities_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                self.write_json_to_s3(all_filtered_facilities, output_filename)
                
                logger.info(f"Processing complete. Found {len(all_filtered_facilities)} facilities with expiring accreditations")
            else:
                logger.info("No facilities found with expiring accreditations")
                
                
                empty_result = {
                    'message': 'No facilities with expiring accreditations found',
                    'processing_date': datetime.now().isoformat(),
                    'facilities': []
                }
                self.write_json_to_s3([empty_result], f"no_expiring_facilities_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            
        except Exception as e:
            logger.error(f"Fatal error in processing: {e}")
            raise


def main():
    """
    Main execution function
    """
    try:
        
        INPUT_BUCKET = "healthcare-facility-data"  
        OUTPUT_BUCKET = "healthcare-facility-data"  
        
        # Initialize processor
        processor = HealthcareFacilityProcessor(
            input_bucket=INPUT_BUCKET,
            output_bucket=OUTPUT_BUCKET,
            input_key="input/",
            output_key="filtered/"
        )
        
        # Process all files
        processor.process_all_files()
        
        logger.info("Healthcare facility processing completed successfully")
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()