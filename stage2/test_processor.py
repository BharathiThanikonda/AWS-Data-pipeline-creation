import unittest
from datetime import datetime, timedelta
from healthcare_facility_processor import HealthcareFacilityProcessor
import json

class TestHealthcareFacilityProcessor(unittest.TestCase):
    
    def setUp(self):
        # Mock data for testing
        self.sample_facilities = [
            {
                "facility_id": "FAC001",
                "facility_name": "Test Hospital",
                "employee_count": 100,
                "accreditations": [
                    {
                        "accreditation_body": "Joint Commission",
                        "valid_until": (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')  # Expires in 1 month
                    }
                ]
            },
            {
                "facility_id": "FAC002", 
                "facility_name": "Safe Clinic",
                "employee_count": 50,
                "accreditations": [
                    {
                        "accreditation_body": "NCQA",
                        "valid_until": (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')  # Expires in 1 year
                    }
                ]
            }
        ]
    
    def test_expiry_check(self):
        processor = HealthcareFacilityProcessor("test", "test")
        
        # Test expiring soon (30 days)
        soon_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.assertTrue(processor.is_expiring_soon(soon_date))
        
        # Test not expiring soon (1 year)
        far_date = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
        self.assertFalse(processor.is_expiring_soon(far_date))
    
    def test_filter_facilities(self):
        processor = HealthcareFacilityProcessor("test", "test")
        filtered = processor.filter_expiring_facilities(self.sample_facilities)
        
        # Should only return facilities with expiring accreditations
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]['facility_id'], 'FAC001')

if __name__ == '__main__':
    unittest.main()