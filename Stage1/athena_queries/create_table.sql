CREATE EXTERNAL TABLE facilities (
  facility_id string,
  facility_name string,
  location struct<
    address: string,
    city: string,
    state: string,
    zip: string
  >,
  employee_count int,
  services array<string>,
  labs array<struct<
    lab_name: string,
    certifications: array<string>
  >>,
  accreditations array<struct<
    accreditation_body: string,
    accreditation_id: string,
    valid_until: string
  >>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3://healthcare-facility-data/input/';