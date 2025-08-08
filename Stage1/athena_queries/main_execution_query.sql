SELECT 
    facility_id,
    facility_name,
    employee_count,
    cardinality(services) as number_of_offered_services,
    (
        SELECT MIN(date_parse(acc.valid_until, '%Y-%m-%d'))
        FROM UNNEST(accreditations) as t(acc)
    ) as expiry_date_of_first_accreditation
FROM healthcare_analytics.facilities
ORDER BY facility_id;