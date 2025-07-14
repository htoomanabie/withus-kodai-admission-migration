# School Staff Error Log Analysis Report

## Executive Summary

The school staff migration process encountered **5,706 failed records** due to missing foreign key references. All failures are related to the same root cause: **missing school records** in the target system.

## Error Pattern Analysis

### Primary Error Type
- **Error Pattern**: `INVALID_FIELD: Foreign key external ID: [ID] not found for field MANAERP__School_Partner_Id__c in entity MANAERP__School__c []`
- **Root Cause**: School staff records reference school IDs that don't exist in the `MANAERP__School__c` entity
- **Impact**: 100% of failed records (5,706/5,706) are due to this issue

### Missing School IDs Distribution
- **Total unique missing school IDs**: 951
- **Most affected school IDs**:
  - School ID 528: 55 failures (1.0%)
  - School ID 503: 52 failures (0.9%)
  - School ID 495: 48 failures (0.8%)
  - School ID 551: 47 failures (0.8%)
  - School ID 527: 46 failures (0.8%)

## Data Structure Analysis

### CSV Columns
- `FirstName`, `LastName`: Staff member names (masked)
- `School_Staff_Role__c`: Role/title of the staff member
- `Active__c`: Whether the staff member is currently active
- `Description`: Additional notes about the staff member
- `MANAERP__Current_School__r:MANAERP__School__c:MANAERP__School_Partner_Id__c`: School ID reference (causing the error)
- `School_Staff_External_Id__c`: Unique identifier for the staff member
- `__Status`: All records show "Failed"
- `__Errors`: Error message details

### Staff Role Distribution in Failed Records
1. **教頭** (Vice Principal): 287 records (5.0%)
2. **教諭** (Teacher): 240 records (4.2%)
3. **教務主任** (Academic Affairs Director): 104 records (1.8%)
4. **進路指導部** (Career Guidance Department): 68 records (1.2%)
5. **進路指導主事** (Career Guidance Director): 57 records (1.0%)
6. **養護教諭** (School Nurse): 57 records (1.0%)
7. **校長** (Principal): 53 records (0.9%)
8. **教務担当** (Academic Affairs Staff): 53 records (0.9%)
9. **事務** (Administrative Staff): 51 records (0.9%)
10. **進路指導** (Career Guidance): 47 records (0.8%)

## Key Findings

### 1. Systematic Data Dependency Issue
- The migration process attempted to create school staff records before ensuring all referenced schools exist
- This indicates a **migration order problem** - schools should be migrated before school staff

### 2. Large Scale Impact
- 951 different schools are missing from the target system
- This suggests either:
  - Schools were not migrated at all
  - School IDs were not properly mapped between source and target systems
  - School migration failed for these specific records

### 3. Role Distribution
- The failed records represent a cross-section of all school staff roles
- No specific role type is more affected than others, indicating this is a systemic issue

## Recommendations

### Immediate Actions
1. **Verify School Migration Status**
   - Check if school records exist in the `MANAERP__School__c` entity
   - Identify which schools were successfully migrated vs. failed

2. **Review Migration Order**
   - Ensure schools are migrated before school staff
   - Implement proper dependency checks in the migration process

3. **Validate School ID Mapping**
   - Compare school IDs between source and target systems
   - Verify the mapping logic is correct

### Long-term Solutions
1. **Create Missing School Records**
   - Migrate the 951 missing schools first
   - Then retry the school staff migration

2. **Update School Staff References**
   - If school IDs have changed, update the references in school staff records
   - Ensure proper foreign key relationships

3. **Implement Migration Validation**
   - Add pre-migration checks to verify all dependencies exist
   - Create rollback procedures for failed migrations

## Technical Details

### Error Message Breakdown
```
INVALID_FIELD: Foreign key external ID: [SCHOOL_ID] not found for field MANAERP__School_Partner_Id__c in entity MANAERP__School__c []
```

- **Field**: `MANAERP__School_Partner_Id__c` (the foreign key field)
- **Entity**: `MANAERP__School__c` (the referenced entity)
- **Issue**: The school ID doesn't exist in the target system

### Data Quality Observations
- Staff names appear to be masked/anonymized
- Role descriptions are in Japanese
- External IDs are properly formatted
- All other data fields appear to be valid

## Next Steps

1. **Audit School Migration**: Review the school migration logs to understand why 951 schools are missing
2. **Fix School Data**: Migrate or create the missing school records
3. **Re-run Staff Migration**: Once schools are in place, retry the school staff migration
4. **Monitor Results**: Track success rates after fixing the dependency issue

---

*Report generated from analysis of `/Users/htoomaung/Repository/withus-kodai-admission-migration/processed/rehearsal3/log/school-staff-fail-log.csv`* 