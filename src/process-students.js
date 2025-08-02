import { promises as fs } from 'fs';
import Papa from 'papaparse';
import _ from 'lodash';
import { fixNextLine } from './fix-line.js';
import { maskData } from './mask-data.js';
import { verifyMaskingIntegrity } from './verify-masking-integrity.js';

// Import mappings and helper functions
import {
    REQUIRED_COLUMNS,
    getFieldValue,
    transformPrefecture,
    transformDmSendable, 
    transformGrade,
    transformCourseType,
    transformOperateType,
    transformSex,
    transformBranchId,
    transformPhone,
    getOutputColumnNames,
    COLUMN_MAPPINGS
} from './student-mappings.js';

// Configuration
const CHUNK_SIZE = 5000; // Smaller chunk size to reduce memory pressure
const TAG_VALUE = "phase22"; // Configurable tag value - can be changed or removed in final migration
const ADD_TAG_COLUMN = TAG_VALUE !== ""; // Flag to determine if tag column should be added
const COUNTER_START = 30000001; // Starting value for the counter column. check from `SELECT Id, MANAERP__Username_Count__c FROM MANAERP__Contact_Username_Counter__c`

// Helper function to create mapping statistics tracker
function createMappingStatsTracker() {
    return {
        mappings: new Map(),           // original_value -> { mapped: 'mapped_value', count: number }
        finalMappedValues: new Map()   // mapped_value -> total_count
    };
}

// Helper function to track mapping statistics
function trackMappingStats(originalValue, transformedValue, statsTracker, columnName = '') {
    if (!statsTracker) return transformedValue;
    
    const originalKey = String(originalValue || '');
    const mappedValue = String(transformedValue);
    
    // Track original -> mapped mappings
    if (statsTracker.mappings.has(originalKey)) {
        statsTracker.mappings.get(originalKey).count++;
    } else {
        statsTracker.mappings.set(originalKey, {
            mapped: mappedValue,
            count: 1,
            columnName: columnName
        });
    }
    
    // Track final mapped values
    statsTracker.finalMappedValues.set(mappedValue, 
        (statsTracker.finalMappedValues.get(mappedValue) || 0) + 1);
    
    return transformedValue;
}

// Helper function to display mapping statistics
function displayMappingStats(statsTracker, columnTitle, processedCount) {
    if (!statsTracker || statsTracker.mappings.size === 0) {
        console.log(`   No ${columnTitle.toLowerCase()} data processed`);
        return;
    }
    
    console.log(`🔄 ${columnTitle} Mappings (Original → Mapped → Count):`);
    Array.from(statsTracker.mappings.entries())
        .sort(([,a], [,b]) => b.count - a.count) // Sort by count descending
        .forEach(([original, {mapped, count}]) => {
            const displayOriginal = original === '' ? '(empty)' : `"${original}"`;
            console.log(`   ├── ${displayOriginal} → "${mapped}" → ${count} records`);
        });
    
    console.log(`\n📊 Final ${columnTitle} Distribution:`);
    Array.from(statsTracker.finalMappedValues.entries())
        .sort(([,a], [,b]) => b - a) // Sort by count descending
        .forEach(([mappedValue, count]) => {
            const displayValue = mappedValue === '' ? '(empty)' : `"${mappedValue}"`;
            const percentage = ((count / processedCount) * 100).toFixed(1);
            console.log(`   ├── ${displayValue} → ${count} records (${percentage}%)`);
        });
}

// Helper function to create mapping stats collection for easy extensibility
function createAllMappingStats() {
    return {
        dmSendable: createMappingStatsTracker(),
        sex: createMappingStatsTracker(),
        grade: createMappingStatsTracker(),
        courseType: createMappingStatsTracker(),
        // Easy to add new column tracking:
        // prefecture: createMappingStatsTracker(),
        // branchId: createMappingStatsTracker(),
        // dmSendable2: createMappingStatsTracker(),
    };
}

// Helper function to display all mapping statistics
function displayAllMappingStats(allMappingStats, processedCount) {
    const statsConfig = [
        { key: 'dmSendable', title: 'DM Sendable', icon: '📧' },
        { key: 'sex', title: 'Sex', icon: '👤' },
        { key: 'grade', title: 'Grade', icon: '🎓' },
        { key: 'courseType', title: 'Course Type', icon: '📚' },
        // Easy to add new column display:
        // { key: 'prefecture', title: 'Prefecture', icon: '🗾' },
        // { key: 'branchId', title: 'Branch ID', icon: '🏢' },
    ];
    
    statsConfig.forEach(({ key, title, icon }) => {
        console.log(`\n${icon} ${title} Processing Statistics:`);
        displayMappingStats(allMappingStats[key], title, processedCount);
    });
}

// Helper function to create data quality tracker
function createDataQualityTracker() {
    return {
        totalRecords: 0,
        totalFields: 0,
        issuesByColumn: new Map(), // column_name -> { original: {}, output: {} }
        overallStats: {
            original: {
                emptyValues: 0,
                nullValues: 0,
                invalidValues: 0,
                validValues: 0
            },
            output: {
                emptyValues: 0,
                nullValues: 0,
                invalidValues: 0,
                validValues: 0
            }
        }
    };
}

// Helper function to determine if a value is invalid based on column type
function isInvalidValue(columnName, originalValue, transformedValue) {
    if (!originalValue && originalValue !== 0) return false; // Skip null/empty checks here
    
    const strValue = String(originalValue);
    
    // Date validation
    if (columnName.includes('date') || columnName.includes('Date') || columnName === 'birthday') {
        if (strValue !== '' && transformedValue === strValue) {
            // If transformation didn't change the value, check if it's a valid date
            const date = new Date(strValue);
            return isNaN(date.getTime());
        }
    }
    
    // Email validation
    if (columnName.includes('email')) {
        if (strValue !== '') {
            // Basic email pattern check
            return !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(strValue);
        }
    }
    
    return false;
}

// Helper function to track data quality for a single field
function trackDataQuality(columnName, originalValue, transformedValue, qualityTracker) {
    if (!qualityTracker) return transformedValue;
    
    // Skip tracking MANAERP__Username__c as it's auto-generated
    if (columnName === 'MANAERP__Username__c') {
        return transformedValue;
    }
    
    qualityTracker.totalFields++;
    
    // Initialize column tracking if not exists
    if (!qualityTracker.issuesByColumn.has(columnName)) {
        qualityTracker.issuesByColumn.set(columnName, {
            original: {
                empty: 0,
                null: 0,
                invalid: 0,
                valid: 0,
                total: 0
            },
            output: {
                empty: 0,
                null: 0,
                invalid: 0,
                valid: 0,
                total: 0
            }
        });
    }
    
    const columnStats = qualityTracker.issuesByColumn.get(columnName);
    
    // Track original value quality
    columnStats.original.total++;
    if (originalValue === null || originalValue === undefined) {
        qualityTracker.overallStats.original.nullValues++;
        columnStats.original.null++;
    } else if (originalValue === '' || (typeof originalValue === 'string' && originalValue.trim() === '')) {
        qualityTracker.overallStats.original.emptyValues++;
        columnStats.original.empty++;
    } else if (isInvalidValue(columnName, originalValue, originalValue)) {
        qualityTracker.overallStats.original.invalidValues++;
        columnStats.original.invalid++;
    } else {
        qualityTracker.overallStats.original.validValues++;
        columnStats.original.valid++;
    }
    
    // Track output value quality  
    columnStats.output.total++;
    if (transformedValue === null || transformedValue === undefined) {
        qualityTracker.overallStats.output.nullValues++;
        columnStats.output.null++;
    } else if (transformedValue === '' || (typeof transformedValue === 'string' && transformedValue.trim() === '')) {
        qualityTracker.overallStats.output.emptyValues++;
        columnStats.output.empty++;
    } else if (isInvalidValue(columnName, transformedValue, transformedValue)) {
        qualityTracker.overallStats.output.invalidValues++;
        columnStats.output.invalid++;
    } else {
        qualityTracker.overallStats.output.validValues++;
        columnStats.output.valid++;
    }
    
    return transformedValue;
}

// Helper function to display data quality statistics
function displayDataQualityStats(qualityTracker, processedCount) {
    if (!qualityTracker) {
        console.log('   No data quality tracking available');
        return;
    }
    
    console.log('\n🔍 Data Quality Analysis (Original vs Output):');
    console.log(`📊 Overall Statistics:`);
    console.log(`   ├── Total records processed: ${qualityTracker.totalRecords || processedCount}`);
    console.log(`   ├── Total fields processed: ${qualityTracker.totalFields} (excluding MANAERP__Username__c)`);
    
    // Show original vs output comparison
    const originalStats = qualityTracker.overallStats.original;
    const outputStats = qualityTracker.overallStats.output;
    const totalOriginal = originalStats.validValues + originalStats.emptyValues + originalStats.nullValues + originalStats.invalidValues;
    const totalOutput = outputStats.validValues + outputStats.emptyValues + outputStats.nullValues + outputStats.invalidValues;
    
    console.log(`   ├── Original → Output Comparison:`);
    console.log(`   │   ├── Valid: ${originalStats.validValues} (${((originalStats.validValues/totalOriginal)*100).toFixed(1)}%) → ${outputStats.validValues} (${((outputStats.validValues/totalOutput)*100).toFixed(1)}%)`);
    console.log(`   │   ├── Empty: ${originalStats.emptyValues} (${((originalStats.emptyValues/totalOriginal)*100).toFixed(1)}%) → ${outputStats.emptyValues} (${((outputStats.emptyValues/totalOutput)*100).toFixed(1)}%)`);
    console.log(`   │   ├── Null: ${originalStats.nullValues} (${((originalStats.nullValues/totalOriginal)*100).toFixed(1)}%) → ${outputStats.nullValues} (${((outputStats.nullValues/totalOutput)*100).toFixed(1)}%)`);
    console.log(`   │   └── Invalid: ${originalStats.invalidValues} (${((originalStats.invalidValues/totalOriginal)*100).toFixed(1)}%) → ${outputStats.invalidValues} (${((outputStats.invalidValues/totalOutput)*100).toFixed(1)}%)`);
    
    // Show transformation improvement
    const originalIssues = originalStats.emptyValues + originalStats.nullValues + originalStats.invalidValues;
    const outputIssues = outputStats.emptyValues + outputStats.nullValues + outputStats.invalidValues;
    const improvement = originalIssues - outputIssues;
    const improvementPercent = originalIssues > 0 ? ((improvement / originalIssues) * 100).toFixed(1) : '0.0';
    
    console.log(`   └── Data Quality Improvement: ${improvement} issues fixed (${improvementPercent}% improvement)`);
    
    // Show columns with most original issues and their transformation results
    const columnsWithOriginalIssues = Array.from(qualityTracker.issuesByColumn.entries())
        .map(([column, stats]) => ({
            column,
            originalIssues: stats.original.empty + stats.original.null + stats.original.invalid,
            outputIssues: stats.output.empty + stats.output.null + stats.output.invalid,
            originalTotal: stats.original.total,
            outputTotal: stats.output.total,
            improvement: (stats.original.empty + stats.original.null + stats.original.invalid) - (stats.output.empty + stats.output.null + stats.output.invalid),
            originalStats: stats.original,
            outputStats: stats.output
        }))
        .filter(item => item.originalIssues > 0)
        .sort((a, b) => b.originalIssues - a.originalIssues);
    
    if (columnsWithOriginalIssues.length > 0) {
        console.log(`\n⚠️  Columns with Original Data Quality Issues → Transformation Results (All ${columnsWithOriginalIssues.length} columns):`);
        columnsWithOriginalIssues.forEach((item, index) => {
            const symbol = index === columnsWithOriginalIssues.length - 1 ? '└──' : '├──';
            const improvementText = item.improvement > 0 ? `✅ Fixed ${item.improvement}` : item.improvement < 0 ? `❌ Added ${Math.abs(item.improvement)}` : '➖ No change';
            console.log(`   ${symbol} ${item.column}: ${item.originalIssues}→${item.outputIssues} issues (${improvementText})`);
            
            // Show detailed breakdown
            console.log(`       ├── Original → Null: ${item.originalStats.null}, Empty: ${item.originalStats.empty}, Invalid: ${item.originalStats.invalid}`);
            console.log(`       └── Output   → Null: ${item.outputStats.null}, Empty: ${item.outputStats.empty}, Invalid: ${item.outputStats.invalid}`);
        });
    }
    
    // Show columns with perfect output quality (no null, empty, or invalid values)
    const perfectOutputColumns = Array.from(qualityTracker.issuesByColumn.entries())
        .filter(([, stats]) => stats.output.empty + stats.output.null + stats.output.invalid === 0)
        .map(([column]) => column)
        .sort(); // Sort alphabetically for better readability
    
    if (perfectOutputColumns.length > 0) {
        console.log(`\n✨ Columns with Perfect Output Quality (${perfectOutputColumns.length} columns - No null, empty, or invalid values):`);
        perfectOutputColumns.forEach((column, index) => {
            const symbol = index === perfectOutputColumns.length - 1 ? '└──' : '├──';
            console.log(`   ${symbol} ${column}`);
        });
    } else {
        console.log(`\n⚠️  No columns with perfect output quality found`);
    }
    
    // Show columns with most improvement
    const mostImproved = columnsWithOriginalIssues
        .filter(item => item.improvement > 0)
        .sort((a, b) => b.improvement - a.improvement)
        .slice(0, 5);
    
    if (mostImproved.length > 0) {
        console.log(`\n🚀 Most Improved Columns:`);
        mostImproved.forEach((item, index) => {
            const symbol = index === mostImproved.length - 1 ? '└──' : '├──';
            console.log(`   ${symbol} ${item.column}: Fixed ${item.improvement} issues (${((item.improvement/item.originalIssues)*100).toFixed(1)}% improvement)`);
        });
    }
}

// Helper function to safely escape CSV values
function escapeCSVValue(value) {
    if (value === null || value === undefined) {
        return '""';
    }
    
    if (typeof value !== 'string') {
        // For non-string values (numbers, booleans), convert to string first
        return String(value);
    }
    
    // Clean up malformed data from input CSV
    let cleanedValue = value
        // Remove any leading/trailing quotes that might cause issues
        .replace(/^"+|"+$/g, '')
        // Clean up excessive whitespace and special characters
        .replace(/\s+/g, ' ')           // Normalize multiple spaces to single space
        .trim();                        // Remove leading/trailing whitespace
    
    // Escape quotes, newlines, tabs, and carriage returns for CSV safety
    const escapedValue = cleanedValue
        .replace(/"/g, '""')      // Escape quotes (must be first)
        .replace(/\r\n/g, ' ')    // Replace CRLF with space
        .replace(/\n/g, ' ')      // Replace LF with space
        .replace(/\r/g, ' ')      // Replace CR with space
        .replace(/\t/g, ' ')      // Replace tabs with space
        .replace(/　/g, ' ');     // Replace full-width spaces with regular space
    
    return `"${escapedValue}"`;
}

// Function to remove dashes from a string
function removeDashes(str) {
    if (!str) return str;
    return String(str).replace(/-/g, '');
}

// Date formatting function - ensures consistent YYYY-MM-DD format
function formatDate(dateValue) {
    if (!dateValue) return '';
    
    try {
        const date = new Date(dateValue);
        if (isNaN(date.getTime())) return dateValue;

        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        
        return `${year}-${month}-${day}`;
    } catch (error) {
        console.warn(`Warning: Could not format date ${dateValue}`);
        return dateValue;
    }
}

// Function to get the last day of the month for a given date
function getLastDayOfMonth(dateValue) {
    if (!dateValue) return '';
    
    try {
        const date = new Date(dateValue);
        if (isNaN(date.getTime())) return dateValue;

        const year = date.getFullYear();
        const month = date.getMonth(); // 0-based month
        
        // Create date with last day of the month (day 0 of next month)
        const lastDayDate = new Date(year, month + 1, 0);
        
        const lastDay = String(lastDayDate.getDate()).padStart(2, '0');
        const monthStr = String(month + 1).padStart(2, '0');
        
        return `${year}-${monthStr}-${lastDay}`;
    } catch (error) {
        console.warn(`Warning: Could not calculate last day of month for ${dateValue}`);
        return formatDate(dateValue); // Fallback to regular formatting
    }
}

// Function to verify phone number logic compliance
function verifyPhoneNumberLogic(record, derivedPhones, counters) {
    if (!counters || !counters.phoneRuleStats) return;
    
    const tel = getFieldValue(record, 'tel');
    const portableTel = getFieldValue(record, 'portable_tel');
    
    // Check if values are truly NULL (not just empty or '0')
    const telIsNotNull = tel !== undefined && tel !== null && tel !== '';
    const portableTelIsNotNull = portableTel !== undefined && portableTel !== null && portableTel !== '';
    
    const finalPhone = derivedPhones.phone;
    const finalOtherPhone = derivedPhones.other_phone;
    
    // Rule 1: tel is not NULL AND portable_tel is not NULL → portable_tel is Phone and tel is Other Phone
    if (telIsNotNull && portableTelIsNotNull) {
        counters.phoneRuleStats.rule1_both_exist++;
        const expectedPhone = removeDashes(portableTel);
        const expectedOtherPhone = removeDashes(tel);
        if (finalPhone === expectedPhone && finalOtherPhone === expectedOtherPhone) {
            counters.phoneRuleStats.rule1_compliant++;
        } else {
            counters.phoneRuleStats.rule1_non_compliant++;
        }
    }
    // Rule 2: tel is not NULL AND portable_tel is NULL → tel is Phone
    else if (telIsNotNull && !portableTelIsNotNull) {
        counters.phoneRuleStats.rule2_tel_only++;
        const expectedPhone = removeDashes(tel);
        if (finalPhone === expectedPhone && (finalOtherPhone === null || finalOtherPhone === '')) {
            counters.phoneRuleStats.rule2_compliant++;
        } else {
            counters.phoneRuleStats.rule2_non_compliant++;
        }
    }
    // Rule 3: tel is NULL AND portable_tel is not NULL → portable_tel is Phone
    else if (!telIsNotNull && portableTelIsNotNull) {
        counters.phoneRuleStats.rule3_portable_only++;
        const expectedPhone = removeDashes(portableTel);
        if (finalPhone === expectedPhone && (finalOtherPhone === null || finalOtherPhone === '')) {
            counters.phoneRuleStats.rule3_compliant++;
        } else {
            counters.phoneRuleStats.rule3_non_compliant++;
        }
    }
    // Rule 4: tel is NULL AND portable_tel is NULL → both are NULL
    else if (!telIsNotNull && !portableTelIsNotNull) {
        counters.phoneRuleStats.rule4_both_null++;
        if ((finalPhone === null || finalPhone === '') && (finalOtherPhone === null || finalOtherPhone === '')) {
            counters.phoneRuleStats.rule4_compliant++;
        } else {
            counters.phoneRuleStats.rule4_non_compliant++;
        }
    }
}

// Function to verify email logic compliance
function verifyEmailLogic(record, derivedEmails, counters) {
    if (!counters || !counters.emailRuleStats) return;
    
    const email = getFieldValue(record, 'email');
    const portableEmail = getFieldValue(record, 'portable_email');
    
    // Check if values are truly NULL (not just empty)
    const emailIsNotNull = email !== undefined && email !== null && email !== '';
    const portableEmailIsNotNull = portableEmail !== undefined && portableEmail !== null && portableEmail !== '';
    
    const finalMainEmail = derivedEmails.main_email;
    const finalSubEmail = derivedEmails.sub_email;
    
    // Rule 1: email is not NULL AND portable_email is not NULL → portable_email is Main Email and email is Sub Email
    if (emailIsNotNull && portableEmailIsNotNull) {
        counters.emailRuleStats.rule1_both_exist++;
        if (finalMainEmail === portableEmail && finalSubEmail === email) {
            counters.emailRuleStats.rule1_compliant++;
        } else {
            counters.emailRuleStats.rule1_non_compliant++;
        }
    }
    // Rule 2: email is not NULL AND portable_email is NULL → email is Main Email
    else if (emailIsNotNull && !portableEmailIsNotNull) {
        counters.emailRuleStats.rule2_email_only++;
        if (finalMainEmail === email && (finalSubEmail === null || finalSubEmail === '')) {
            counters.emailRuleStats.rule2_compliant++;
        } else {
            counters.emailRuleStats.rule2_non_compliant++;
        }
    }
    // Rule 3: email is NULL AND portable_email is not NULL → portable_email is Main Email
    else if (!emailIsNotNull && portableEmailIsNotNull) {
        counters.emailRuleStats.rule3_portable_only++;
        if (finalMainEmail === portableEmail && (finalSubEmail === null || finalSubEmail === '')) {
            counters.emailRuleStats.rule3_compliant++;
        } else {
            counters.emailRuleStats.rule3_non_compliant++;
        }
    }
    // Rule 4: email is NULL AND portable_email is NULL → both are NULL
    else if (!emailIsNotNull && !portableEmailIsNotNull) {
        counters.emailRuleStats.rule4_both_null++;
        if ((finalMainEmail === null || finalMainEmail === '') && (finalSubEmail === null || finalSubEmail === '')) {
            counters.emailRuleStats.rule4_compliant++;
        } else {
            counters.emailRuleStats.rule4_non_compliant++;
        }
    }
}

// Transform and derive phone numbers
function derivePhoneNumbers(record) {
    // Use the helper function to check all possible column names
    const tel = getFieldValue(record, 'tel');
    const portableTel = getFieldValue(record, 'portable_tel');
    
    let phone = null;
    let otherPhone = null;

    // Use explicit existence checks rather than truthiness
    const hasTel = tel !== undefined && tel !== null && tel !== '' && tel !== '0';
    const hasPortableTel = portableTel !== undefined && portableTel !== null && portableTel !== '' && portableTel !== '0';

    // Both numbers exist
    if (hasTel && hasPortableTel) {
        phone = removeDashes(portableTel);
        otherPhone = removeDashes(tel);
    }
    // Only tel exists
    else if (hasTel) {
        phone = removeDashes(tel);
    }
    // Only portable_tel exists
    else if (hasPortableTel) {
        phone = removeDashes(portableTel);
    }
    // Check if phone or other_phone already exist
    else {
        if (record.phone !== undefined && record.phone !== null && record.phone !== '' && record.phone !== '0') {
            phone = record.phone;
        }
        if (record.other_phone !== undefined && record.other_phone !== null && record.other_phone !== '' && record.other_phone !== '0') {
            otherPhone = record.other_phone;
        }
    }

    return {
        phone,
        other_phone: otherPhone
    };
}

// Transform and derive email addresses
function deriveEmails(record) {
    // Use the helper function to check all possible column names
    const email = getFieldValue(record, 'email');
    const portableEmail = getFieldValue(record, 'portable_email');
    
    let mainEmail = null;
    let subEmail = null;

    // Important fix: Use explicit existence checks instead of truthy/falsy
    const hasEmail = email !== undefined && email !== null && email !== '';
    const hasPortableEmail = portableEmail !== undefined && portableEmail !== null && portableEmail !== '';

    // Both emails exist
    if (hasEmail && hasPortableEmail) {
        mainEmail = portableEmail;
        subEmail = email;
    }
    // Only email exists
    else if (hasEmail) {
        mainEmail = email;
    }
    // Only portable_email exists
    else if (hasPortableEmail) {
        mainEmail = portableEmail;
    }
    // Check if main_email already exists in the record
    else if (record.main_email !== undefined && record.main_email !== null && record.main_email !== '') {
        mainEmail = record.main_email;
    }
    
    return {
        main_email: mainEmail,
        sub_email: subEmail
    };
}

function filterColumns(record, mappingStats = {}, dataQualityTracker = null, counters = {}) {
    const filteredRecord = {};
    
    // Get derived phone numbers and emails
    const phoneNumbers = derivePhoneNumbers(record);
    const emails = deriveEmails(record);
    
    // Verify phone number logic compliance
    verifyPhoneNumberLogic(record, phoneNumbers, counters);
    
    // Verify email logic compliance
    verifyEmailLogic(record, emails, counters);
    
    // Process each required column
    REQUIRED_COLUMNS.forEach(column => {
        let value;
        let originalValueForQuality;
        
        // Handle special columns
        if (column === 'phone') {
            value = phoneNumbers.phone;
            originalValueForQuality = getFieldValue(record, 'tel') || getFieldValue(record, 'portable_tel') || record.phone;
        } else if (column === 'other_phone') {
            value = phoneNumbers.other_phone;
            originalValueForQuality = getFieldValue(record, 'tel') || getFieldValue(record, 'portable_tel') || record.other_phone;
        } else if (column === 'main_email') {
            value = emails.main_email;
            originalValueForQuality = getFieldValue(record, 'email') || getFieldValue(record, 'portable_email') || record.main_email;
        } else if (column === 'sub_email') {
            value = emails.sub_email;
            originalValueForQuality = getFieldValue(record, 'email') || getFieldValue(record, 'portable_email') || record.sub_email;
        } else if (column === 'main_school_branch_id') {
            // For main_school_branch_id, check if branch_id is 43 or 46
            const branchId = record.branch_id;
            if (branchId !== null && (String(branchId) === '43' || String(branchId) === '46')) {
                value = branchId;
            } else {
                // Check if main_school_branch_id already exists in the record
                // value = record.main_school_branch_id || '';

                // need post-migration apex to set this value
                value = '';
            }
            originalValueForQuality = record.main_school_branch_id || record.branch_id;
        } else if (column === 'branch_id') {
            // For branch_id, only keep value if it's not 43 or 46
            const branchId = record.branch_id;
            if (branchId !== null && (String(branchId) === '43' || String(branchId) === '46')) {
                value = '';
            } else {
                value = branchId;
            }
            originalValueForQuality = record.branch_id;
        } else {
            value = record[column] !== undefined ? record[column] : '';
            originalValueForQuality = record[column];
        }
        
        // Apply transformations
        if (column === 'entrance_date' || column === 'birthday') {
            value = formatDate(value);
                } else if (column === 'graduate_date') {
            // Special logic for graduate_date based on graduate_flg from student_info.csv
            const graduateDateFromStudentInfo = record._graduateDateFromStudentInfo || false;
            const graduateFlg = record._graduateFlg || false;
            
            if (graduateDateFromStudentInfo) {
                // Record has student_info.csv data - apply graduate_flg logic
                // Increment counter for records with graduate_date from student_info.csv
                if (counters && counters.graduateDateFromStudentInfoCount !== undefined) {
                    counters.graduateDateFromStudentInfoCount++;
                }
                
                if (graduateFlg) {
                    // graduate_flg = true (1) - set day to last day of month
                    value = getLastDayOfMonth(value);
                    // Increment counter for modified graduate_date
                    if (counters && counters.graduateDateModifiedCount !== undefined) {
                        counters.graduateDateModifiedCount++;
                    }
                } else {
                    // graduate_flg = false (0) - use value as-is but ensure proper formatting
                    value = formatDate(value);
                    // Increment counter for unmodified graduate_date
                    if (counters && counters.graduateDateUnmodifiedCount !== undefined) {
                        counters.graduateDateUnmodifiedCount++;
                    }
                }
            } else {
                // Record has no student_info.csv data - use as-is with formatting
                value = formatDate(value);
            }
        } else if (column === 'Graduation_Day__c') {
            // Set this column only if graduate_date came from student_info.csv AND graduate_flg = 1
            const graduateDateFromStudentInfo = record._graduateDateFromStudentInfo || false;
            const graduateFlg = record._graduateFlg || false;
            
            if (graduateDateFromStudentInfo && graduateFlg && record.graduate_date) {
                // Use the graduate_date value and format it
                value = formatDate(record.graduate_date);
                // Increment counter for populated Graduation_Day__c
                if (counters && counters.graduationDayCount !== undefined) {
                    counters.graduationDayCount++;
                }
            } else {
                value = ''; // Empty if conditions not met
            }
            originalValueForQuality = (graduateDateFromStudentInfo && graduateFlg) ? record.graduate_date : '';
        } else if (column === 'sex') {
            const originalValue = value;
            value = transformSex(value);
            value = trackMappingStats(originalValue, value, mappingStats.sex, 'sex');
        } else if (column === 'operate_type_id') {
            value = transformOperateType(value);
        } else if (column === 'course_type') {
            const originalValue = value;
            value = transformCourseType(value);
            value = trackMappingStats(originalValue, value, mappingStats.courseType, 'course_type');
        } else if (column === 'grade') {
            const originalValue = value;
            const gradeFromCustomer = record._gradeFromCustomer || false;
            
            if (gradeFromCustomer) {
                // Grade comes from customer.csv - set to empty value
                value = '';
                // Increment counter for grades emptied due to customer.csv source
                if (counters && counters.gradeEmptiedFromCustomerCount !== undefined) {
                    counters.gradeEmptiedFromCustomerCount++;
                }
            } else {
                // Grade comes from student_info.csv - use transformed value
                value = transformGrade(value);
            }
            
            // Apply empty-to-dash conversion for ALL grades (regardless of source)
            if (!value || (typeof value === 'string' && value.trim() === '') || value === '卒業') {
                value = '-';
            }
            
            value = trackMappingStats(originalValue, value, mappingStats.grade, 'grade');
        } else if (column === 'Customer_Grade_Old_system__c') {
            // Set this column only if grade came from customer.csv
            const gradeFromCustomer = record._gradeFromCustomer || false;
            if (gradeFromCustomer && record.grade) {
                // Use the original grade value before transformation
                const originalGradeValue = record.grade;
                value = transformGrade(originalGradeValue);
                // Set empty grade values to '-'
                if (!value || (typeof value === 'string' && value.trim() === '') || value === '卒業') {
                    value = '-';
                }
                // Increment counter for populated Customer_Grade_Old_system__c
                if (counters && counters.customerGradeCount !== undefined) {
                    counters.customerGradeCount++;
                }
            } else {
                value = ''; // Empty if grade didn't come from customer.csv
            }
            originalValueForQuality = gradeFromCustomer ? record.grade : '';
        } else if (column === 'dm_sendable') {
            const originalValue = value;
            value = transformDmSendable(value);
            value = trackMappingStats(originalValue, value, mappingStats.dmSendable, 'dm_sendable');
        } else if (column === 'pref_id') {
            value = transformPrefecture(value);
        } else if (column === 'zip_cd') {
            value = removeDashes(value);
        } else if (column === 'branch_id' && value) {
            value = transformBranchId(value);
        } else if (column === 'main_school_branch_id' && value) {
            value = transformBranchId(value);
        } else if (column === 'description') {
            // Escape commas and wrap with double quotes if value contains comma
            if (value && typeof value === 'string' && value.includes(',')) {
                value = `"${value.replace(/"/g, '""')}"`;
            }
        } else if (column === 'FirstName' || column === 'LastName' || column === 'fname1' || column === 'fname2' || column === 'kname1' || column === 'kname2') {
            // Replace empty firstname or lastname with "不明"
            if (!value || (typeof value === 'string' && value.trim() === '')) {
                value = '不明';
            }
        } else if (column === 'phone' || column === 'other_phone') {
            value = transformPhone(value);
        }
        
        // Track data quality for this column (after all transformations)
        trackDataQuality(column, originalValueForQuality, value, dataQualityTracker);
        
        filteredRecord[column] = value;
    });
        
    return filteredRecord;
}

// Reading CSV with manual stream for memory saving
async function readAndParseCSV(filename) {
    try {
        console.log(`Reading ${filename}...`);
        const content = await fs.readFile(filename, 'utf8');
        return new Promise((resolve, reject) => {
            Papa.parse(content, {
                header: true,
                skipEmptyLines: true,
                dynamicTyping: true,
                delimiter: ",",
                newline: "\n",
                transform: (value) => {
                    if (typeof value === 'string') {
                        return value.replace(/\r$/, '');
                    }
                    return value;
                },
                complete: (results) => {
                    if (results.errors.length > 0) {
                        console.log(`   ⚠️  Warnings while parsing ${filename}:`, results.errors);
                    }
                    
                    // Log the header fields to help with debugging
                    console.log(`   ℹ️ Headers in ${filename}: ${results.meta.fields.join(', ')}`);
                    console.log(`   ✓ Successfully parsed ${results.data.length} records from ${filename}`);
                    
                    resolve(results);
                },
                error: (error) => {
                    reject(error);
                }
            });
        });
    } catch (error) {
        console.error(`\n❌ Error reading ${filename}:`, error);
        throw error;
    }
}

// Main function to process student data
async function processStudentData(inputFile) {
    try {
        console.log('🚀 Starting student data processing...\n');
        const startTime = Date.now();
        
        // Initialize counter
        let currentCounter = COUNTER_START;
        
        // Log the tag value being used
        console.log(`Using tag value: "${TAG_VALUE}" (ADD_TAG_COLUMN: ${ADD_TAG_COLUMN})`);
        
        // Step 1: Read student.csv
        console.log('Step 1: Reading student.csv...');
        const studentData = await readAndParseCSV(inputFile);
        console.log(`Found ${studentData.data.length} records in student.csv`);
        
        // Create a map of student_id to customer_id for quick lookups
        const studentIdToCustomerId = new Map();
        studentData.data.forEach(student => {
            if (student.student_id) {
                studentIdToCustomerId.set(String(student.student_id), String(student.customer_id));
            }
        });
        
        // Step 2: Read student_info.csv
        console.log('\nStep 2: Reading student_info.csv...');
        const studentInfoData = await readAndParseCSV('student_info.csv');
        console.log(`Found ${studentInfoData.data.length} records in student_info.csv`);
        
        // Create a map for quick lookups by student_id
        const studentInfoMap = new Map();
        studentInfoData.data.forEach(info => {
            if (info.student_id) {
                studentInfoMap.set(String(info.student_id), info);
            }
        });
        
        // Free memory
        studentInfoData.data = [];
        
        // Step 3: Read customer.csv
        console.log('\nStep 3: Reading customer.csv...');
        const customerData = await readAndParseCSV('customer.csv');
        console.log(`Found ${customerData.data.length} records in customer.csv`);
        
        // Create a map for quick lookups by customer_id
        const customerMap = new Map();
        customerData.data.forEach(customer => {
            if (customer.customer_id) {
                customerMap.set(String(customer.customer_id), customer);
            }
        });
        
        // Free memory
        customerData.data = [];
        
        // Step 3.5: Read student_info_history.csv
        console.log('\nStep 3.5: Reading student_info_history.csv...');
        const studentHistoryData = await readAndParseCSV('student_info_history.csv');
        console.log(`Found ${studentHistoryData.data.length} records in student_info_history.csv`);
        

        
        // Process history records to get latest entries
        console.log('   Processing history records to get latest entries...');
        

        
        const latestHistoryRecords = _.chain(studentHistoryData.data)
            .groupBy('student_id')
            .mapValues(group => _.maxBy(group, 'created_at'))
            .values()
            .value();
        console.log(`   ✓ Processed ${latestHistoryRecords.length} unique student histories`);
        
        // Create a map for quick lookups by student_id
        const historyMap = new Map();
        latestHistoryRecords.forEach(history => {
            if (history.student_id) {
                historyMap.set(String(history.student_id), history);
            }
        });
        console.log(`   ✓ Created lookup map for ${historyMap.size} history records`);
        

        
        // Free memory
        studentHistoryData.data = [];
        latestHistoryRecords.length = 0;
        
        // Step 4: Process student data and combine with info
        console.log('\nStep 4: Processing student data and combining with info...');
        
        // Initialize output files with headers
        const outputFilename = 'processed_student_data.csv';
        const incompleteFilename = 'processed_student_data_incomplete.csv';
        
        // Add tag column to the headers if needed
        const headers = ADD_TAG_COLUMN ? [...REQUIRED_COLUMNS, 'tag'] : REQUIRED_COLUMNS;
        const headerLine = headers.join(',') + '\n';
        
        await fs.writeFile(outputFilename, headerLine);
        await fs.writeFile(incompleteFilename, headerLine);
        
        // Process student data in chunks
        const chunks = _.chunk(studentData.data, CHUNK_SIZE);
        let processedCount = 0;
        let studentInfoCount = 0;
        let customerInfoCount = 0;
        let incompleteCount = 0;
        let historyOperateTypeCount = 0;
        let fallbackOperateTypeCount = 0;
        let operateTypeBreakdown = {
            '高認': 0,
            '本科': 0,
            'empty': 0,
            'fromHistory': 0
        };
        let customerGradeCount = 0; // Track records with Customer_Grade_Old_system__c populated
        let graduationDayCount = 0; // Track records with Graduation_Day__c populated
        let graduateDateModifiedCount = 0; // Track records where graduate_date was set to end of month
        let graduateDateFromStudentInfoCount = 0; // Track total records with graduate_date from student_info.csv
        let graduateDateUnmodifiedCount = 0; // Track records where graduate_date was not modified (graduate_flg = false)
        let gradeEmptiedFromCustomerCount = 0; // Track records where grade was set to empty because it came from customer.csv
        
        // Phone rule compliance statistics
        let phoneRuleStats = {
            rule1_both_exist: 0,
            rule1_compliant: 0,
            rule1_non_compliant: 0,
            rule2_tel_only: 0,
            rule2_compliant: 0,
            rule2_non_compliant: 0,
            rule3_portable_only: 0,
            rule3_compliant: 0,
            rule3_non_compliant: 0,
            rule4_both_null: 0,
            rule4_compliant: 0,
            rule4_non_compliant: 0
        };
        
        // Email rule compliance statistics
        let emailRuleStats = {
            rule1_both_exist: 0,
            rule1_compliant: 0,
            rule1_non_compliant: 0,
            rule2_email_only: 0,
            rule2_compliant: 0,
            rule2_non_compliant: 0,
            rule3_portable_only: 0,
            rule3_compliant: 0,
            rule3_non_compliant: 0,
            rule4_both_null: 0,
            rule4_compliant: 0,
            rule4_non_compliant: 0
        };
        
        // Create mapping statistics trackers using helper function
        let operateTypeStats = {
            historyMappings: createMappingStatsTracker(),
            fallbackAssignments: {
                '高認': 0,  // student_id starts with '1'
                '本科': 0,  // student_id starts with '4'  
                'empty': 0  // student_id starts with '5'
            },
            finalMappedValues: new Map() // mapped_value -> count
        };
        
        // Create all mapping statistics trackers
        let allMappingStats = createAllMappingStats();
        
        // Create data quality tracker
        let dataQualityTracker = createDataQualityTracker();
        
        for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];
            console.log(`\n   Processing student chunk ${i + 1} of ${chunks.length}`);
            
            const processedChunk = [];
            const incompleteChunk = [];
            
            chunk.forEach(student => {
                const studentId = String(student.student_id);
                const customerId = studentIdToCustomerId.get(studentId);
                
                // Try to find info in student_info.csv first
                let infoRecord = studentInfoMap.get(studentId);
                let gradeFromCustomer = false; // Track if grade comes from customer.csv
                let graduateDateFromStudentInfo = false; // Track if graduate_date comes from student_info.csv
                let graduateFlg = false; // Track graduate_flg from student_info.csv
                
                // If not found in student_info.csv, try customer.csv
                if (!infoRecord && customerId) {
                    infoRecord = customerMap.get(customerId);
                    if (infoRecord) {
                        gradeFromCustomer = true; // Entire record from customer.csv
                        customerInfoCount++;
                    }
                } else if (infoRecord) {
                    // Record comes from student_info.csv - always track this for graduate_flg logic
                    graduateDateFromStudentInfo = true;
                    
                    // Capture graduate_flg value (could be 1, '1', true, 'true', etc.)
                    const graduateFlagValue = infoRecord.graduate_flg || infoRecord['graduate_flg'];
                    graduateFlg = (graduateFlagValue === 1 || graduateFlagValue === '1' || graduateFlagValue === true || graduateFlagValue === 'true');
                    
                    // If we have student_info record, check for null values and fill from customer.csv
                    if (customerId) {
                        const customerRecord = customerMap.get(customerId);
                        if (customerRecord) {
                            // Map the fields from customer.csv to student_info.csv format
                            const fieldMappings = {
                                'birthday': 'birthday',
                                'sex': 'sex',
                                'phone': 'phone',
                                'other_phone': 'other_phone',
                                'main_email': 'main_email',
                                'sub_email': 'sub_email',
                                'description': 'description',
                                'grade': 'grade', // Add grade to the mapping
                                'Customer_Grade_Old_system__c': 'grade' // Map customer grade to Customer_Grade_Old_system__c
                            };

                            // Fill in null values from customer record
                            Object.entries(fieldMappings).forEach(([studentInfoField, customerField]) => {
                                if (!infoRecord[studentInfoField] && customerRecord[customerField]) {
                                    infoRecord[studentInfoField] = customerRecord[customerField];
                                    // Track if grade was filled from customer.csv
                                    if (studentInfoField === 'grade') {
                                        gradeFromCustomer = true;
                                    }
                                }
                            });
                        }
                    }
                    studentInfoCount++;
                }
                
                // Check if this is an incomplete record (no info found in either source)
                if (!infoRecord) {
                    // This is an incomplete record
                    incompleteCount++;
                    
                    // Create a record with just the student data and tag
                    const incompleteRecord = {
                        ...student
                    };
                    
                    // Ensure customer_id is set properly for incomplete records
                    if (customerId) {
                        incompleteRecord.customer_id = customerId;
                        
                        // Get customer data to populate Customer_Grade_Old_system__c even for incomplete records
                        const customerRecord = customerMap.get(customerId);
                        if (customerRecord && customerRecord.grade) {
                            incompleteRecord.Customer_Grade_Old_system__c = customerRecord.grade;
                        }
                    }
                    
                    // For incomplete records, no info comes from customer.csv or student_info.csv since no info was found
                    incompleteRecord._gradeFromCustomer = false;
                    incompleteRecord._graduateDateFromStudentInfo = false;
                    incompleteRecord._graduateFlg = false;
                    
                    // Apply transformations
                    const counters = { 
                        customerGradeCount, 
                        graduationDayCount, 
                        graduateDateModifiedCount,
                        graduateDateFromStudentInfoCount,
                        graduateDateUnmodifiedCount,
                        gradeEmptiedFromCustomerCount,
                        phoneRuleStats,
                        emailRuleStats
                    };
                    const filteredRecord = filterColumns(incompleteRecord, allMappingStats, dataQualityTracker, counters);
                    customerGradeCount = counters.customerGradeCount; // Update the counter
                    graduationDayCount = counters.graduationDayCount; // Update the counter
                    graduateDateModifiedCount = counters.graduateDateModifiedCount; // Update the counter
                    graduateDateFromStudentInfoCount = counters.graduateDateFromStudentInfoCount; // Update the counter
                    graduateDateUnmodifiedCount = counters.graduateDateUnmodifiedCount; // Update the counter
                    gradeEmptiedFromCustomerCount = counters.gradeEmptiedFromCustomerCount; // Update the counter
                    
                    // Add tag if needed
                    if (ADD_TAG_COLUMN) {
                        filteredRecord.tag = TAG_VALUE;
                    }
                    
                    incompleteChunk.push(filteredRecord);
                } else {
                    // Check for operate_type_id in student_info_history.csv
                    const historyRecord = historyMap.get(studentId);
                    if (historyRecord) {
                        // Handle the case where operate_type_id might have \r character
                        const originalOperateTypeId = historyRecord.operate_type_id || historyRecord['operate_type_id\r'];
                        if (originalOperateTypeId) {
                            // Apply transformation to get mapped value
                            const mappedOperateTypeId = transformOperateType(originalOperateTypeId);
                            infoRecord.operate_type_id = mappedOperateTypeId;
                            
                            // Track detailed statistics using helper function
                            trackMappingStats(originalOperateTypeId, mappedOperateTypeId, operateTypeStats.historyMappings, 'operate_type_id');
                            
                            // Track final mapped values for operate_type_id summary
                            const finalMappedKey = String(mappedOperateTypeId);
                            operateTypeStats.finalMappedValues.set(finalMappedKey, 
                                (operateTypeStats.finalMappedValues.get(finalMappedKey) || 0) + 1);
                            
                            historyOperateTypeCount++;
                            operateTypeBreakdown.fromHistory++;
                        }
                    } else {
                        // If no student_info_history, determine operate_type_id from first letter of student_id
                        const firstLetter = studentId.charAt(0);
                        if (firstLetter === '1') {
                            infoRecord.operate_type_id = '高認';
                            operateTypeStats.fallbackAssignments['高認']++;
                            operateTypeStats.finalMappedValues.set('高認', 
                                (operateTypeStats.finalMappedValues.get('高認') || 0) + 1);
                            fallbackOperateTypeCount++;
                            operateTypeBreakdown['高認']++;
                        } else if (firstLetter === '4') {
                            infoRecord.operate_type_id = '本科';
                            operateTypeStats.fallbackAssignments['本科']++;
                            operateTypeStats.finalMappedValues.set('本科', 
                                (operateTypeStats.finalMappedValues.get('本科') || 0) + 1);
                            fallbackOperateTypeCount++;
                            operateTypeBreakdown['本科']++;
                        } else if (firstLetter === '5') {
                            // do nothing. 
                            infoRecord.operate_type_id = '';
                            operateTypeStats.fallbackAssignments['empty']++;
                            operateTypeStats.finalMappedValues.set('', 
                                (operateTypeStats.finalMappedValues.get('') || 0) + 1);
                            fallbackOperateTypeCount++;
                            operateTypeBreakdown.empty++;
                        }
                    }
                    
                                    // Combine student data with info
                const combinedRecord = {
                    ...student,
                    ...infoRecord
                };
                
                // Ensure student_id is set properly
                combinedRecord.student_id = studentId;
                
                // Ensure customer_id is set properly
                if (customerId) {
                    combinedRecord.customer_id = customerId;
                }
                
                // Add metadata about data sources
                combinedRecord._gradeFromCustomer = gradeFromCustomer;
                combinedRecord._graduateDateFromStudentInfo = graduateDateFromStudentInfo;
                combinedRecord._graduateFlg = graduateFlg;
                    
                    // Apply transformations
                    const counters = { 
                        customerGradeCount, 
                        graduationDayCount, 
                        graduateDateModifiedCount,
                        graduateDateFromStudentInfoCount,
                        graduateDateUnmodifiedCount,
                        gradeEmptiedFromCustomerCount,
                        phoneRuleStats,
                        emailRuleStats
                    };
                    const filteredRecord = filterColumns(combinedRecord, allMappingStats, dataQualityTracker, counters);
                    customerGradeCount = counters.customerGradeCount; // Update the counter
                    graduationDayCount = counters.graduationDayCount; // Update the counter
                    graduateDateModifiedCount = counters.graduateDateModifiedCount; // Update the counter
                    graduateDateFromStudentInfoCount = counters.graduateDateFromStudentInfoCount; // Update the counter
                    graduateDateUnmodifiedCount = counters.graduateDateUnmodifiedCount; // Update the counter
                    gradeEmptiedFromCustomerCount = counters.gradeEmptiedFromCustomerCount; // Update the counter
                    
                    // Add tag if needed
                    if (ADD_TAG_COLUMN) {
                        filteredRecord.tag = TAG_VALUE;
                    }
                    
                    processedChunk.push(filteredRecord);
                }
            });
            
            // Convert chunks to CSV
            const processedCsv = processedChunk.map(record => {
                const counter = currentCounter++;
                return [counter, ...headers.slice(1).map(column => {
                    const value = record[column];
                    return escapeCSVValue(value);
                })].join(',');
            }).join('\n');
            
            const incompleteCsv = incompleteChunk.map(record => {
                const counter = currentCounter++;
                return [counter, ...headers.slice(1).map(column => {
                    const value = record[column];
                    return escapeCSVValue(value);
                })].join(',');
            }).join('\n');
            
            if (processedChunk.length > 0) {
                await fs.appendFile(outputFilename, processedCsv + '\n');
            }
            
            if (incompleteChunk.length > 0) {
                await fs.appendFile(incompleteFilename, incompleteCsv + '\n');
            }
            
            processedCount += processedChunk.length;
            console.log(`   ✓ Processed ${processedCount}/${studentData.data.length} student records`);
            console.log(`   ✓ Incomplete records: ${incompleteCount}`);
            console.log(`   ✓ Operate_type_id: ${historyOperateTypeCount} from history, ${fallbackOperateTypeCount} from fallback`);
            console.log(`   ✓ Customer_Grade_Old_system__c populated: ${customerGradeCount} records`);
            console.log(`   ✓ Graduation_Day__c populated: ${graduationDayCount} records`);
            console.log(`   ✓ Graduate_date processing: ${graduateDateFromStudentInfoCount} from student_info, ${graduateDateModifiedCount} modified to end-of-month`);
            console.log(`   ✓ Grade emptied from customer.csv: ${gradeEmptiedFromCustomerCount} records`);
            
            // Phone rule compliance progress
            const totalPhoneRecordsProgress = phoneRuleStats.rule1_both_exist + phoneRuleStats.rule2_tel_only + phoneRuleStats.rule3_portable_only + phoneRuleStats.rule4_both_null;
            const totalCompliantProgress = phoneRuleStats.rule1_compliant + phoneRuleStats.rule2_compliant + phoneRuleStats.rule3_compliant + phoneRuleStats.rule4_compliant;
            const complianceRateProgress = totalPhoneRecordsProgress > 0 ? ((totalCompliantProgress / totalPhoneRecordsProgress) * 100).toFixed(1) : '0.0';
            console.log(`   ✓ Phone rule compliance: ${totalCompliantProgress}/${totalPhoneRecordsProgress} (${complianceRateProgress}%)`);
            
            // Email rule compliance progress
            const totalEmailRecordsProgress = emailRuleStats.rule1_both_exist + emailRuleStats.rule2_email_only + emailRuleStats.rule3_portable_only + emailRuleStats.rule4_both_null;
            const totalEmailCompliantProgress = emailRuleStats.rule1_compliant + emailRuleStats.rule2_compliant + emailRuleStats.rule3_compliant + emailRuleStats.rule4_compliant;
            const emailComplianceRateProgress = totalEmailRecordsProgress > 0 ? ((totalEmailCompliantProgress / totalEmailRecordsProgress) * 100).toFixed(1) : '0.0';
            console.log(`   ✓ Email rule compliance: ${totalEmailCompliantProgress}/${totalEmailRecordsProgress} (${emailComplianceRateProgress}%)`);
            console.log(`   ✓ Mapping stats: DM(${allMappingStats.dmSendable.mappings.size}) Sex(${allMappingStats.sex.mappings.size}) Grade(${allMappingStats.grade.mappings.size}) Course(${allMappingStats.courseType.mappings.size})`);
            const originalIssues = dataQualityTracker.overallStats.original.emptyValues + dataQualityTracker.overallStats.original.nullValues + dataQualityTracker.overallStats.original.invalidValues;
            const outputIssues = dataQualityTracker.overallStats.output.emptyValues + dataQualityTracker.overallStats.output.nullValues + dataQualityTracker.overallStats.output.invalidValues;
            console.log(`   ✓ Data quality: ${dataQualityTracker.totalFields} fields, ${originalIssues}→${outputIssues} issues (fixed ${originalIssues - outputIssues})`);
            
            // Free memory
            processedChunk.length = 0;
            incompleteChunk.length = 0;
            
            // Log memory usage
            const currentMemory = process.memoryUsage();
            console.log(`   Current heap used: ${Math.round(currentMemory.heapUsed / 1024 / 1024)}MB`);
        }
        
        // Free memory
        chunks.length = 0;
        studentData.data = [];
        
        // Step 5: Rename columns in the final output
        console.log('\nStep 5: Renaming columns in the final output...');
        await renameColumnsInFinalOutput(outputFilename);
        await renameColumnsInFinalOutput(incompleteFilename);
        
        // Print summary
        console.log('\n✅ Processing completed successfully!');
        console.log(`Total student records processed: ${processedCount}`);
        console.log(`Records with student_info data: ${studentInfoCount}`);
        console.log(`Records with customer data: ${customerInfoCount}`);
        console.log(`Incomplete records (written to ${incompleteFilename}): ${incompleteCount}`);
        console.log(`Records with Customer_Grade_Old_system__c populated: ${customerGradeCount}`);
        console.log(`Records with Graduation_Day__c populated: ${graduationDayCount}`);
        console.log(`Records with grade emptied from customer.csv: ${gradeEmptiedFromCustomerCount}`);
        
        // Graduate date manipulation summary  
        console.log(`\n📅 Graduate Date Processing Summary:`);
        console.log(`├── Total records with graduate_date from student_info.csv: ${graduateDateFromStudentInfoCount}`);
        console.log(`├── Records with graduate_date modified to end-of-month (graduate_flg = 1): ${graduateDateModifiedCount}`);
        console.log(`└── Records with graduate_date unchanged (graduate_flg = 0): ${graduateDateUnmodifiedCount}`);
        
        // Phone rule compliance summary
        const totalPhoneRecords = phoneRuleStats.rule1_both_exist + phoneRuleStats.rule2_tel_only + phoneRuleStats.rule3_portable_only + phoneRuleStats.rule4_both_null;
        const totalCompliantRecords = phoneRuleStats.rule1_compliant + phoneRuleStats.rule2_compliant + phoneRuleStats.rule3_compliant + phoneRuleStats.rule4_compliant;
        const complianceRate = totalPhoneRecords > 0 ? ((totalCompliantRecords / totalPhoneRecords) * 100).toFixed(1) : '0.0';
        
        console.log(`\n📞 Phone Number Rule Compliance Summary:`);
        console.log(`├── Total records processed: ${totalPhoneRecords}`);
        console.log(`├── Compliant records: ${totalCompliantRecords} (${complianceRate}%)`);
        console.log(`├── Rule 1 (both tel & portable_tel): ${phoneRuleStats.rule1_both_exist} total, ${phoneRuleStats.rule1_compliant} compliant, ${phoneRuleStats.rule1_non_compliant} non-compliant`);
        console.log(`├── Rule 2 (tel only): ${phoneRuleStats.rule2_tel_only} total, ${phoneRuleStats.rule2_compliant} compliant, ${phoneRuleStats.rule2_non_compliant} non-compliant`);
        console.log(`├── Rule 3 (portable_tel only): ${phoneRuleStats.rule3_portable_only} total, ${phoneRuleStats.rule3_compliant} compliant, ${phoneRuleStats.rule3_non_compliant} non-compliant`);
        console.log(`└── Rule 4 (both NULL): ${phoneRuleStats.rule4_both_null} total, ${phoneRuleStats.rule4_compliant} compliant, ${phoneRuleStats.rule4_non_compliant} non-compliant`);
        
        // Email rule compliance summary
        const totalEmailRecords = emailRuleStats.rule1_both_exist + emailRuleStats.rule2_email_only + emailRuleStats.rule3_portable_only + emailRuleStats.rule4_both_null;
        const totalEmailCompliantRecords = emailRuleStats.rule1_compliant + emailRuleStats.rule2_compliant + emailRuleStats.rule3_compliant + emailRuleStats.rule4_compliant;
        const emailComplianceRate = totalEmailRecords > 0 ? ((totalEmailCompliantRecords / totalEmailRecords) * 100).toFixed(1) : '0.0';
        
        console.log(`\n📧 Email Rule Compliance Summary:`);
        console.log(`├── Total records processed: ${totalEmailRecords}`);
        console.log(`├── Compliant records: ${totalEmailCompliantRecords} (${emailComplianceRate}%)`);
        console.log(`├── Rule 1 (both email & portable_email): ${emailRuleStats.rule1_both_exist} total, ${emailRuleStats.rule1_compliant} compliant, ${emailRuleStats.rule1_non_compliant} non-compliant`);
        console.log(`├── Rule 2 (email only): ${emailRuleStats.rule2_email_only} total, ${emailRuleStats.rule2_compliant} compliant, ${emailRuleStats.rule2_non_compliant} non-compliant`);
        console.log(`├── Rule 3 (portable_email only): ${emailRuleStats.rule3_portable_only} total, ${emailRuleStats.rule3_compliant} compliant, ${emailRuleStats.rule3_non_compliant} non-compliant`);
        console.log(`└── Rule 4 (both NULL): ${emailRuleStats.rule4_both_null} total, ${emailRuleStats.rule4_compliant} compliant, ${emailRuleStats.rule4_non_compliant} non-compliant`);
        
        // Detailed operate_type_id insights
        console.log('\n📊 Operate Type ID Processing Statistics:');
        console.log(`├── Records with operate_type_id from history: ${historyOperateTypeCount}`);
        console.log(`├── Records using fallback logic (student_id first letter): ${fallbackOperateTypeCount}`);
        
        // Show detailed history mappings (Original -> Mapped -> Count)
        if (operateTypeStats.historyMappings.mappings.size > 0) {
            console.log('\n🔄 Operate Type ID History Statistics:');
            displayMappingStats(operateTypeStats.historyMappings, 'Operate Type ID History', historyOperateTypeCount);
        }
        
        // Show fallback assignments
        console.log('\n🎯 Fallback Assignments (Student ID First Letter → Type → Count):');
        console.log(`   ├── Student ID starts with '1' → '高認' → ${operateTypeStats.fallbackAssignments['高認']} records`);
        console.log(`   ├── Student ID starts with '4' → '本科' → ${operateTypeStats.fallbackAssignments['本科']} records`);
        console.log(`   └── Student ID starts with '5' → '' (empty) → ${operateTypeStats.fallbackAssignments['empty']} records`);
        
        // Show final mapped value summary
        console.log('\n📈 Final Operate Type Distribution:');
        Array.from(operateTypeStats.finalMappedValues.entries())
            .sort(([,a], [,b]) => b - a) // Sort by count descending
            .forEach(([mappedValue, count]) => {
                const displayValue = mappedValue === '' ? '(empty)' : mappedValue;
                const percentage = ((count / processedCount) * 100).toFixed(1);
                console.log(`   ├── "${displayValue}" → ${count} records (${percentage}%)`);
            });
        
        const totalOperateTypeProcessed = historyOperateTypeCount + fallbackOperateTypeCount;
        console.log(`\n✅ Coverage: ${totalOperateTypeProcessed}/${processedCount} (${((totalOperateTypeProcessed/processedCount)*100).toFixed(1)}%) records have operate_type_id assigned`);
        
        // Show all mapping statistics using helper function
        displayAllMappingStats(allMappingStats, processedCount);
        
        // Set total records for data quality tracker
        dataQualityTracker.totalRecords = processedCount;
        
        // Show data quality statistics
        displayDataQualityStats(dataQualityTracker, processedCount);
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`\nTotal execution time: ${duration} seconds`);
        
        return {
            outputFilename,
            incompleteFilename,
            processedCount,
            incompleteCount
        };
    } catch (error) {
        console.error('\n❌ Script failed:', error);
        throw error;
    }
}

// Function to rename columns in the final output
async function renameColumnsInFinalOutput(filename) {
    try {
        console.log(`\nPerforming final column name transformation for ${filename}...`);
        
        // Read the original file with original column names
        const content = await fs.readFile(filename, 'utf8');
        
        // Split into lines
        const lines = content.split('\n');
        
        if (lines.length <= 1) {
            console.log('   ⚠️ File appears to be empty or has only header row. Skipping rename.');
            return;
        }
        
        // Get the header line and split into columns
        const headerLine = lines[0];
        const headers = headerLine.split(',');
        
        // Replace the header line with mapped column names
        const newHeaderLine = ADD_TAG_COLUMN 
            ? getOutputColumnNames().join(',') + ',tag'
            : getOutputColumnNames().join(',');
        
        // Create new content with updated header line
        lines[0] = newHeaderLine;
        const newContent = lines.join('\n');
        
        // Write back to file
        await fs.writeFile(filename, newContent, 'utf8');
        
        console.log(`   ✓ Successfully renamed columns in ${filename}`);
    } catch (error) {
        console.error(`   ❌ Error renaming columns: ${error.message}`);
    }
}

// Main function
async function main() {
    console.log('🚀 Starting student data processing...\n');
    const startTime = Date.now();
    
    try {
        // Parse command line arguments
        const args = process.argv.slice(2);
        const shouldMask = args.includes('--mask');
        
        // Remove the --mask flag from args to get the input file
        const inputFileArg = args.filter(arg => arg !== '--mask')[0];
        const inputFile = inputFileArg || 'student.csv';
        
        console.log(`Using input file: ${inputFile}`);
        console.log(`Masking enabled: ${shouldMask}`);
        
        // Step 1: Process student data
        const { outputFilename, incompleteFilename } = await processStudentData(inputFile);
        
        // Step 2: Run fix-line.js
        console.log('\nRunning fix-line.js to process the final output...');
        const fixedFilename = fixNextLine(outputFilename);
        if (!fixedFilename) {
            throw new Error('Failed to process file with fix-line.js');
        }

        // Step 3: Run masking as the last step if flag is present
        if (shouldMask) {
            console.log('\nRunning mask-data.js to mask sensitive information...');
            const result = await maskData(fixedFilename);
            if (!result.success) {
                throw new Error(`Failed to mask data: ${result.error}`);
            }
            console.log(`\n📊 Masking summary:`);
            console.log(`- Total records processed: ${result.totalRecords}`);
            console.log(`- Columns masked: ${result.maskedColumns.join(', ')}`);
            console.log(`- Output saved to: ${result.outputFile}`);
            
            // Step 4: Verify masking integrity
            console.log('\n🔍 Verifying masking integrity...');
            try {
                const verificationSuccess = await verifyMaskingIntegrity(fixedFilename, result.outputFile);
                
                if (verificationSuccess) {
                    console.log('\n🎉 MASKING VERIFICATION PASSED: No data loss detected!');
                    console.log('✅ All sensitive data was properly masked while preserving data integrity.');
                } else {
                    console.log('\n⚠️ MASKING VERIFICATION FAILED: Data integrity issues detected!');
                    console.log('❌ Please review the verification report above before using the masked data.');
                    console.log(`   Before file: ${fixedFilename}`);
                    console.log(`   After file: ${result.outputFile}`);
                    console.log('\n💡 You can re-run verification manually with:');
                    console.log(`   node verify-masking-integrity.js "${fixedFilename}" "${result.outputFile}"`);
                }
            } catch (verificationError) {
                console.log('\n⚠️ Could not verify masking integrity:', verificationError.message);
                console.log('💡 You can verify manually with:');
                console.log(`   node verify-masking-integrity.js "${fixedFilename}" "${result.outputFile}"`);
            }
        } else {
            console.log('\nSkipping data masking (use --mask flag to enable)');
        }
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`\nTotal execution time: ${duration} seconds`);
    } catch (error) {
        console.error('\nScript failed:', error);
        process.exit(1);
    }
}

// Run the script
main(); 