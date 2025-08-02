import { promises as fs } from 'fs';
import Papa from 'papaparse';
import _ from 'lodash';
import { fixNextLine } from './fix-line.js';
import { maskData } from './mask-data.js';
import { verifyMaskingIntegrity } from './verify-masking-integrity.js';

const CHUNK_SIZE = 5000; // Smaller chunk size to reduce memory pressure
const TAG_VALUE = "phase22"; // Configurable tag value - can be changed or removed in final migration

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
        prefecture: createMappingStatsTracker(),
        gender: createMappingStatsTracker(),
        zipCode: createMappingStatsTracker(),
        // Easy to add new column tracking:
        // email: createMappingStatsTracker(),
        // phone: createMappingStatsTracker(),
    };
}

// Helper function to display all mapping statistics
function displayAllMappingStats(allMappingStats, processedCount) {
    const statsConfig = [
        { key: 'prefecture', title: 'Prefecture', icon: '🗾' },
        { key: 'gender', title: 'Gender', icon: '👤' },
        // { key: 'zipCode', title: 'Zip Code', icon: '📮' },  // Commented out to reduce verbose logging
        // Easy to add new column display:
        // { key: 'email', title: 'Email', icon: '📧' },
        // { key: 'phone', title: 'Phone', icon: '📞' },
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
    
    // Email validation
    if (columnName.includes('email')) {
        if (strValue !== '') {
            // Basic email pattern check
            return !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(strValue);
        }
    }
    
    // Phone validation
    if (columnName.includes('tel') || columnName.includes('phone')) {
        if (strValue !== '') {
            // Check if phone contains only digits, spaces, hyphens, parentheses
            return !/^[\d\s\-\(\)]+$/.test(strValue);
        }
    }
    
    // Zip code validation
    if (columnName.includes('zip')) {
        if (strValue !== '') {
            // Japanese zip code should be 7 digits (with or without hyphen)
            return !/^\d{3}-?\d{4}$/.test(strValue.replace(/\s/g, ''));
        }
    }
    
    return false;
}

// Helper function to track data quality for a single field
function trackDataQuality(columnName, originalValue, transformedValue, qualityTracker) {
    if (!qualityTracker) return transformedValue;
    
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
    console.log(`   ├── Total fields processed: ${qualityTracker.totalFields}`);
    
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

// Function to verify phone number logic compliance
function verifyPhoneNumberLogic(record, derivedPhones, counters) {
    if (!counters || !counters.phoneRuleStats) return;
    
    const tel = record['tel'];
    const portableTel = record['portable_tel'];
    
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
    
    const email = record['email'];
    const portableEmail = record['portable_email'];
    
    // Check if values are truly NULL (not just empty)
    const emailIsNotNull = email !== undefined && email !== null && email !== '';
    const portableEmailIsNotNull = portableEmail !== undefined && portableEmail !== null && portableEmail !== '';
    
    const finalEmail1 = derivedEmails.email1;  // portable_email becomes Email1
    const finalEmail2 = derivedEmails.email2;  // email becomes Email2
    
    // Rule 1: email is not NULL AND portable_email is not NULL → portable_email is Email1 and email is Email2
    if (emailIsNotNull && portableEmailIsNotNull) {
        counters.emailRuleStats.rule1_both_exist++;
        if (finalEmail1 === portableEmail && finalEmail2 === email) {
            counters.emailRuleStats.rule1_compliant++;
        } else {
            counters.emailRuleStats.rule1_non_compliant++;
        }
    }
    // Rule 2: email is not NULL AND portable_email is NULL → email is Email1
    else if (emailIsNotNull && !portableEmailIsNotNull) {
        counters.emailRuleStats.rule2_email_only++;
        if (finalEmail1 === email && (finalEmail2 === null || finalEmail2 === '')) {
            counters.emailRuleStats.rule2_compliant++;
        } else {
            counters.emailRuleStats.rule2_non_compliant++;
        }
    }
    // Rule 3: email is NULL AND portable_email is not NULL → portable_email is Email1
    else if (!emailIsNotNull && portableEmailIsNotNull) {
        counters.emailRuleStats.rule3_portable_only++;
        if (finalEmail1 === portableEmail && (finalEmail2 === null || finalEmail2 === '')) {
            counters.emailRuleStats.rule3_compliant++;
        } else {
            counters.emailRuleStats.rule3_non_compliant++;
        }
    }
    // Rule 4: email is NULL AND portable_email is NULL → both are NULL
    else if (!emailIsNotNull && !portableEmailIsNotNull) {
        counters.emailRuleStats.rule4_both_null++;
        if ((finalEmail1 === null || finalEmail1 === '') && (finalEmail2 === null || finalEmail2 === '')) {
            counters.emailRuleStats.rule4_compliant++;
        } else {
            counters.emailRuleStats.rule4_non_compliant++;
        }
    }
}

// Define the column name mappings for the output
const OUTPUT_COLUMN_MAPPING = {
    'parent_id': 'MANAERP__External_User_Id__c',
    'kname1': 'LastName',
    'kname2': 'FirstName',
    'fname1': 'MANAERP__Last_Name_Phonetic__c',
    'fname2': 'MANAERP__First_Name_Phonetic__c',
    'portable_email': 'Email',
    'email': 'Sub_Email__c',
    'portable_tel': 'OtherPhone',
    'tel': 'Phone',
    'zip_cd': 'MANAERP__Postal_Code__c',
    'pref_id': 'MANAERP__Prefecture__c',
    'address1': 'MANAERP__City__c',
    'address2': 'MANAERP__Street_1__c',
    'address3': 'MANAERP__Street_2__c',
    'combined_info': 'Description',
    'gender': 'GenderIdentity',
    'tag': '_MANAERP__Tag__c' // to be ignored
};
// Define the mappings
const PREFECTURE_MAPPING = {
    '1': '北海道',
    '2': '青森県',
    '3': '岩手県',
    '4': '宮城県',
    '5': '秋田県',
    '6': '山形県',
    '7': '福島県',
    '8': '茨城県',
    '9': '栃木県',
    '10': '群馬県',
    '11': '埼玉県',
    '12': '千葉県',
    '13': '東京都',
    '14': '神奈川県',
    '15': '新潟県',
    '16': '富山県',
    '17': '石川県',
    '18': '福井県',
    '19': '山梨県',
    '20': '長野県',
    '21': '岐阜県',
    '22': '静岡県',
    '23': '愛知県',
    '24': '三重県',
    '25': '滋賀県',
    '26': '京都府',
    '27': '大阪府',
    '28': '兵庫県',
    '29': '奈良県',
    '30': '和歌山県',
    '31': '鳥取県',
    '32': '島根県',
    '33': '岡山県',
    '34': '広島県',
    '35': '山口県',
    '36': '徳島県',
    '37': '香川県',
    '38': '愛媛県',
    '39': '高知県',
    '40': '福岡県',
    '41': '佐賀県',
    '42': '長崎県',
    '43': '熊本県',
    '44': '大分県',
    '45': '宮崎県',
    '46': '鹿児島県',
    '47': '沖縄県',
    '99': 'その他'
};

// Define the required columns for our output
const REQUIRED_COLUMNS = [
    'parent_id',
    'kname1',
    'kname2',
    'fname1',
    'fname2',
    'portable_email',
    'email',
    'portable_tel',
    'tel',
    'zip_cd',
    'pref_id',
    'address1',
    'address2',
    'address3',
    'combined_info',
    'gender',
    'tag'
];

// Function to remove dashes from a string
function removeDashes(str) {
    if (!str) return str;
    return String(str).replace(/-/g, '');
}

// Transform prefecture values
function transformPrefecture(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return PREFECTURE_MAPPING[strValue] || value;
}

// Transform and derive email addresses
function deriveEmailAddresses(record) {
    const email = record['email'];
    const portableEmail = record['portable_email'];
    
    let email1 = null;
    let email2 = null;

    // Use explicit existence checks rather than truthiness
    const hasEmail = email !== undefined && email !== null && email !== '';
    const hasPortableEmail = portableEmail !== undefined && portableEmail !== null && portableEmail !== '';

    // Both emails exist
    if (hasEmail && hasPortableEmail) {
        email1 = portableEmail;  // portable_email is Email1
        email2 = email;          // email is Email2
    }
    // Only email exists
    else if (hasEmail) {
        email1 = email;          // email is Email1
    }
    // Only portable_email exists
    else if (hasPortableEmail) {
        email1 = portableEmail;  // portable_email is Email1
    }
    // Both are NULL - both remain null

    return {
        email1,
        email2
    };
}

// Transform and derive phone numbers
function derivePhoneNumbers(record) {
    const tel = record['tel'];
    const portableTel = record['portable_tel'];
    
    let phone = null;
    let otherPhone = null;

    // Use explicit existence checks rather than truthiness
    const hasTel = tel !== undefined && tel !== null && tel !== '';
    const hasPortableTel = portableTel !== undefined && portableTel !== null && portableTel !== '';

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
        if (record.phone !== undefined && record.phone !== null && record.phone !== '') {
            phone = record.phone;
        }
        if (record.other_phone !== undefined && record.other_phone !== null && record.other_phone !== '') {
            otherPhone = record.other_phone;
        }
    }

    return {
        phone,
        other_phone: otherPhone
    };
}

function filterColumns(record, mappingStats = {}, dataQualityTracker = null, counters = {}) {
    const filteredRecord = {};
    
    // Get derived phone numbers and email addresses
    const phoneNumbers = derivePhoneNumbers(record);
    const emailAddresses = deriveEmailAddresses(record);
    
    // Verify phone number logic compliance
    verifyPhoneNumberLogic(record, phoneNumbers, counters);
    
    // Verify email logic compliance
    verifyEmailLogic(record, emailAddresses, counters);
    
    // Create combined info field with line breaks, but only for non-empty values
    const employment = record['employment'] !== undefined ? record['employment'] : '';
    const employmentTel = record['employment_tel'] !== undefined ? record['employment_tel'] : '';
    const comment = record['comment'] !== undefined ? record['comment'] : '';
    
    // Filter out empty values and combine the remaining ones with line breaks
    const valuesToCombine = [employment, employmentTel, comment].filter(value => 
        value !== null && value !== undefined && value !== ''
    );
    
    // Join the non-empty values with forward slashes
    const combinedInfo = valuesToCombine.join('/');
    
    // Create a modified record with the combined field
    const modifiedRecord = {
        ...record,
        combined_info: combinedInfo,
        gender: '不明',
        tag: TAG_VALUE // Alternate between phase1 and ,phase1 based on parent_id
    };
    
    REQUIRED_COLUMNS.forEach(column => {
        let value;
        let originalValueForQuality;
        
        // Handle special columns
        if (column === 'tel') {
            value = phoneNumbers.phone;
            originalValueForQuality = record['tel'] || record['portable_tel'];
        } else if (column === 'portable_tel') {
            value = phoneNumbers.other_phone;
            originalValueForQuality = record['tel'] || record['portable_tel'];
        } else if (column === 'email') {
            value = emailAddresses.email2;  // email becomes Email2 when both exist, otherwise null
            originalValueForQuality = record['email'] || record['portable_email'];
        } else if (column === 'portable_email') {
            value = emailAddresses.email1;  // portable_email becomes Email1 when it exists
            originalValueForQuality = record['email'] || record['portable_email'];
        } else {
            value = modifiedRecord[column] !== undefined ? modifiedRecord[column] : '';
            originalValueForQuality = modifiedRecord[column];
        }
        
        // Apply transformations with mapping statistics tracking
        if (column === 'pref_id') {
            const originalValue = value;
            value = transformPrefecture(value);
            value = trackMappingStats(originalValue, value, mappingStats.prefecture, 'pref_id');
        } else if (column === 'zip_cd') {
            const originalValue = value;
            // Remove dashes from zip code
            value = removeDashes(value);
            value = trackMappingStats(originalValue, value, mappingStats.zipCode, 'zip_cd');
        } else if (column === 'gender') {
            const originalValue = value;
            value = trackMappingStats(originalValue, value, mappingStats.gender, 'gender');
        } else if (column === 'parent_id') {
            // Add 'p' prefix to parent_id
            // if (value !== null && value !== undefined && value !== '') {
            //     value = 'p' + value;
            // }
        } else if (column === 'FirstName' || column === 'LastName' || column === 'fname1' || column === 'fname2' || column === 'kname1' || column === 'kname2') {
            // Replace empty firstname or lastname with "不明"
            if (!value || (typeof value === 'string' && value.trim() === '')) {
                value = '不明';
            }
        }
        
        // Track data quality for this column (after all transformations)
        trackDataQuality(column, originalValueForQuality, value, dataQualityTracker);
        
        filteredRecord[column] = value;
    });
    
    return filteredRecord;
}

/**
 * Pre-processes CSV content to fix common quote issues
 * @param {string} content - The CSV content to pre-process
 * @returns {string} - The processed content
 */
function preprocessCSVContent(content) {
    // Split into lines
    const lines = content.split('\n');
    
    // Get the expected number of fields from the header
    const headerFields = lines[0].split(',').length;
    
    // Process each line
    return lines.map((line, index) => {
        // Skip empty lines
        if (!line.trim()) return line;
        
        // First, properly handle quoted fields that might contain commas
        let processedLine = '';
        let inQuotes = false;
        let currentField = '';
        let fieldCount = 0;
        
        for (let i = 0; i < line.length; i++) {
            const char = line[i];
            
            if (char === '"') {
                if (!inQuotes) {
                    inQuotes = true;
                    currentField += char;
                } else if (i + 1 < line.length && line[i + 1] === '"') {
                    // Handle escaped quotes
                    currentField += '""';
                    i++; // Skip the next quote
                } else {
                    inQuotes = false;
                    currentField += char;
                }
            } else if (char === ',' && !inQuotes) {
                // End of field
                processedLine += currentField + ',';
                currentField = '';
                fieldCount++;
            } else {
                currentField += char;
            }
        }
        
        // Add the last field
        processedLine += currentField;
        fieldCount++;
        
        // If this is not the header row and we have too many fields, try to fix it
        if (index > 0 && fieldCount > headerFields) {
            console.warn(`Warning: Line ${index + 1} has ${fieldCount} fields (expected ${headerFields}). Attempting to fix...`);
            
            // Split the line into fields
            const fields = processedLine.split(',');
            
            // If we have too many fields, try to merge the extra fields
            if (fields.length > headerFields) {
                // Find the last field that contains a quote
                let lastQuotedFieldIndex = -1;
                for (let i = fields.length - 1; i >= 0; i--) {
                    if (fields[i].includes('"')) {
                        lastQuotedFieldIndex = i;
                        break;
                    }
                }
                
                // If we found a quoted field, merge all fields after it
                if (lastQuotedFieldIndex !== -1) {
                    const extraFields = fields.slice(lastQuotedFieldIndex + 1);
                    fields[lastQuotedFieldIndex] = fields[lastQuotedFieldIndex] + ',' + extraFields.join(',');
                    fields.splice(lastQuotedFieldIndex + 1, extraFields.length);
                }
                
                // If we still have too many fields, merge the last fields
                while (fields.length > headerFields) {
                    fields[fields.length - 2] = fields[fields.length - 2] + ',' + fields[fields.length - 1];
                    fields.pop();
                }
                
                processedLine = fields.join(',');
            }
        }
        
        return processedLine;
    }).join('\n');
}

// Reading CSV with robust parsing for quote handling
async function readAndParseCSV(filename) {
    try {
        let content = await fs.readFile(filename, 'utf8');
        
        // Pre-process the content to fix quote issues
        console.log('   Pre-processing CSV content to fix quote issues...');
        content = preprocessCSVContent(content);
        
        return new Promise((resolve, reject) => {
            Papa.parse(content, {
                header: true,
                skipEmptyLines: true,
                dynamicTyping: false,  // Changed to false to avoid type conversion issues
                delimiter: ",",
                newline: "\n",
                transform: (value) => {
                    if (typeof value === 'string') {
                        // Clean up malformed quotes and normalize the value
                        return value
                            .replace(/^"|"$/g, '') // Remove surrounding quotes
                            .replace(/""/g, '"')   // Convert double quotes to single
                            .replace(/\r/g, '')    // Remove carriage returns
                            .trim();               // Remove extra whitespace
                    }
                    return value;
                },
                transformHeader: (header) => {
                    // Clean up header names
                    return header
                        .trim()
                        .replace(/^"|"$/g, '')
                        .replace(/\r/g, '');
                },
                // More lenient parsing options
                comments: false,
                delimitersToGuess: [',', '\t', '|', ';', Papa.RECORD_SEP, Papa.UNIT_SEP],
                // Handle quotes more flexibly
                quoteChar: '"',
                escapeChar: '"',
                // Skip empty lines and handle errors
                skipEmptyLines: true,
                complete: (results) => {
                    if (results.errors.length > 0) {
                        console.log(`   ⚠️  Warnings while parsing ${filename}:`, results.errors);
                        // Continue processing even with warnings unless they're critical
                    }
                    resolve(results);
                },
                error: (error) => {
                    console.warn(`Warning: CSV parsing issue: ${error.message}`);
                    // Don't reject on parsing warnings, continue processing
                }
            });
        });
    } catch (error) {
        console.error(`\n❌ Error reading ${filename}:`, error);
        throw error;
    }
}

// Process parent data in chunks
async function processParentData(parentData) {
    try {
        // Initialize tracking counters
        let processedCount = 0;
        
        // Initialize statistics trackers
        let allMappingStats = createAllMappingStats();
        let dataQualityTracker = createDataQualityTracker();
        
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
        
        // Initialize final output file with headers
        const finalFilename = 'processed_parent_data.csv';
        const headers = REQUIRED_COLUMNS.join(',') + '\n';
        await fs.writeFile(finalFilename, headers);
        
        // Process parent data in chunks
        const chunks = _.chunk(parentData.data, CHUNK_SIZE);
        
        for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];
            console.log(`\n   Processing parent chunk ${i + 1} of ${chunks.length}`);
            
            const processedChunk = chunk.map(parent => {
                // Apply transformations with statistics tracking
                const counters = { phoneRuleStats, emailRuleStats };
                return filterColumns(parent, allMappingStats, dataQualityTracker, counters);
            });
            
            // Convert chunk to CSV with more robust quoting and newline handling
            const csv = processedChunk.map(record => 
                REQUIRED_COLUMNS.map(column => {
                    const value = record[column];
                    
                    // Handle null/undefined values consistently
                    if (value === null || value === undefined) {
                        return '""';  // Quote empty values for consistency
                    }
                    
                    // Convert to string for processing
                    const stringValue = String(value);
                    
                    // Clean up malformed data from input CSV
                    let cleanedValue = stringValue
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
                    
                    // Always quote the combined_info field
                    if (column === 'combined_info') {
                        return `"${escapedValue}"`;
                    }
                    
                    // For other fields, always quote string values to prevent field splitting
                    // Only leave numeric values unquoted if they're actually numbers
                    if (typeof value === 'number' && !isNaN(value)) {
                        return value;
                    } else {
                        // Quote all string values to prevent CSV parsing issues
                        return `"${escapedValue}"`;
                    }
                }).join(',')
            ).join('\n');
            
            if (processedChunk.length > 0) {
                await fs.appendFile(finalFilename, csv + '\n');
            }
            
            processedCount += processedChunk.length;
            console.log(`   ✓ Processed ${processedCount}/${parentData.data.length} parent records`);
            
            // Progress reporting with rule compliance
            const totalPhoneRecords = phoneRuleStats.rule1_both_exist + phoneRuleStats.rule2_tel_only + phoneRuleStats.rule3_portable_only + phoneRuleStats.rule4_both_null;
            const totalPhoneCompliant = phoneRuleStats.rule1_compliant + phoneRuleStats.rule2_compliant + phoneRuleStats.rule3_compliant + phoneRuleStats.rule4_compliant;
            const phoneComplianceRate = totalPhoneRecords > 0 ? ((totalPhoneCompliant / totalPhoneRecords) * 100).toFixed(1) : '0.0';
            
            const totalEmailRecords = emailRuleStats.rule1_both_exist + emailRuleStats.rule2_email_only + emailRuleStats.rule3_portable_only + emailRuleStats.rule4_both_null;
            const totalEmailCompliant = emailRuleStats.rule1_compliant + emailRuleStats.rule2_compliant + emailRuleStats.rule3_compliant + emailRuleStats.rule4_compliant;
            const emailComplianceRate = totalEmailRecords > 0 ? ((totalEmailCompliant / totalEmailRecords) * 100).toFixed(1) : '0.0';
            
            console.log(`   ✓ Phone rule compliance: ${totalPhoneCompliant}/${totalPhoneRecords} (${phoneComplianceRate}%)`);
            console.log(`   ✓ Email rule compliance: ${totalEmailCompliant}/${totalEmailRecords} (${emailComplianceRate}%)`);
            console.log(`   ✓ Mapping stats: Prefecture(${allMappingStats.prefecture.mappings.size}) Gender(${allMappingStats.gender.mappings.size})`);
            
            const originalIssues = dataQualityTracker.overallStats.original.emptyValues + dataQualityTracker.overallStats.original.nullValues + dataQualityTracker.overallStats.original.invalidValues;
            const outputIssues = dataQualityTracker.overallStats.output.emptyValues + dataQualityTracker.overallStats.output.nullValues + dataQualityTracker.overallStats.output.invalidValues;
            console.log(`   ✓ Data quality: ${dataQualityTracker.totalFields} fields, ${originalIssues}→${outputIssues} issues (fixed ${originalIssues - outputIssues})`);
            
            // Free memory
            processedChunk.length = 0;
            
            // Log memory usage
            const currentMemory = process.memoryUsage();
            console.log(`   Current heap used: ${Math.round(currentMemory.heapUsed / 1024 / 1024)}MB`);
        }
        
        // Free memory
        chunks.length = 0;
        
        // Comprehensive Summary Reporting
        console.log('\n📊 Processing Summary:');
        console.log('=====================');
        console.log(`Total parent records processed: ${processedCount}`);
        
        // Phone rule compliance summary
        const totalPhoneRecords = phoneRuleStats.rule1_both_exist + phoneRuleStats.rule2_tel_only + phoneRuleStats.rule3_portable_only + phoneRuleStats.rule4_both_null;
        const totalPhoneCompliant = phoneRuleStats.rule1_compliant + phoneRuleStats.rule2_compliant + phoneRuleStats.rule3_compliant + phoneRuleStats.rule4_compliant;
        const phoneComplianceRate = totalPhoneRecords > 0 ? ((totalPhoneCompliant / totalPhoneRecords) * 100).toFixed(1) : '0.0';
        
        console.log(`\n📞 Phone Number Rule Compliance Summary:`);
        console.log(`├── Total records processed: ${totalPhoneRecords}`);
        console.log(`├── Compliant records: ${totalPhoneCompliant} (${phoneComplianceRate}%)`);
        console.log(`├── Rule 1 (both tel & portable_tel): ${phoneRuleStats.rule1_both_exist} total, ${phoneRuleStats.rule1_compliant} compliant, ${phoneRuleStats.rule1_non_compliant} non-compliant`);
        console.log(`├── Rule 2 (tel only): ${phoneRuleStats.rule2_tel_only} total, ${phoneRuleStats.rule2_compliant} compliant, ${phoneRuleStats.rule2_non_compliant} non-compliant`);
        console.log(`├── Rule 3 (portable_tel only): ${phoneRuleStats.rule3_portable_only} total, ${phoneRuleStats.rule3_compliant} compliant, ${phoneRuleStats.rule3_non_compliant} non-compliant`);
        console.log(`└── Rule 4 (both NULL): ${phoneRuleStats.rule4_both_null} total, ${phoneRuleStats.rule4_compliant} compliant, ${phoneRuleStats.rule4_non_compliant} non-compliant`);
        
        // Email rule compliance summary
        const totalEmailRecords = emailRuleStats.rule1_both_exist + emailRuleStats.rule2_email_only + emailRuleStats.rule3_portable_only + emailRuleStats.rule4_both_null;
        const totalEmailCompliant = emailRuleStats.rule1_compliant + emailRuleStats.rule2_compliant + emailRuleStats.rule3_compliant + emailRuleStats.rule4_compliant;
        const emailComplianceRate = totalEmailRecords > 0 ? ((totalEmailCompliant / totalEmailRecords) * 100).toFixed(1) : '0.0';
        
        console.log(`\n📧 Email Rule Compliance Summary:`);
        console.log(`├── Total records processed: ${totalEmailRecords}`);
        console.log(`├── Compliant records: ${totalEmailCompliant} (${emailComplianceRate}%)`);
        console.log(`├── Rule 1 (both email & portable_email): ${emailRuleStats.rule1_both_exist} total, ${emailRuleStats.rule1_compliant} compliant, ${emailRuleStats.rule1_non_compliant} non-compliant`);
        console.log(`├── Rule 2 (email only): ${emailRuleStats.rule2_email_only} total, ${emailRuleStats.rule2_compliant} compliant, ${emailRuleStats.rule2_non_compliant} non-compliant`);
        console.log(`├── Rule 3 (portable_email only): ${emailRuleStats.rule3_portable_only} total, ${emailRuleStats.rule3_compliant} compliant, ${emailRuleStats.rule3_non_compliant} non-compliant`);
        console.log(`└── Rule 4 (both NULL): ${emailRuleStats.rule4_both_null} total, ${emailRuleStats.rule4_compliant} compliant, ${emailRuleStats.rule4_non_compliant} non-compliant`);
        
        // Show all mapping statistics
        displayAllMappingStats(allMappingStats, processedCount);
        
        // Set total records for data quality tracker
        dataQualityTracker.totalRecords = processedCount;
        
        // Show data quality statistics
        displayDataQualityStats(dataQualityTracker, processedCount);

        return {
            finalFilename,
            processedCount
        };
    } catch (error) {
        console.error(`Error processing parent data: ${error.message}`);
        throw error;
    }
}

async function processParentFile(filename) {
    try {
        console.log('Starting parent data processing...');
        const initialMemoryUsage = process.memoryUsage();
        console.log(`Initial memory usage: ${Math.round(initialMemoryUsage.heapUsed / 1024 / 1024)}MB`);

        // Step 1: Read parent CSV file
        console.log('\n1. Reading parent.csv...');
        const parentData = await readAndParseCSV(filename);
        console.log(`   ✓ Found ${parentData.data.length} parent records`);

        // Step 2: Process and transform the data
        console.log('\n2. Processing parent data...');
        const result = await processParentData(parentData);
        
        // Free memory
        parentData.data = [];

        // Print final summary
        console.log('\nFinal Summary:');
        console.log('-------------');
        console.log(`Total processed records in final output: ${result.processedCount}`);
        console.log(`Output file: ${result.finalFilename}`);
        
        const finalMemory = process.memoryUsage();
        console.log(`Final memory usage: ${Math.round(finalMemory.heapUsed / 1024 / 1024)}MB`);
        console.log('\nProcess completed successfully! ✨');

        return result.finalFilename;
    } catch (error) {
        console.error('\n❌ Error processing data:', error);
        throw error;
    }
}

// Function to get output column names
function getOutputColumnNames() {
    return REQUIRED_COLUMNS.map(column => OUTPUT_COLUMN_MAPPING[column] || column);
}

// Function to rename columns in the final output file
async function renameColumnsInFinalOutput(filename) {
    try {
        console.log('\nPerforming final column name transformation...');
        
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
        const newHeaderLine = getOutputColumnNames().join(',');
        
        // Process each data line to ensure Description is quoted
        const processedLines = lines.map((line, index) => {
            if (index === 0) return newHeaderLine; // Return new header line
            
            const columns = line.split(',');
            const descriptionIndex = headers.indexOf('combined_info');
            if (descriptionIndex !== -1 && columns[descriptionIndex]) {
                // Ensure the Description field is properly quoted
                columns[descriptionIndex] = `"${columns[descriptionIndex].replace(/"/g, '""')}"`;
            }
            return columns.join(',');
        });
        
        // Write back to file
        await fs.writeFile(filename, processedLines.join('\n'), 'utf8');
        
        console.log(`   ✓ Successfully renamed columns in ${filename}`);
    } catch (error) {
        console.error(`   ❌ Error renaming columns: ${error.message}`);
    }
}

async function main() {
    console.log('🚀 Starting parent data processing...\n');
    const startTime = Date.now();
    
    try {
        // Parse command line arguments
        const args = process.argv.slice(2);
        const shouldMask = args.includes('--mask');
        
        // Remove the --mask flag from args to get the input file
        const inputFileArg = args.filter(arg => arg !== '--mask')[0];
        const inputFile = inputFileArg || 'parent.csv';
        
        console.log(`Using input file: ${inputFile}`);
        console.log(`Masking enabled: ${shouldMask}`);
        
        // Step 1: Process parent data
        const outputFilename = await processParentFile(inputFile);
        
        // Step 2: Rename columns
        await renameColumnsInFinalOutput(outputFilename);
        
        // Step 3: Run fix-line.js
        console.log('\nRunning fix-line.js to process the final output...');
        const fixedFilename = fixNextLine(outputFilename);
        if (!fixedFilename) {
            throw new Error('Failed to process file with fix-line.js');
        }

        // Step 4: Run masking as the last step if flag is present
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
            
            // Step 5: Verify masking integrity
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