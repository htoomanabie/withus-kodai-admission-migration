import { promises as fs } from 'fs';
import Papa from 'papaparse';

// Configuration
const PLACEHOLDER_DOMAIN = 'withusmanabie.com';

// Masking configuration: maps column names to masking types
// Types: 'email', 'phone', 'name', or null (to skip masking)
//
// CUSTOMIZATION EXAMPLES:
// =====================
// 
// 1. Add a new column to mask:
//    { column: 'NewColumn', type: 'email' }
//
// 2. Skip masking for a specific column:
//    { column: 'LastName', type: null }
//
// 3. Change masking type for existing column:
//    { column: 'Phone', type: 'name' }  // Treat phone as name instead
//
// 4. Add multiple columns at once:
//    { column: 'ParentName', type: 'name' },
//    { column: 'ParentEmail', type: 'email' },
//    { column: 'ParentPhone', type: 'phone' },
//
// MASKING BEHAVIOR:
// ================
// - 'email': Masks username part, replaces domain with withusmanabie.com
//   Example: "john.doe@gmail.com" → "jo5R8Kl2@withusmanabie.com"
//
// - 'phone': Keeps last 3 digits, masks the rest with random digits
//   Example: "08012345678" → "12945671678"
//
// - 'name': Keeps first character, masks rest with random characters
//   Example: "田中太郎" → "田Kx8", "Tanaka" → "TnR5w"
//
const MASKING_CONFIG = [
    // User identification
    { column: 'MANAERP__Username__c', type: null },
    
    // Names
    { column: 'LastName', type: 'name' },
    { column: 'FirstName', type: 'name' },
    { column: 'MANAERP__Last_Name_Phonetic__c', type: 'name' },
    { column: 'MANAERP__First_Name_Phonetic__c', type: 'name' },
    
    // Contact information
    { column: 'Phone', type: 'phone' },
    { column: 'OtherPhone', type: 'phone' },
    { column: 'Email', type: 'email' },
    { column: 'Sub_Email__c', type: 'email' },
    
    // Skipped columns. It might need to mask for specific cases.
    { column: 'MANAERP__Postal_Code__c', type: null },
    { column: 'MANAERP__City__c', type: null },
    { column: 'MANAERP__Street_1__c', type: null },
    { column: 'MANAERP__Street_2__c', type: null },
    
    // ========================================
    // ADD NEW COLUMNS HERE:
    // ========================================
    // { column: 'YourColumnName', type: 'email' },     // Email masking
    // { column: 'YourColumnName', type: 'phone' },     // Phone masking  
    // { column: 'YourColumnName', type: 'name' },      // Name/text masking
    // { column: 'YourColumnName', type: null },        // Skip masking entirely
    //
    // IMPORTANT: Column names must match exactly (case-sensitive)
];

/**
 * Creates a lookup map from the masking configuration
 * @returns {Map} - Map of column name to masking type
 */
function createMaskingMap() {
    const map = new Map();
    MASKING_CONFIG.forEach(config => {
        if (config.type !== null) {  // Only add columns that should be masked
            map.set(config.column, config.type);
        }
    });
    return map;
}

/**
 * Generates random characters for masking
 * @param {number} length - Length of random string to generate
 * @returns {string} - Random string
 */
function generateRandomChars(length) {
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

/**
 * Generates random digits for phone numbers
 * @param {number} length - Length of random digits to generate
 * @returns {string} - Random digits
 */
function generateRandomDigits(length) {
    let result = '';
    for (let i = 0; i < length; i++) {
        result += Math.floor(Math.random() * 10);
    }
    return result;
}

/**
 * Masks an email address
 * @param {string} email - The email to mask
 * @returns {string} - The masked email
 */
function maskEmail(email) {
    if (!email || typeof email !== 'string') return email;
    
    const trimmed = email.trim();
    if (!trimmed) return trimmed;
    
    // Check if it contains @
    if (!trimmed.includes('@')) {
        // Not an email format, treat as regular text
        return maskName(trimmed);
    }
    
    const atIndex = trimmed.indexOf('@');
    const username = trimmed.substring(0, atIndex);
    
    // Mask username: keep first character if it exists, mask the rest
    let maskedUsername;
    if (username.length <= 1) {
        maskedUsername = username;
    } else if (username.length <= 3) {
        maskedUsername = username.charAt(0) + generateRandomChars(username.length - 1);
    } else {
        maskedUsername = username.substring(0, 3) + generateRandomChars(username.length - 2);
    }
    
    return `${maskedUsername}@${PLACEHOLDER_DOMAIN}`;
}

/**
 * Masks a phone number
 * @param {string} phone - The phone to mask
 * @returns {string} - The masked phone
 */
function maskPhone(phone) {
    if (!phone || typeof phone !== 'string') return phone;
    
    const trimmed = phone.trim();
    if (!trimmed) return trimmed;
    
    // Remove all non-digits to check if it's a phone number
    const digitsOnly = trimmed.replace(/\D/g, '');
    
    if (digitsOnly.length < 4) {
        // Too short to be a meaningful phone number, don't mask
        return trimmed;
    }
    
    // Keep the last 3 digits, mask the rest with random digits
    const maskedDigits = generateRandomDigits(digitsOnly.length - 3) + digitsOnly.slice(-3);
    
    // Try to preserve the original format by replacing digits in the original string
    let result = trimmed;
    let maskedIndex = 0;
    
    for (let i = 0; i < result.length; i++) {
        if (/\d/.test(result[i])) {
            if (maskedIndex < maskedDigits.length) {
                result = result.substring(0, i) + maskedDigits[maskedIndex] + result.substring(i + 1);
                maskedIndex++;
            }
        }
    }
    
    return result;
}

/**
 * Masks text (names, addresses, etc.) - preserves Japanese characters properly
 * @param {string} text - The text to mask
 * @returns {string} - The masked text
 */
function maskName(text) {
    if (!text || typeof text !== 'string') return text;
    
    const trimmed = text.trim();
    if (!trimmed) return trimmed;
    
    // For very short text (1 character), don't mask to avoid over-masking
    if (trimmed.length <= 1) {
        return trimmed;
    }
    
    // For names and other sensitive text, always mask regardless of length
    // Keep the first character, mask the rest
    // This works well for both ASCII and Japanese characters
    const firstChar = trimmed.charAt(0);
    const maskedPart = generateRandomChars(Math.min(trimmed.length - 1, 8)); // Limit length to avoid overly long masked values
    
    return firstChar + maskedPart;
}

/**
 * Applies the appropriate masking based on the masking type
 * @param {any} value - The value to mask
 * @param {string} maskingType - The type of masking to apply ('email', 'phone', 'name')
 * @returns {any} - The masked value
 */
function applyMasking(value, maskingType) {
    // Don't mask null, undefined, empty values, or 'null' strings
    if (value === null || value === undefined || value === '' || value === 'null') {
        return value;
    }
    
    // Convert to string for processing
    const stringValue = String(value).trim();
    
    // Don't mask if the trimmed string is empty
    if (!stringValue) {
        return value;
    }
    
    // Apply the specified masking type
    switch (maskingType) {
        case 'email':
            return maskEmail(stringValue);
        case 'phone':
            return maskPhone(stringValue);
        case 'name':
            return maskName(stringValue);
        default:
            console.warn(`Unknown masking type: ${maskingType}`);
            return value;
    }
}

/**
 * Preprocesses CSV content to fix common formatting issues
 * @param {string} content - Raw CSV content
 * @returns {string} - Cleaned CSV content
 */
function preprocessCSVContent(content) {
    // Fix common line break issues
    let processed = content;
    
    // Ensure lines end with proper newlines - look for patterns where a record
    // ends with "phase22" followed immediately by a number (start of next record)
    processed = processed.replace(/"phase22"\s*(\d)/g, '"phase22"\n$1');
    
    // Also handle cases where lines might end with other tag patterns
    processed = processed.replace(/"TRUE"\s*"phase22"\s*(\d)/g, '"TRUE","phase22"\n$1');
    
    // Fix any remaining cases where lines run together (number followed immediately by quote)
    processed = processed.replace(/(\d)"([^,])/g, '$1\n"$2');
    
    return processed;
}

/**
 * Main function to mask data in a CSV file
 * @param {string} inputFile - Path to the input CSV file
 * @returns {Object} - Result object with success status and details
 */
async function maskData(inputFile) {
    try {
        console.log(`Starting data masking for ${inputFile}...`);
        
        // Create masking lookup map
        const maskingMap = createMaskingMap();
        
        // Read and preprocess the CSV file
        let content = await fs.readFile(inputFile, 'utf8');
        
        // Apply preprocessing to fix line break issues
        content = preprocessCSVContent(content);
        
        const results = Papa.parse(content, {
            header: true,
            skipEmptyLines: true,
            dynamicTyping: false, // Keep everything as strings to preserve formatting
            encoding: 'utf8',
            // Handle errors more gracefully
            transform: (value, column) => {
                // Clean up any stray characters
                if (typeof value === 'string') {
                    return value.trim();
                }
                return value;
            },
            // More lenient error handling
            beforeFirstChunk: function(chunk) {
                // Clean up the chunk before parsing
                return chunk;
            },
            // Custom error handling
            error: function(error) {
                // Only show critical errors, not field mismatch warnings
                if (error.code !== 'TooFewFields' && error.code !== 'TooManyFields') {
                    console.warn(`CSV parsing error on row ${error.row}:`, error.message);
                }
            }
        });
        
        // Filter out only critical errors
        const criticalErrors = results.errors.filter(error => 
            error.code !== 'TooFewFields' && 
            error.code !== 'TooManyFields' &&
            error.code !== 'InvalidQuotes'
        );
        
        if (criticalErrors.length > 0) {
            console.warn('Critical CSV parsing errors:', criticalErrors);
        }
        
        // Show field mismatch summary without overwhelming detail
        const fieldMismatchErrors = results.errors.filter(error => 
            error.code === 'TooFewFields' || error.code === 'TooManyFields'
        );
        
        if (fieldMismatchErrors.length > 0) {
            console.warn(`Note: ${fieldMismatchErrors.length} rows have field count mismatches (will be processed as-is)`);
        }
        
        if (!results.data || results.data.length === 0) {
            throw new Error('No data found in the CSV file');
        }
        
        const originalHeaders = results.meta.fields;
        console.log(`Found ${originalHeaders.length} columns and ${results.data.length} records`);
        
        // Identify columns to be masked based on configuration
        const columnsToMask = originalHeaders.filter(column => maskingMap.has(column));
        const maskingStats = {};
        columnsToMask.forEach(column => {
            const type = maskingMap.get(column);
            if (!maskingStats[type]) {
                maskingStats[type] = [];
            }
            maskingStats[type].push(column);
        });
        
        console.log(`Identified ${columnsToMask.length} columns to mask:`);
        Object.entries(maskingStats).forEach(([type, columns]) => {
            console.log(`  ${type.toUpperCase()}: ${columns.join(', ')}`);
        });
        
        // Process the data
        console.log('Masking sensitive data...');
        const maskedData = results.data.map((record, index) => {
            const maskedRecord = { ...record };
            
            // Apply masking based on configuration
            columnsToMask.forEach(column => {
                if (record.hasOwnProperty(column)) {
                    const maskingType = maskingMap.get(column);
                    maskedRecord[column] = applyMasking(record[column], maskingType);
                }
            });
            
            return maskedRecord;
        });
        
        // Create output filename
        const outputFile = inputFile.replace('.csv', '_masked.csv');
        
        // Convert back to CSV
        const outputCsv = Papa.unparse(maskedData, {
            header: true,
            columns: originalHeaders,
            quotes: true,
            delimiter: ',',
            newline: '\n'
        });
        
        // Write the masked data
        await fs.writeFile(outputFile, outputCsv, 'utf8');
        
        console.log(`Successfully masked data and saved to ${outputFile}`);
        
        return {
            success: true,
            totalRecords: maskedData.length,
            maskedColumns: columnsToMask,
            outputFile: outputFile,
            originalFile: inputFile,
            maskingStats: maskingStats
        };
        
    } catch (error) {
        console.error('Error during masking:', error.message);
        return {
            success: false,
            error: error.message
        };
    }
}

/**
 * Main function for command line usage
 */
async function main() {
    const args = process.argv.slice(2);
    const inputFile = args[0];
    
    if (!inputFile) {
        console.error('Usage: node mask-data.js <input-file.csv>');
        process.exit(1);
    }
    
    console.log('🎭 Configurable Data Masking Tool');
    console.log('==================================');
    
    const result = await maskData(inputFile);
    
    if (result.success) {
        console.log('\n📊 Masking Summary:');
        console.log(`✅ Records processed: ${result.totalRecords}`);
        console.log(`🔒 Columns masked: ${result.maskedColumns.length}`);
        
        // Show breakdown by masking type
        if (result.maskingStats) {
            Object.entries(result.maskingStats).forEach(([type, columns]) => {
                console.log(`   ${type.toUpperCase()}: ${columns.length} columns`);
            });
        }
        
        console.log(`📁 Output file: ${result.outputFile}`);
        console.log('\n🎉 Masking completed successfully!');
    } else {
        console.error('\n❌ Masking failed:', result.error);
        process.exit(1);
    }
}

// Export the maskData function
export { maskData };

// Run main if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
    main();
}