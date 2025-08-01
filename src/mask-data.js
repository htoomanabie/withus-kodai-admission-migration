import { promises as fs } from 'fs';
import path from 'path';
import Papa from 'papaparse';
import _ from 'lodash';

// Constants
const SOURCE_FOLDER = 'split_files';
const TARGET_FOLDER = 'mask_files';
const PLACEHOLDER_DOMAIN = 'withusmanabie.com';

// Define columns that need to be masked
const MASKED_COLUMNS = [
    'Last Name',
    'First Name',
    'Last Name (Phonetic)',
    'First Name (Phonetic)',
    'LastName',
    'FirstName',
    'MANAERP__Last_Name_Phonetic__c',
    'MANAERP__First_Name_Phonetic__c',
    'Phone',
    'Other Phone',
    'portable_tel',
    'tel',
    'Email',
    'Parent Email',
    'portable_email',
    'main_email',
    'Sub Email',
    'Postal Code',
    'City',
    'Street 1',
    'Street 2',
    'Web App Log in Id',
    'Parent Email',
    'Student PC address',
    'Student First Name', 
    'Student Last Name',
    'Student First Name (Phonetic)',
    'Student Last Name (Phonetic)',
    'Parent Phone Number',
    'Parent cell phone number',
    'Inquirer\'s First Name',
    'Inquirer\'s First Name (Phonetic)',
    'Inquirer\'s Last Name',
    'Inquirer\'s Last Name (Phonetic)',
    'Parent Family Name',
    'Parent First Name',
    'Parent Family Name(katakana)',
    'Parent First Name(katakana)',
    'Web App Log in Id',
    'Student PC address'
];

// Helper function to generate random characters
function generateRandomChars(length) {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

// Helper function to generate random integers
function generateRandomIntegers(length) {
  let result = '';
  for (let i = 0; i < length; i++) {
    result += Math.floor(Math.random() * 10);
  }
  return result;
}

/**
 * Masks a name according to privacy rules
 * @param {string} name - The name to mask
 * @param {boolean} preserveLength - Whether to preserve the original length
 * @returns {string} - The masked name
 */
function maskName(name, preserveLength = true) {
  if (!name) return '';
  
  // If name is not a string, convert to string
  const nameStr = String(name).trim();
  
  // If empty after trimming, return empty string
  if (!nameStr) return '';
  
  // For very short names (1 character), don't mask
  if (nameStr.length <= 1) {
    return nameStr;
  }
  
  // For longer names, preserve only the first character, mask everything else
  // If preserveLength is true, generate exactly the same length as original
  const firstChar = nameStr.charAt(0);
  
  const maskedPart = preserveLength 
    ? generateRandomChars(nameStr.length - 1)
    : generateRandomChars(7);
    
  return firstChar + maskedPart;
}

/**
 * Masks an email address using a fixed domain
 * @param {string} email - The email to mask
 * @returns {string} - The masked email
 */
function maskEmail(email) {
  if (!email) return '';
  
  // If email is not a string, convert to string
  const emailStr = String(email).trim();
  
  // If empty after trimming, return empty string
  if (!emailStr) return '';
  
  // Check if it's a valid email format
  const emailRegex = /^([^@\s]+)@((?:[-a-z0-9]+\.)+[a-z]{2,})$/i;
  const match = emailStr.match(emailRegex);
  
  if (!match) {
    // If not a standard email format, use basic masking
    if (emailStr.length <= 3) return `${generateRandomChars(3)}@${PLACEHOLDER_DOMAIN}`;
    // Keep first 3 characters if possible, otherwise keep what we have and mask the rest
    const preserveChars = Math.min(3, emailStr.length);
    const preserved = emailStr.slice(0, preserveChars);
    const masked = generateRandomChars(Math.max(0, emailStr.length - preserveChars));
    return preserved + masked + `@${PLACEHOLDER_DOMAIN}`;
  }
  
  // Extract username and domain parts
  const [, username] = match;
  
  // Mask username part, keep first 3 characters if long enough
  let maskedUsername;
  if (username.length <= 3) {
    // For usernames with 3 or fewer characters, don't mask (too short to safely mask)
    maskedUsername = username;
  } else {
    // For longer usernames, keep first 3 characters
    maskedUsername = username.slice(0, 3) + 
                     generateRandomChars(username.length - 3);
  }
  
  // Use fixed placeholder domain
  return `${maskedUsername}@${PLACEHOLDER_DOMAIN}`;
}

/**
 * Identifies which column maps to which field type
 * @param {string} columnName - The column name to check
 * @returns {string|null} - The field type or null if not found
 */
function getColumnType(columnName) {
  for (const [type, possibleNames] of Object.entries(COLUMNS_TO_MASK)) {
    if (possibleNames.includes(columnName)) {
      return type;
    }
  }
  return null;
}

// Function to mask a value
function maskValue(value) {
    if (!value || typeof value !== 'string') return value;
    
    // For phone numbers and zip codes
    if (/^\d+$/.test(value)) {
        if (value.length <= 3) return value;
        // Keep last 3 digits, mask the beginning
        return generateRandomIntegers(value.length - 3) + value.slice(-3);
    }
    
    // For email addresses
    if (value.includes('@')) {
        return maskEmail(value);
    }
    
    // For addresses and other text
    return maskName(value, true);
}

/**
 * Cleans a string value by removing unwanted characters and normalizing spaces
 * @param {string} value - The value to clean
 * @returns {string} - The cleaned value
 */
function cleanString(value) {
    if (!value) return '';
    
    // Convert to string if not already
    let str = String(value);
    
    // Remove control characters except newlines and tabs
    str = str.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '');
    
    // Normalize whitespace (convert multiple spaces to single space)
    str = str.replace(/\s+/g, ' ');
    
    // Remove leading/trailing whitespace
    str = str.trim();
    
    return str;
}

/**
 * Cleans an email address by removing invalid characters and normalizing format
 * @param {string} email - The email to clean
 * @returns {string} - The cleaned email
 */
function cleanEmail(email) {
    if (!email) return '';
    
    let cleaned = cleanString(email);
    
    // Remove any spaces
    cleaned = cleaned.replace(/\s+/g, '');
    
    // Convert to lowercase
    cleaned = cleaned.toLowerCase();
    
    // Remove any characters that aren't valid in email addresses
    cleaned = cleaned.replace(/[^a-z0-9@._-]/g, '');
    
    return cleaned;
}

/**
 * Cleans a phone number by removing non-numeric characters
 * @param {string} phone - The phone number to clean
 * @returns {string} - The cleaned phone number
 */
function cleanPhone(phone) {
    if (!phone) return '';
    
    // Remove all non-numeric characters
    return String(phone).replace(/\D/g, '');
}

/**
 * Cleans a postal code by removing non-numeric characters
 * @param {string} postalCode - The postal code to clean
 * @returns {string} - The cleaned postal code
 */
function cleanPostalCode(postalCode) {
    if (!postalCode) return '';
    
    // Remove all non-numeric characters
    return String(postalCode).replace(/\D/g, '');
}

/**
 * Cleans a record by applying appropriate cleaning functions to each field
 * @param {Object} record - The record to clean
 * @returns {Object} - The cleaned record
 */
function cleanRecord(record) {
    const cleanedRecord = {};
    
    for (const [key, value] of Object.entries(record)) {
        if (value === null || value === undefined) {
            cleanedRecord[key] = '';
            continue;
        }
        
        // Convert to string and clean
        let cleanedValue = String(value).trim();
        
        // Remove surrounding quotes if present
        cleanedValue = cleanedValue.replace(/^"|"$/g, '');
        
        // Apply specific cleaning based on field type
        if (key.toLowerCase().includes('email')) {
            cleanedRecord[key] = cleanEmail(cleanedValue);
        } else if (key.toLowerCase().includes('phone') || key.toLowerCase().includes('tel')) {
            cleanedRecord[key] = cleanPhone(cleanedValue);
        } else if (key.toLowerCase().includes('postal') || key.toLowerCase().includes('zip')) {
            cleanedRecord[key] = cleanPostalCode(cleanedValue);
        } else {
            cleanedRecord[key] = cleanString(cleanedValue);
        }
    }
    
    return cleanedRecord;
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

/**
 * Main function to mask data in a CSV file
 * @param {string} inputFile - Path to the input CSV file
 * @returns {boolean} - True if masking was successful, false otherwise
 */
async function maskData(inputFile) {
    try {
        console.log(`\nStarting data masking for ${inputFile}...`);
        
        // Read the input file
        let content = await fs.readFile(inputFile, 'utf8');
        
        // Get headers from first line
        const firstLine = content.split('\n')[0];
        const headers = firstLine.split(',').map(h => h.trim().replace(/^"|"$/g, ''));
        
        // Pre-process the content to fix quote issues
        console.log('Pre-processing CSV content...');
        content = preprocessCSVContent(content);
        
        // Parse the CSV with more lenient options
        const results = Papa.parse(content, {
            header: true,
            skipEmptyLines: true,
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
            dynamicTyping: false,
            comments: false,
            delimitersToGuess: [',', '\t', '|', ';', Papa.RECORD_SEP, Papa.UNIT_SEP],
            // Handle quotes more flexibly
            quoteChar: '"',
            escapeChar: '"',
            // Skip empty lines and handle errors
            skipEmptyLines: true,
            error: (error) => {
                console.warn(`Warning: CSV parsing issue at row ${error.row}: ${error.message}`);
            }
        });
        
        if (results.errors.length > 0) {
            console.warn('Warnings during CSV parsing:', results.errors);
            // Continue processing even with warnings
        }
        
        if (!results.data || results.data.length === 0) {
            throw new Error('No data found in the CSV file');
        }

        // Store original columns
        const originalColumns = headers;
        
        console.log('Processing data...');
        
        // Clean and mask sensitive data
        const maskedData = results.data.map((record, index) => {
            try {
                // Create a new record with all original columns and their values
                const maskedRecord = {};
                
                // First, copy all original values
                originalColumns.forEach(column => {
                    // Get the original value, preserving empty strings
                    const originalValue = record[column];
                    maskedRecord[column] = originalValue !== undefined ? originalValue : '';
                });
                
                // Then clean the record
                const cleanedRecord = cleanRecord(maskedRecord);
                
                // Apply masking only to specified columns
                MASKED_COLUMNS.forEach(column => {
                    if (cleanedRecord[column] !== undefined && cleanedRecord[column] !== '') {
                        cleanedRecord[column] = maskValue(cleanedRecord[column]);
                    }
                });

                // Preserve the __tag column if it exists
                if (record.__tag !== undefined) {
                    cleanedRecord.__tag = record.__tag;
                }

                return cleanedRecord;
            } catch (error) {
                console.warn(`Warning: Error processing record at index ${index}: ${error.message}`);
                return record; // Return original record if processing fails
            }
        });
        
        // Create output filename
        const outputFile = inputFile.replace('.csv', '_masked.csv');
        
        // Convert back to CSV using Papa.unparse with explicit configuration
        const csv = Papa.unparse(maskedData, {
            header: true,
            columns: originalColumns,
            quotes: true,
            quoteChar: '"',
            escapeChar: '"',
            delimiter: ',',
            newline: '\n'
        });
        
        // Write the masked data to a new file
        await fs.writeFile(outputFile, csv, 'utf8');
        
        // Verify the output has all columns and values
        const outputContent = await fs.readFile(outputFile, 'utf8');
        const outputResults = Papa.parse(outputContent, { header: true });
        const outputHeaders = outputResults.meta.fields;
        
        // Check for missing columns
        const missingColumns = originalColumns.filter(col => !outputHeaders.includes(col));
        if (missingColumns.length > 0) {
            console.warn('Warning: The following columns are missing from the output:', missingColumns);
        }
        
        console.log(`✓ Successfully masked data and saved to ${outputFile}`);
        return {
            success: true,
            totalRecords: maskedData.length,
            maskedColumns: MASKED_COLUMNS,
            outputFile: outputFile,
            originalColumns: originalColumns,
            outputColumns: outputHeaders,
            missingColumns: missingColumns
        };
    } catch (error) {
        console.error('Error masking data:', error);
        return {
            success: false,
            error: error.message || 'Unknown error occurred during masking'
        };
    }
}

/**
 * Processes all CSV files in a directory
 * @param {string} sourceFolder - Folder containing source CSV files
 * @param {string} targetFolder - Folder to save masked CSV files
 * @param {boolean} preserveNameLength - Whether to preserve the original length of masked values
 * @returns {Object} - Statistics about the batch processing
 */
async function processAllFiles(sourceFolder, targetFolder, preserveNameLength = true) {
  try {
    // Ensure target directory exists
    await fs.mkdir(targetFolder, { recursive: true });
    
    // Read all files from source directory
    const files = await fs.readdir(sourceFolder);
    const csvFiles = files.filter(file => path.extname(file).toLowerCase() === '.csv');
    
    console.log(`Found ${csvFiles.length} CSV files in ${sourceFolder}`);
    
    if (csvFiles.length === 0) {
      console.log('No CSV files to process.');
      return { processedFiles: 0 };
    }
    
    // Process each CSV file
    const results = [];
    for (const file of csvFiles) {
      const inputPath = path.join(sourceFolder, file);
      const outputPath = path.join(targetFolder, file.replace('.csv', '_masked.csv'));
      
      try {
        const result = await maskData(inputPath);
        results.push(result);
        console.log(`✅ Successfully processed ${file}`);
      } catch (error) {
        console.error(`❌ Failed to process ${file}:`, error.message);
      }
    }
    
    return {
      processedFiles: results.length,
      failedFiles: csvFiles.length - results.length,
      fileResults: results
    };
  } catch (error) {
    console.error('Error processing files:', error);
    throw error;
  }
}

/**
 * Main function that handles both single file and batch processing modes
 */
async function main() {
  // Get command line arguments
  const args = process.argv.slice(2);
  
  // Set default option
  const preserveNameLength = true; // Set to false if you want fixed-length masks
  
  // Check for batch mode flag
  const batchModeIndex = args.indexOf('--batch');
  const batchMode = batchModeIndex !== -1;
  
  if (batchMode) {
    // Remove the --batch flag from args
    args.splice(batchModeIndex, 1);
    
    // Get source and target folders for batch processing
    const sourceFolder = args[0] || SOURCE_FOLDER;
    const targetFolder = args[1] || TARGET_FOLDER;
    
    console.log('🚀 Starting batch data masking process...');
    console.log(`Source folder: ${sourceFolder}`);
    console.log(`Target folder: ${targetFolder}`);
    
    try {
      const result = await processAllFiles(sourceFolder, targetFolder, preserveNameLength);
      
      console.log(`\n📊 Masking summary:`);
      console.log(`- Total files processed: ${result.processedFiles}`);
      console.log(`- Failed files: ${result.failedFiles}`);
      
      if (result.fileResults && result.fileResults.length > 0) {
        console.log(`\n📄 File details:`);
        result.fileResults.forEach((fileResult, index) => {
          console.log(`\n  ${index + 1}. ${path.basename(fileResult.inputFile)}:`);
          console.log(`     - Records processed: ${fileResult.totalRecords}`);
          console.log(`     - Columns masked: ${fileResult.maskedColumns.length}`);
        });
      }
      
      console.log('\n✨ Batch masking complete!');
    } catch (error) {
      console.error('Failed to mask data:', error);
      process.exit(1);
    }
  } else {
    // Single file mode
    const inputFile = args[0];
    if (!inputFile) {
      console.error('Error: Please provide an input file path');
      process.exit(1);
    }
    
    console.log('🚀 Starting single file data masking process...');
    console.log(`Input file: ${inputFile}`);
    
    try {
      const result = await maskData(inputFile);
      
      if (!result.success) {
        console.error('Failed to mask data:', result.error);
        process.exit(1);
      }
      
      console.log(`\n📊 Masking summary:`);
      console.log(`- Total records processed: ${result.totalRecords}`);
      console.log(`- Columns masked: ${result.maskedColumns.join(', ')}`);
      console.log(`- Output saved to: ${result.outputFile}`);
      
      console.log('\n✨ Single file masking complete!');
    } catch (error) {
      console.error('Failed to mask data:', error);
      process.exit(1);
    }
  }
}

// Export the maskData function
export { maskData };

// Only run the main function if this file is being run directly
if (import.meta.url === `file://${process.argv[1]}`) {
    main();
}