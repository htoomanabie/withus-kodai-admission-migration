import { promises as fs } from 'fs';
import Papa from 'papaparse';
import path from 'path';

// Configuration
const DEFAULT_INPUT_FILE = 'combined_student_data_direct_merge_fixed_masked.csv';
const DEFAULT_OUTPUT_PREFIX = (inputFile = DEFAULT_INPUT_FILE) => {
  const baseName = path.basename(inputFile, path.extname(inputFile));
  return `${baseName}_${new Date().toLocaleString('en-US', { 
    year: 'numeric', 
    month: '2-digit', 
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  }).replace(/[/:]/g, '-').replace(/,/g, '')}`;
};
const DEFAULT_ROWS_PER_FILE = 50000;

/**
 * Splits a large CSV file into multiple smaller files
 * @param {string} inputFilePath - Path to the input CSV file
 * @param {string} outputPrefix - Prefix for output files
 * @param {number} rowsPerFile - Number of rows per output file
 * @param {string} outputDir - Directory to store the output files
 * @returns {Promise<Object>} - Statistics about the splitting operation
 */
async function splitCsvFile(inputFilePath, outputPrefix, rowsPerFile, outputDir = '') {
  try {
    console.log(`Reading CSV file: ${inputFilePath}`);
    
    // Check if input file exists
    try {
      await fs.access(inputFilePath);
    } catch (error) {
      throw new Error(`Input file not found: ${inputFilePath}`);
    }
    
    // Create output directory if it doesn't exist and is specified
    if (outputDir) {
      try {
        await fs.mkdir(outputDir, { recursive: true });
        console.log(`Created output directory: ${outputDir}`);
      } catch (error) {
        console.warn(`Warning: Could not create output directory: ${error.message}`);
      }
    }
    
    // Read the input file
    const csvData = await fs.readFile(inputFilePath, 'utf8');
    console.log(`File read successfully. Parsing CSV...`);
    
    // Parse CSV data
    const results = Papa.parse(csvData, {
      header: true,
      skipEmptyLines: true
    });
    
    if (results.errors.length > 0) {
      console.warn('Warning: Some parsing errors occurred:', results.errors);
    }
    
    const totalRows = results.data.length;
    const headers = results.meta.fields;
    
    console.log(`CSV parsed successfully.`);
    console.log(`Total rows: ${totalRows}`);
    console.log(`Number of columns: ${headers.length}`);
    
    // Calculate number of files needed
    const numFiles = Math.ceil(totalRows / rowsPerFile);
    console.log(`Will create ${numFiles} output files with up to ${rowsPerFile} rows each.`);
    
    // Create output files
    const fileStats = [];
    
    for (let fileIndex = 0; fileIndex < numFiles; fileIndex++) {
      const startRow = fileIndex * rowsPerFile;
      const endRow = Math.min((fileIndex + 1) * rowsPerFile, totalRows);
      const rowsInFile = endRow - startRow;
      
      // Get data slice for this file
      const dataSlice = results.data.slice(startRow, endRow);
      
      // Format file number with leading zeros
      const fileNumber = String(fileIndex + 1).padStart(numFiles.toString().length, '0');
      const outputFileName = `${outputPrefix}_${fileNumber}.csv`;
      const outputPath = outputDir ? path.join(outputDir, outputFileName) : outputFileName;
      
      // Convert to CSV including headers
      const csv = Papa.unparse({
        fields: headers,
        data: dataSlice
      });
      
      // Write to file
      await fs.writeFile(outputPath, csv, 'utf8');
      
      console.log(`Created file ${outputPath} with ${rowsInFile} rows (${startRow + 1}-${endRow} of ${totalRows})`);
      
      fileStats.push({
        fileName: outputPath,
        rowCount: rowsInFile,
        startRow: startRow + 1,
        endRow
      });
    }
    
    console.log(`\nSplitting complete!`);
    console.log(`Created ${numFiles} files from ${totalRows} total rows.`);
    
    return {
      totalRows,
      numFiles,
      fileStats
    };
  } catch (error) {
    console.error(`Error splitting CSV file: ${error.message}`);
    console.error(error.stack);
    throw error;
  }
}

/**
 * Parse command line arguments
 * @returns {Object} - Parsed arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  let inputFile = DEFAULT_INPUT_FILE;
  let rowsPerFile = DEFAULT_ROWS_PER_FILE;
  let outputDir = '';
  
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    
    if (arg === '--rows' && i + 1 < args.length) {
      rowsPerFile = parseInt(args[++i], 10);
      if (isNaN(rowsPerFile) || rowsPerFile <= 0) {
        console.error('Invalid rows per file. Using default:', DEFAULT_ROWS_PER_FILE);
        rowsPerFile = DEFAULT_ROWS_PER_FILE;
      }
    } else if (arg === '--output-dir' && i + 1 < args.length) {
      outputDir = args[++i];
    } else if (i === 0 && !arg.startsWith('--')) {
      // First non-flag argument is input file
      inputFile = arg;
    }
  }
  
  // Set outputPrefix based on the input file name
  const outputPrefix = DEFAULT_OUTPUT_PREFIX(inputFile);
  
  return {
    inputFile,
    outputPrefix,
    rowsPerFile,
    outputDir
  };
}

/**
 * Main function
 */
async function main() {
  console.log('🔪 CSV File Splitter');
  console.log('------------------');
  
  try {
    const { inputFile, outputPrefix, rowsPerFile, outputDir } = parseArgs();
    
    console.log(`Input file: ${inputFile}`);
    console.log(`Output prefix: ${outputPrefix}`);
    console.log(`Rows per file: ${rowsPerFile}`);
    console.log(`Output directory: ${outputDir || '(current directory)'}`);
    console.log(`------------------\n`);
    
    const startTime = Date.now();
    const result = await splitCsvFile(inputFile, outputPrefix, rowsPerFile, outputDir);
    const endTime = Date.now();
    
    console.log(`\nOperation completed in ${((endTime - startTime) / 1000).toFixed(2)} seconds.`);
    
    // Write summary file if there are multiple output files
    if (result.numFiles > 1) {
      const summaryFile = outputDir ? path.join(outputDir, 'split_summary.json') : 'split_summary.json';
      await fs.writeFile(summaryFile, JSON.stringify({
        originalFile: inputFile,
        totalRows: result.totalRows,
        numFiles: result.numFiles,
        rowsPerFile,
        files: result.fileStats,
        timestamp: new Date().toISOString()
      }, null, 2), 'utf8');
      
      console.log(`Summary written to ${summaryFile}`);
    }
    
    process.exit(0);
  } catch (error) {
    console.error('\nError:', error.message);
    process.exit(1);
  }
}

// Execute the script
main();