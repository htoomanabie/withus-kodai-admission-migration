import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Configuration constant for phase check
const PHASE_CHECK = 'phase11';

function fixNextLine(inputFile = 'combined_student_data_direct_merge.csv', outputFile = null) {
    if (!outputFile) {
        outputFile = inputFile.replace('.csv', '_fixed.csv');
    }

    try {
        // Read the file with UTF-8 encoding
        const content = fs.readFileSync(inputFile, 'utf8');
        const lines = content.split('\n');
        
        const processedLines = [lines[0]]; // Preserve header
        
        let i = 1;
        while (i < lines.length) {
            let currentLine = lines[i].trim();
            
            // Keep concatenating lines until we find a line ending with the configured phase
            while (i + 1 < lines.length && !(
                currentLine.endsWith(PHASE_CHECK) ||
                currentLine.endsWith(`,${PHASE_CHECK}`) ||
                currentLine.endsWith(`"${PHASE_CHECK}"`) ||
                currentLine.endsWith(`,"${PHASE_CHECK}"`)
            )) {
                i++;
                currentLine += ' ' + lines[i].trim();
            }
            
            processedLines.push(currentLine);
            i++;
        }
        
        // Write the processed lines to the output file
        fs.writeFileSync(outputFile, processedLines.join('\n'), 'utf8');
        
        console.log(`Processed ${lines.length - 1} input lines (excluding header)`);
        console.log(`Generated ${processedLines.length - 1} output lines (excluding header)`);
        console.log(`Output saved to ${outputFile}`);
        return outputFile;
    } catch (error) {
        console.error(`Error processing file: ${error.message}`);
        console.log('Trying with error handling...');
        return concatPhase1LinesWithErrorHandling(inputFile, outputFile);
    }
}

function concatPhase1LinesWithErrorHandling(inputFile = 'combined_student_data_direct_merge.csv', outputFile = null) {
    if (!outputFile) {
        outputFile = inputFile.replace('.csv', '_fixed.csv');
    }

    try {
        // Read the file with UTF-8 encoding
        const content = fs.readFileSync(inputFile, 'utf8');
        const lines = content.split('\n');
        
        const processedLines = [lines[0]]; // Preserve header
        let currentLine = '';
        
        for (let i = 1; i < lines.length; i++) {
            const line = lines[i].trim();
            
            if (line.includes(PHASE_CHECK)) {
                if (currentLine) {
                    processedLines.push(currentLine);
                    currentLine = '';
                }
                processedLines.push(line);
            } else {
                currentLine += (currentLine ? ' ' : '') + line;
            }
        }
        
        if (currentLine) {
            processedLines.push(currentLine);
        }
        
        // Write the processed lines to the output file
        fs.writeFileSync(outputFile, processedLines.join('\n'), 'utf8');
        
        console.log(`Processed ${lines.length - 1} input lines (excluding header)`);
        console.log(`Generated ${processedLines.length - 1} output lines (excluding header)`);
        console.log(`Output saved to ${outputFile}`);
        return outputFile;
    } catch (error) {
        console.error(`Error in error handling mode: ${error.message}`);
        return null;
    }
}

// If script is run directly
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

if (process.argv[1] === fileURLToPath(import.meta.url)) {
    const inputFile = process.argv[2] || 'combined_student_data_direct_merge.csv';
    const outputFile = process.argv[3] || null;
    
    const success = fixNextLine(inputFile, outputFile);
    
    if (success) {
        console.log('File processing completed successfully.');
    } else {
        console.log('File processing failed.');
        process.exit(1);
    }
}

export {
    fixNextLine,
    concatPhase1LinesWithErrorHandling
}; 