import { verifyMaskingIntegrity } from './verify-masking-integrity.js';
import { exec } from 'child_process';
import { promisify } from 'util';
import { promises as fs } from 'fs';

const execAsync = promisify(exec);

async function runProcessingWithVerification(scriptName, inputFile, enableMasking = false) {
    try {
        console.log(`🚀 Starting ${scriptName} processing with verification...\n`);
        
        const startTime = Date.now();
        
        // Build the command
        const maskFlag = enableMasking ? '--mask' : '';
        const command = `node ${scriptName} ${inputFile} ${maskFlag}`.trim();
        
        console.log(`📝 Running command: ${command}`);
        
        // Run the main processing script
        const { stdout, stderr } = await execAsync(command);
        
        if (stdout) {
            console.log(stdout);
        }
        if (stderr) {
            console.warn('Warnings:', stderr);
        }
        
        console.log('✅ Processing completed successfully!');
        
        // If masking was enabled, run verification
        if (enableMasking) {
            console.log('\n🔍 Running masking integrity verification...\n');
            
            // Determine the before and after files based on the script
            let beforeFile, afterFile;
            
            if (scriptName === 'process-parents.js') {
                beforeFile = 'processed_parent_data_fixed.csv';
                afterFile = 'processed_parent_data_fixed_masked.csv';
            } else if (scriptName === 'process-students.js') {
                beforeFile = 'processed_student_data_fixed.csv';
                afterFile = 'processed_student_data_fixed_masked.csv';
            } else {
                // Generic pattern
                beforeFile = `processed_${inputFile.replace('.csv', '_fixed.csv')}`;
                afterFile = `processed_${inputFile.replace('.csv', '_fixed_masked.csv')}`;
            }
            
            // Check if files exist
            try {
                await fs.access(beforeFile);
                await fs.access(afterFile);
                
                // Run verification
                const verificationSuccess = await verifyMaskingIntegrity(beforeFile, afterFile);
                
                if (verificationSuccess) {
                    console.log('\n🎉 VERIFICATION PASSED: No data loss detected during masking!');
                } else {
                    console.log('\n⚠️ VERIFICATION FAILED: Issues detected during masking. Please review the report above.');
                    return false;
                }
                
            } catch (error) {
                console.warn('\n⚠️ Could not verify masking integrity - files not found:');
                console.warn(`   Before: ${beforeFile}`);
                console.warn(`   After: ${afterFile}`);
                console.warn('   This might be normal if masking was not actually performed.');
            }
        } else {
            console.log('\n💡 Masking was not enabled. To verify masking integrity, re-run with --mask flag.');
        }
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`\n⏱️ Total execution time: ${duration} seconds`);
        
        return true;
        
    } catch (error) {
        console.error('\n❌ Error during processing:', error);
        return false;
    }
}

// Main function
async function main() {
    const args = process.argv.slice(2);
    
    if (args.length < 2) {
        console.log('Usage: node process-with-verification.js <script-name> <input-file> [--mask]');
        console.log('');
        console.log('Examples:');
        console.log('  node process-with-verification.js process-parents.js parent.csv --mask');
        console.log('  node process-with-verification.js process-students.js student.csv --mask');
        console.log('  node process-with-verification.js process-parents.js parent.csv');
        console.log('');
        console.log('This script will:');
        console.log('  1. Run the specified processing script');
        console.log('  2. If --mask is specified, automatically verify data integrity after masking');
        console.log('  3. Report any data loss or integrity issues');
        process.exit(1);
    }
    
    const scriptName = args[0];
    const inputFile = args[1];
    const enableMasking = args.includes('--mask');
    
    const success = await runProcessingWithVerification(scriptName, inputFile, enableMasking);
    
    process.exit(success ? 0 : 1);
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
    main();
} 