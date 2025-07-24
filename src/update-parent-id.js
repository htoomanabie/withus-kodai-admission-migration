import { promises as fs } from 'fs';
import Papa from 'papaparse';

async function updateParentIds() {
    console.log('🚀 Starting parent_id update process...\n');
    
    try {
        // Step 1: Build student_id to customer_id mapping from student.csv
        console.log('1. Reading student.csv to build student_id → customer_id mapping...');
        
        const studentContent = await fs.readFile('student.csv', 'utf8');
        const studentData = Papa.parse(studentContent, {
            header: true,
            skipEmptyLines: true,
            dynamicTyping: true
        });
        
        const studentToCustomerMap = new Map();
        studentData.data.forEach(row => {
            if (row.student_id && row.customer_id) {
                studentToCustomerMap.set(String(row.student_id), String(row.customer_id));
            }
        });
        
        console.log(`   ✓ Built ${studentToCustomerMap.size} student → customer mappings`);
        
        // Step 2: Build customer_id to parent_id mapping from customer.csv
        console.log('\n2. Reading customer.csv to build customer_id → parent_id mapping...');
        
        const customerContent = await fs.readFile('customer.csv', 'utf8');
        const customerData = Papa.parse(customerContent, {
            header: true,
            skipEmptyLines: true,
            dynamicTyping: true
        });
        
        const customerToParentMap = new Map();
        customerData.data.forEach(row => {
            if (row.customer_id && row.parent_id) {
                customerToParentMap.set(String(row.customer_id), String(row.parent_id));
            }
        });
        
        console.log(`   ✓ Built ${customerToParentMap.size} customer → parent mappings`);
        
        // Step 3: Read the processed_inquiry_data.csv file
        console.log('\n3. Reading processed_inquiry_data.csv...');
        
        const processedContent = await fs.readFile('processed_inquiry_data.csv', 'utf8');
        const processedData = Papa.parse(processedContent, {
            header: true,
            skipEmptyLines: true,
            transformHeader: function(header) {
                // Keep headers as-is (don't lowercase)
                return header.trim();
            }
        });
        
        console.log(`   ✓ Read ${processedData.data.length} records from processed_inquiry_data.csv`);
        
        // Step 4: Update records with parent_id
        console.log('\n4. Updating records with parent_id...');
        
        let updatedCount = 0;
        let totalStudentIds = 0;
        
        processedData.data.forEach(row => {
            // Get student_id from the "Student__r:Contact:MANAERP__External_User_Id__c" column
            const studentId = row['Student__r:Contact:MANAERP__External_User_Id__c'];
            
            if (studentId && studentId.trim() !== '') {
                totalStudentIds++;
                const cleanStudentId = String(studentId).trim();
                
                // Look up customer_id using student_id
                if (studentToCustomerMap.has(cleanStudentId)) {
                    const customerId = studentToCustomerMap.get(cleanStudentId);
                    
                    // Look up parent_id using customer_id
                    if (customerToParentMap.has(customerId)) {
                        const parentId = customerToParentMap.get(customerId);
                        
                        // Update the parent_id column
                        row['Parent__r:Contact:MANAERP__External_User_Id__c'] = parentId;
                        updatedCount++;
                    }
                }
            }
        });
        
        console.log(`   ✓ Updated ${updatedCount} records with parent_id (out of ${totalStudentIds} records with student_id)`);
        
        // Step 5: Write the updated data back to the CSV file
        console.log('\n5. Writing updated data back to processed_inquiry_data.csv...');
        
        const csvOutput = Papa.unparse(processedData.data, {
            header: true,
            quotes: true
        });
        
        await fs.writeFile('processed_inquiry_data.csv', csvOutput, 'utf8');
        
        console.log('   ✓ Successfully updated processed_inquiry_data.csv');
        
        // Step 6: Summary
        console.log('\n📊 Update Summary:');
        console.log('================');
        console.log(`Total records in file: ${processedData.data.length}`);
        console.log(`Records with student_id: ${totalStudentIds}`);
        console.log(`Records updated with parent_id: ${updatedCount}`);
        console.log(`Update success rate: ${totalStudentIds > 0 ? ((updatedCount / totalStudentIds) * 100).toFixed(1) : 0}%`);
        
        console.log('\n✨ Parent ID update process completed successfully!');
        
    } catch (error) {
        console.error('\n❌ Error updating parent IDs:', error);
        throw error;
    }
}

// Run the update process
updateParentIds().catch(error => {
    console.error('Script failed:', error);
    process.exit(1);
});

