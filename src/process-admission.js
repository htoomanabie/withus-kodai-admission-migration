import { promises as fs } from 'fs';
import Papa from 'papaparse';
import _ from 'lodash';

const CHUNK_SIZE = 5000; // Smaller chunk size to reduce memory pressure

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

// Define the mapping for operate_type_id
const OPERATE_TYPE_MAPPING = {
    '39': '専攻科',
    '40': '専門カレッジ',
    '41': '選科',
    '42': '高認',
    '43': '高認',
    '44': '高認',
    '38': '本科',
    '48': '中等部',
    '47': 'フリースクール',
    '46': '聴講生',
    '51': 'オンラインカレッジ',
    '53': 'BASE'
};

// Define the mapping for webentry_payment
const PAYMENT_MAPPING = {
    '1': '銀行振込',
    '2': 'クレジット決済'
};

// Define the mapping for webentry_other
const OTHER_MAPPING = {
    '0': '専願',
    '1': '併願'
};

// Define the mapping for webentry_payment_result
const PAYMENT_RESULT_MAPPING = {
    '1': '決済OK',
    '0': '決済NG'
};

// Define the mapping for webentry_application
const APPLICATION_MAPPING = {
    '1': '新卒',
    '2': '転入/編入',
    '3': '高認取得通信コースご希望の方',
    '4': 'その他'
};

// Define the mapping for webentry_style
const STYLE_MAPPING = {
    '1': 'キャンパス通学',
    '2': 'MHS'
};

// Define the mapping for webentry_gender
const GENDER_MAPPING = {
    '1': '男',
    '2': '女'
};

// Define the mapping for webentry_g_relation
const RELATION_MAPPING = {
    '1': '本人',
    '2': '父',
    '3': '母',
    '4': 'その他',
    '6': 'その他',
    '7': 'その他',
    '8': 'その他',
    '9': 'その他',
    '11': 'その他'
};

// Define the mapping for webentry_ed_jhs_status
const JHS_STATUS_MAPPING = {
    '0': '卒業見込み',
    '1': '卒業'
};

// Define the mapping for webentry_ed_hs1_curriculum
const HS_CURRICULUM_MAPPING = {
    '1': '全日制課程',
    '2': '定時制課程',
    '3': '通信制課程'
};

// Define the mapping for webentry_ed_hs1_status
const HS_STATUS_MAPPING = {
    '1': '在学中',
    '2': '休学中',
    '3': '退学'
};

// Define the mapping for webentry_free
const FREE_MAPPING = {
    '0': 'FALSE',
    '1': 'TRUE'
};

// Define the mapping for webentry_status
const STATUS_MAPPING = {
    '0': '出願登録前',
    '1': '出願登録中'
};

// Define the required columns for our output
const REQUIRED_COLUMNS = [
    'web_entry_id',
    'student_id',
    'parent_id',
    'webentry_free',
    'webentry_photo',
    'webentry_other',
    'webentry_hope',
    'webentry_price',
    'webentry_payment',
    'webentry_payment_date',
    'webentry_payment_result',
    'webentry_status',
    'webentry_email',
    'student_pc_address',
    'webentry_reason',
    'operate_type_id',
    'date',
    'receipt_branch_id',
    'webentry_application',
    'webentry_style',
    'webentry_school',
    'webentry_introduction',
    'webentry_firstname',
    'webentry_lastname',
    'webentry_firstkana',
    'webentry_lastkana',
    'webentry_birthday',
    'webentry_gender',
    'webentry_tel1',
    'webentry_tel2',
    'webentry_postal_code',
    'webentry_pref',
    'webentry_address1',
    'webentry_address2',
    'webentry_address3',
    'webentry_g_relation',
    'webentry_g_lastname',
    'webentry_g_firstname',
    'webentry_g_lastkana',
    'webentry_g_firstkana',
    'webentry_g_gender',
    'webentry_g_tel1',
    'webentry_g_tel2',
    'webentry_g_postal_code',
    'webentry_g_pref',
    'webentry_g_address1',
    'webentry_g_address2',
    'webentry_g_address3',
    'webentry_g_email',
    'webentry_ed_jhs_pref',
    'webentry_ed_jhs_city',
    'webentry_ed_jhs_code',
    'webentry_ed_jhs_name',
    'webentry_ed_jhs_name_manual',
    'webentry_ed_jhs_to',
    'webentry_ed_jhs_status',
    'webentry_ed_hs1_pref',
    'webentry_ed_hs1_city',
    'webentry_ed_hs1_code',
    'webentry_ed_hs1_name',
    'webentry_ed_hs1_name_manual',
    'webentry_ed_hs1_curriculum',
    'webentry_ed_hs1_dept',
    'webentry_ed_hs1_from',
    'webentry_ed_hs1_to',
    'webentry_ed_hs1_status',
    'webentry_ed_hs1_absence',
    'webentry_other_hs1_pref',
    'webentry_other_hs1_city',
    'webentry_other_hs1_code',
    'webentry_other_hs1_name',
    'webentry_other_hs1_name_manual',
    'webentry_other_hs1_date',
    'remarks',
    'customer_id',
    'admission_judgement_status'
];
const COLUMN_HEADER_MAPPING = {
    'web_entry_id': 'Admission_External_Id__c',
    'student_id': 'Student__r:Contact:MANAERP__External_User_Id__c',
    'parent_id': 'Parent__r:Contact:MANAERP__External_User_Id__c',
    'webentry_photo': 'Photo__c',
    'webentry_other': 'Applicant_Category__c',
    'webentry_hope': 'What_You_Want_To_Do_In_High_School__c',
    'webentry_price': 'Examination_Fee__c',
    'webentry_payment': 'Settlement_Method__c',
    'webentry_payment_date': 'Settlement_Date__c',
    'webentry_payment_result': 'Settlement_Result__c',
    'webentry_status': 'Status__c',
    'webentry_email': 'Web_App_Log_In_Id__c',
    'webentry_reason': 'Reason_For_Applying_For_Admission__c',
    'operate_type_id': 'Student_Category__c',
    'date': 'Submit_Date__c',
    'receipt_branch_id': '_PICdepartment',
    'webentry_application': 'Preferred_Application_Category__c',
    'webentry_style': 'Preferred_Study_Method__c',
    'webentry_school': 'Preferred_Location__r:Account:MANAERP__School_Code__c',
    'webentry_introduction': 'Utilization_Of_Referral_System__c',
    'webentry_firstname': 'Student_First_Name__c',
    'webentry_lastname': 'Student_Last_Name__c',
    'webentry_firstkana': 'Student_First_Name_Phonetic__c',
    'webentry_lastkana': 'Student_Last_Name_Phonetic__c',
    'webentry_birthday': 'Student_Date_Of_Birth__c',
    'webentry_gender': 'Student_Gender__c',
    'student_pc_address': 'Student_PC_Address__c',
    'webentry_tel1': 'Student_Phone_Number__c',
    'webentry_tel2': 'Student_Cell_Phone_Number__c',
    'webentry_postal_code': 'Student_Address_Zip__c',
    'webentry_pref': 'Student_Address_State__c',
    'webentry_address1': 'Student_Address_City__c',
    'webentry_address2': 'Student_Address_Street__c',
    'webentry_address3': 'Student_Address_Building__c',
    'webentry_g_relation': 'Parent_Relationship__c',
    'webentry_g_lastname': 'Parent_Family_Name__c',
    'webentry_g_firstname': 'Parent_First_Name__c',
    'webentry_g_lastkana': 'Parent_Family_Name_Katakana__c',
    'webentry_g_firstkana': 'Parent_First_Name_Katakana__c',
    'webentry_g_gender': 'Parent_Gender__c',
    'webentry_g_tel1': 'Parent_Phone_Number__c',
    'webentry_g_tel2': 'Parent_Cell_Phone_Number__c',
    'webentry_g_postal_code': 'Parent_Address_Zip__c',
    'webentry_g_pref': 'Parent_Address_State__c',
    'webentry_g_address1': 'Parent_Address_City__c',
    'webentry_g_address2': 'Parent_Address_Street__c',
    'webentry_g_address3': 'Parent_Address_Building_Name_Room_Num__c',
    'webentry_g_email': 'Parent_Email__c',
    'webentry_ed_jhs_pref': 'Middle_School_Prefecture__c',
    'webentry_ed_jhs_city': 'Middle_School_City__c',
    'webentry_ed_jhs_code': 'Middle_School_Code__c',
    'webentry_ed_jhs_name': 'Middle_School_School_Name__c',
    'webentry_ed_jhs_name_manual': 'Middle_School_School_Name_Manual__c',
    'webentry_ed_jhs_to': 'Middle_School_Graduation_Date__c',
    'webentry_ed_jhs_status': 'Middle_School_Graduation_Status__c',
    'webentry_ed_hs1_pref': 'High_School_1_Prefecture__c',
    'webentry_ed_hs1_city': 'High_School_1_City__c',
    'webentry_ed_hs1_code': 'High_School_1_Code__c',
    'webentry_ed_hs1_name': 'High_School_1_Name__c',
    'webentry_ed_hs1_name_manual': 'High_School_1_Name_Manual__c',
    'webentry_ed_hs1_curriculum': 'High_School_1_Curriculum__c',
    'webentry_ed_hs1_dept': 'High_School_1_Course_Of_Study__c',
    'webentry_ed_hs1_from': 'High_School_1_Enrollment_Start_Date__c',
    'webentry_ed_hs1_to': 'High_School_1_Enrollment_End_Date__c',
    'webentry_ed_hs1_status': 'High_School_1_Enrollment_Status__c',
    'webentry_ed_hs1_absence': 'High_School_1_Period_Of_Absence__c',
    'webentry_other_hs1_pref': 'Other_School_1_Prefecture__c',
    'webentry_other_hs1_city': 'Other_School_1_City__c',
    'webentry_other_hs1_code': 'Other_School_1_Code__c',
    'webentry_other_hs1_name': 'Other_School_1_Name__c',
    'webentry_other_hs1_name_manual': 'Other_School_1_Name_Manual__c',
    'webentry_other_hs1_date': 'Other_School_1_Date_Of_Pass_Announcement__c',
    'webentry_free': 'Exemption_From_Examination_Fee__c',
    'remarks': 'Remarks__c',
    'admission_judgement_status': 'Judgement_Status__c'
};

// const COLUMN_HEADER_MAPPING = {
//     'web_entry_id': 'Admission External Id',
//     'student_id': 'Student',
//     'parent_id': 'Parent',
//     'webentry_photo': 'Photo',
//     'webentry_other': 'Applicant Category',
//     'webentry_hope': 'What you want to do in high school',
//     'webentry_price': 'Examination fee',
//     'webentry_payment': 'Settlement Method',
//     'webentry_payment_date': 'Settlement Date',
//     'webentry_payment_result': 'Settlement Result',
//     'webentry_status': 'Status',
//     'webentry_email': 'Web App Log in Id',
//     'webentry_reason': 'Reason for applying for admission',
//     'operate_type_id': 'Student Category',
//     'date': 'Submit Date',
//     'receipt_branch_id': 'PIC department',
//     'webentry_application': 'Preferred Application Category',
//     'webentry_style': 'Preferred Study Method',
//     'webentry_school': 'Preferred Location',
//     'webentry_introduction': 'Utilization of referral system',
//     'webentry_firstname': 'Student First Name',
//     'webentry_lastname': 'Student Last Name',
//     'webentry_firstkana': 'Student First Name (Phonetic)',
//     'webentry_lastkana': 'Student Last Name (Phonetic)',
//     'webentry_birthday': 'Student Date of birth',
//     'webentry_gender': 'Student Gender',
//     'student_pc_address': 'Student PC address',
//     'webentry_tel1': 'Student Phone Number',
//     'webentry_tel2': 'Student Cell phone number',
//     'webentry_postal_code': 'Student Address (zip/postal code)',
//     'webentry_pref': 'Student Address (State/Province)',
//     'webentry_address1': 'Student Address (City)',
//     'webentry_address2': 'Student Address Street',
//     'webentry_address3': 'Student Address (Building/Apartment)',
//     'webentry_g_relation': 'Parent Relationship',
//     'webentry_g_lastname': 'Parent Family Name',
//     'webentry_g_firstname': 'Parent First Name',
//     'webentry_g_lastkana': 'Parent Family Name(katakana)',
//     'webentry_g_firstkana': 'Parent First Name(katakana)',
//     'webentry_g_gender': 'Parent gender',
//     'webentry_g_tel1': 'Parent Phone Number',
//     'webentry_g_tel2': 'Parent cell phone number',
//     'webentry_g_postal_code': 'Parent Address (Zip)',
//     'webentry_g_pref': 'Parent Address (State/Province)',
//     'webentry_g_address1': 'Parent Address (City)',
//     'webentry_g_address2': 'Parent Address (Street)',
//     'webentry_g_address3': 'Parent Address (Building Name/Room Num)',
//     'webentry_g_email': 'Parent Email',
//     'webentry_ed_jhs_pref': 'Middle School Prefecture',
//     'webentry_ed_jhs_city': 'Middle School City',
//     'webentry_ed_jhs_code': 'Middle School Code',
//     'webentry_ed_jhs_name': 'Middle School School Name',
//     'webentry_ed_jhs_name_manual': 'Middle School School Name (Manual)',
//     'webentry_ed_jhs_to': 'Middle School Graduation Date',
//     'webentry_ed_jhs_status': 'Middle School Graduation Status',
//     'webentry_ed_hs1_pref': 'High school 1 Prefecture',
//     'webentry_ed_hs1_city': 'High School 1 City',
//     'webentry_ed_hs1_code': 'High School 1 Code',
//     'webentry_ed_hs1_name': 'High School 1 Name',
//     'webentry_ed_hs1_name_manual': 'High School School Name (Manual)',
//     'webentry_ed_hs1_curriculum': 'High School 1 Curriculum',
//     'webentry_ed_hs1_dept': 'High School 1 Course of Study',
//     'webentry_ed_hs1_from': 'High School 1 Enrollment Start Date',
//     'webentry_ed_hs1_to': 'High School 1 Enrollment End Date',
//     'webentry_ed_hs1_status': 'High School 1 Enrollment Status',
//     'webentry_ed_hs1_absence': 'High School 1 Period Of Absence',
//     'webentry_other_hs1_pref': 'Other school 1 Prefecture',
//     'webentry_other_hs1_city': 'Other school 1 City',
//     'webentry_other_hs1_code': 'Other school 1 Code',
//     'webentry_other_hs1_name': 'Other School 1 Name',
//     'webentry_other_hs1_name_manual': 'Other School 1 Name (Manual)',
//     'webentry_other_hs1_date': 'Other school 1 Date of Pass Announcement',
//     'webentry_free': 'Exemption from examination fee',
//     'remarks': 'Remarks',
//     'admission_judgement_status': 'Admission Judgement Status'
// };

// Function to format date to YYYY-MM-DD
function formatDateToYYYYMMDD(value) {
    // Handle empty values
    if (!value && value !== 0) {
        return '';
    }
    
    // Force conversion to string and remove any whitespace
    let strValue = String(value).replace(/\s/g, '');
    
    // If it's already in YYYY-MM-DD format, return as is
    if (/^\d{4}-\d{2}-\d{2}$/.test(strValue)) {
        return strValue;
    }
    
    // If it's in YYYY/MM/DD format, convert to YYYY-MM-DD
    if (/^\d{4}\/\d{2}\/\d{2}$/.test(strValue)) {
        return strValue.replace(/\//g, '-');
    }
    
    // If it's in YYYYMMDD format
    if (/^\d{8}$/.test(strValue)) {
        const year = strValue.slice(0, 4);
        const month = strValue.slice(4, 6);
        const day = strValue.slice(6, 8);
        return `${year}-${month}-${day}`;
    }
    
    // If it's in YYYYMM format
    if (/^\d{6}$/.test(strValue)) {
        const year = strValue.slice(0, 4);
        const month = strValue.slice(4, 6);
        return `${year}-${month}-01`;
    }
    
    // If it's a number less than 6 digits, pad with zeros
    if (/^\d{1,5}$/.test(strValue)) {
        strValue = strValue.padStart(6, '0');
        const year = strValue.slice(0, 4);
        const month = strValue.slice(4, 6);
        return `${year}-${month}-01`;
    }
    
    // Return empty string if conversion failed
    return '';
}

// Transform prefecture values
function transformPrefecture(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return PREFECTURE_MAPPING[strValue] || value;
}

// Transform operate_type_id values
function transformOperateType(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return OPERATE_TYPE_MAPPING[strValue] || value;
}

// Transform webentry_payment values
function transformPayment(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return PAYMENT_MAPPING[strValue] || value;
}

// Transform webentry_other values
function transformOther(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return OTHER_MAPPING[strValue] || value;
}

// Transform webentry_payment_result values
function transformPaymentResult(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return PAYMENT_RESULT_MAPPING[strValue] || value;
}

// Transform webentry_application values
function transformApplication(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return APPLICATION_MAPPING[strValue] || value;
}

// Transform webentry_style values
function transformStyle(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return STYLE_MAPPING[strValue] || value;
}

// Transform webentry_gender values
function transformGender(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return GENDER_MAPPING[strValue] || value;
}

// Transform webentry_g_relation values
function transformRelation(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return RELATION_MAPPING[strValue] || value;
}

// Transform webentry_ed_jhs_status values
function transformJhsStatus(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return JHS_STATUS_MAPPING[strValue] || value;
}

// Transform webentry_ed_hs1_curriculum values
function transformHsCurriculum(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return HS_CURRICULUM_MAPPING[strValue] || value;
}

// Transform webentry_ed_hs1_status values
function transformHsStatus(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return HS_STATUS_MAPPING[strValue] || value;
}

// Transform webentry_free values
function transformFree(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return FREE_MAPPING[strValue] || value;
}

// Transform webentry_status values
function transformStatus(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return STATUS_MAPPING[strValue] || value;
}

// Function to remove line breaks from a string
function removeLineBreaks(value) {
    if (typeof value === 'string') {
        return value.replace(/[\r\n]+/g, ' ');
    }
    return value;
}

// Function to clean text by handling quoted content with newlines and normalizing whitespace
function cleanText(value) {
    if (typeof value === 'string') {
        // Handle the pattern "[^"]*\n[^"]*" (text with newlines inside quotes)
        // This replaces newlines that occur within quoted text
        value = value.replace(/("[^"]*)\n([^"]*")/g, '$1 $2');
        
        // Replace any remaining line breaks with spaces
        value = value.replace(/[\r\n]+/g, ' ');
        
        // Replace multiple spaces with a single space
        value = value.replace(/\s+/g, ' ');
        
        // Trim leading and trailing whitespace
        value = value.trim();
        
        return value;
    }
    return value;
}

// Function to remove hyphens from a string
function removeHyphens(value) {
    if (typeof value === 'string') {
        return value.replace(/-/g, '');
    }
    return value;
}

// Function to find the latest operate_type_id for a student
// In case there are multiple records for the same student_id
function findLatestOperateTypeId(records, studentId) {
    if (!records || records.length === 0) return null;
    
    // Filter records for this student_id
    const studentRecords = records.filter(record => 
        record.student_id && String(record.student_id) === String(studentId));
    
    if (studentRecords.length === 0) return null;
    
    // Sort by date if available (assuming newer records come later)
    if (studentRecords[0].date) {
        studentRecords.sort((a, b) => {
            // Try to parse dates, defaulting to string comparison if parse fails
            const dateA = a.date ? new Date(a.date) : null;
            const dateB = b.date ? new Date(b.date) : null;
            
            if (dateA && dateB && !isNaN(dateA) && !isNaN(dateB)) {
                return dateB - dateA; // Descending order (newest first)
            }
            
            // Fallback to string comparison
            return String(b.date || '').localeCompare(String(a.date || ''));
        });
    }
    
    // Return operate_type_id from the first (newest) record
    return studentRecords[0].operate_type_id;
}

function processRecord(record) {
    const processedRecord = {};
    
    // Process each required column
    REQUIRED_COLUMNS.forEach(column => {
        let value = record[column] !== undefined ? record[column] : '';
        
        // Apply transformations
        if (column === 'webentry_pref' || column === 'webentry_g_pref' || 
            column === 'webentry_ed_jhs_pref' || column === 'webentry_ed_hs1_pref' || 
            column === 'webentry_other_hs1_pref') {
            value = transformPrefecture(value);
        }
        // Format all date fields to YYYY-MM-DD
        else if (column === 'webentry_payment_date' || 
                 column === 'date' || 
                 column === 'webentry_birthday' ||
                 column === 'webentry_ed_jhs_to' ||
                 column === 'webentry_ed_hs1_from' ||
                 column === 'webentry_ed_hs1_to' ||
                 column === 'webentry_other_hs1_date') {
            value = formatDateToYYYYMMDD(value);
        }
        // Transform operate_type_id
        else if (column === 'operate_type_id') {
            value = transformOperateType(value);
        }
        // Transform webentry_payment
        else if (column === 'webentry_payment') {
            value = transformPayment(value);
        }
        // Transform webentry_other
        else if (column === 'webentry_other') {
            value = transformOther(value);
        }
        // Transform webentry_payment_result
        else if (column === 'webentry_payment_result') {
            value = transformPaymentResult(value);
        }
        // Transform webentry_application
        else if (column === 'webentry_application') {
            value = transformApplication(value);
        }
        // Transform webentry_style
        else if (column === 'webentry_style') {
            value = transformStyle(value);
        }
        // Transform webentry_gender and webentry_g_gender
        else if (column === 'webentry_gender' || column === 'webentry_g_gender') {
            value = transformGender(value);
        }
        // Special case for webentry_g_relation
        else if (column === 'webentry_g_relation') {
            // Handle special case for grandparent (code 5)
            if (String(value) === '5') {
                const gender = record['webentry_g_gender'];
                if (String(gender) === '1') {
                    value = '祖父'; // Grandfather
                } else if (String(gender) === '2') {
                    value = '祖母'; // Grandmother
                } else {
                    value = 'その他'; // Other if gender is null/undefined
                }
            } else {
                // Use the standard mapping for other relation values
                value = transformRelation(value);
            }
        }

        // Transform webentry_ed_jhs_status
        else if (column === 'webentry_ed_jhs_status') {
            value = transformJhsStatus(value);
        }
        // Transform webentry_ed_hs1_curriculum
        else if (column === 'webentry_ed_hs1_curriculum') {
            value = transformHsCurriculum(value);
        }
        // Transform webentry_ed_hs1_status
        else if (column === 'webentry_ed_hs1_status') {
            value = transformHsStatus(value);
        }
        // Transform webentry_free
        else if (column === 'webentry_free') {
            value = transformFree(value);
        }
        // Clean text columns by removing line breaks and normalizing whitespace
        else if (column === 'webentry_hope' || column === 'remarks') {
            value = cleanText(value);
        }
        // Remove hyphens from phone numbers and postal codes
        else if (column === 'webentry_tel1' || column === 'webentry_tel2' || 
                 column === 'webentry_g_tel1' || column === 'webentry_g_tel2' || 
                 column === 'webentry_postal_code' || column === 'webentry_g_postal_code') {
            value = removeHyphens(value);
        }

        // Add this transformation for webentry_status
        else if (column === 'webentry_status') {
            // Direct transformation for integer values
            if (value === 0 || value === '0') {
                value = '出願登録前';
            } else if (value === 1 || value === '1') {
                value = '出願登録中';
            }
        } 
        
        // Add this new condition for parent_id
        else if (column === 'parent_id') {
            // Only add prefix if value exists and isn't empty
            // if (value) {
            //     value = `p${value}`;
            // }
        }
        
        // Set fixed value for admission_judgement_status
        if (column === 'admission_judgement_status') {
            value = 'passed';
        }

        if (column === 'student_pc_address') {
            // Copy the value from webentry_email
            value = record['webentry_email'] !== undefined ? record['webentry_email'] : '';
        }
        
        else if (column === 'webentry_reason') {
            value = cleanText(value);
        }

        processedRecord[column] = value;
    });
    
    return processedRecord;
}

// Reading CSV with manual stream for memory saving
async function readAndParseCSV(filename) {
    try {
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

// Process web entry data in chunks
async function processWebEntryData(webEntryData) {
    try {
        // Initialize tracking counters
        let processedCount = 0;
        
        // Initialize final output file with headers
        const finalFilename = 'processed_admissions.csv';
        const headers = REQUIRED_COLUMNS.map(column => 
            COLUMN_HEADER_MAPPING[column] || column
        ).join(',') + '\n';
        await fs.writeFile(finalFilename, headers);
        
        // Process web entry data in chunks
        const chunks = _.chunk(webEntryData.data, CHUNK_SIZE);
        
        for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];
            console.log(`\n   Processing web entry chunk ${i + 1} of ${chunks.length}`);
            
            const processedChunk = chunk.map(entry => {
                // Apply transformations
                return processRecord(entry);
            });
            
            // Convert chunk to CSV
            const csv = processedChunk.map(record => 
                REQUIRED_COLUMNS.map(column => {
                    const value = record[column];
                    return typeof value === 'string' ? `"${value.replace(/"/g, '""')}"` : (value ?? '');
                }).join(',')
            ).join('\n');
            
            if (processedChunk.length > 0) {
                await fs.appendFile(finalFilename, csv + '\n');
            }
            
            processedCount += processedChunk.length;
            console.log(`   ✓ Processed ${processedCount}/${webEntryData.data.length} web entry records`);
            
            // Free memory
            processedChunk.length = 0;
            
            // Log memory usage
            const currentMemory = process.memoryUsage();
            console.log(`   Current heap used: ${Math.round(currentMemory.heapUsed / 1024 / 1024)}MB`);
        }
        
        // Free memory
        chunks.length = 0;
        
        return {
            finalFilename,
            processedCount
        };
    } catch (error) {
        console.error(`Error processing web entry data: ${error.message}`);
        throw error;
    }
}

async function processWebEntryFile() {
    try {
        console.log('Starting web entry data processing...');
        const initialMemoryUsage = process.memoryUsage();
        console.log(`Initial memory usage: ${Math.round(initialMemoryUsage.heapUsed / 1024 / 1024)}MB`);

        // Step 1: Read web_entry CSV file
        console.log('\n1. Reading web_entry.csv...');
        const webEntryData = await readAndParseCSV('web_entry.csv');
        console.log(`   ✓ Found ${webEntryData.data.length} web entry records`);

        // Step 2: Read student_info_history CSV file
        console.log('\n2. Reading student_info_history.csv...');
        const studentInfoData = await readAndParseCSV('student_info_history.csv');
        console.log(`   ✓ Found ${studentInfoData.data.length} student info history records`);
        
        // Step 3: Read student CSV file
        console.log('\n3. Reading student.csv...');
        const studentData = await readAndParseCSV('student.csv');
        console.log(`   ✓ Found ${studentData.data.length} student records`);
        
        // Step 4: Read customer CSV file
        console.log('\n4. Reading customer.csv...');
        const customerData = await readAndParseCSV('customer.csv');
        console.log(`   ✓ Found ${customerData.data.length} customer records`);

        // Step 5: Group student_info_history data by student_id
        console.log('\n5. Creating student_id to operate_type_id mapping...');
        let studentInfoByStudentId = _.groupBy(studentInfoData.data, 'student_id');
        
        // Step 6: Create student_id to customer_id mapping
        console.log('\n6. Creating student_id to customer_id mapping...');
        let studentCustomerMap = new Map();
        studentData.data.forEach(student => {
            if (student.student_id && student.customer_id) {
                studentCustomerMap.set(String(student.student_id), student.customer_id);
            }
        });
        console.log(`   ✓ Created mapping for ${studentCustomerMap.size} student records`);
        
        // Step 7: Create customer_id to parent_id mapping
        console.log('\n7. Creating customer_id to parent_id mapping...');
        let customerParentMap = new Map();
        customerData.data.forEach(customer => {
            if (customer.customer_id && customer.parent_id) {
                customerParentMap.set(String(customer.customer_id), customer.parent_id);
            }
        });
        console.log(`   ✓ Created mapping for ${customerParentMap.size} customer records`);
        
        // Step 8: Merge data into web entry data
        console.log('\n8. Merging data into web entries...');
        let operateTypeMatchCount = 0;
        let customerIdMatchCount = 0;
        let parentIdMatchCount = 0;
        
        webEntryData.data.forEach(entry => {
            if (entry.student_id) {
                const studentId = String(entry.student_id);
                
                // Add operate_type_id from student_info_history
                const studentRecords = studentInfoByStudentId[studentId];
                if (studentRecords && studentRecords.length > 0) {
                    const latestOperateTypeId = findLatestOperateTypeId(studentRecords, studentId);
                    if (latestOperateTypeId !== null) {
                        entry.operate_type_id = latestOperateTypeId;
                        operateTypeMatchCount++;
                    }
                }
                
                // Add customer_id from student
                if (studentCustomerMap.has(studentId)) {
                    const customerId = studentCustomerMap.get(studentId);
                    entry.customer_id = customerId;
                    customerIdMatchCount++;
                    
                    // Add parent_id from customer using the customer_id
                    if (customerParentMap.has(String(customerId))) {
                        entry.parent_id = customerParentMap.get(String(customerId));
                        parentIdMatchCount++;
                    }
                }
            }
        });
        
        console.log(`   ✓ Merged operate_type_id data for ${operateTypeMatchCount} records`);
        console.log(`   ✓ Merged customer_id data for ${customerIdMatchCount} records`);
        console.log(`   ✓ Merged parent_id data for ${parentIdMatchCount} records`);

        // Free memory
        studentInfoData.data = [];
        studentInfoByStudentId = null;
        studentData.data = [];
        studentCustomerMap.clear();
        customerData.data = [];
        customerParentMap.clear();

        // Step 9: Process and transform the data
        console.log('\n9. Processing web entry data...');
        const result = await processWebEntryData(webEntryData);
        
        // Free memory
        webEntryData.data = [];

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

async function main() {
    console.log('🚀 Starting web entry data processing...\n');
    const startTime = Date.now();
    
    try {
        await processWebEntryFile();
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`\nTotal execution time: ${duration} seconds`);
    } catch (error) {
        console.error('\nScript failed:', error);
        process.exit(1);
    }
}

// Run the script
main();