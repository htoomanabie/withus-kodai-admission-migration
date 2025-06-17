// Column mappings for possible variations in field names
export const COLUMN_MAPPINGS = {
    // Phone columns
    'tel': ['tel', 'TEL', 'phone', 'PHONE', 'telephone', 'TELEPHONE'],
    'portable_tel': ['portable_tel', 'PORTABLE_TEL', 'cell_phone', 'CELL_PHONE', 'mobile', 'MOBILE', 'mobile_phone', 'MOBILE_PHONE'],
    
    // Email columns
    'email': ['email', 'EMAIL', 'mail', 'MAIL', 'e_mail', 'E_MAIL'],
    'portable_email': ['portable_email', 'PORTABLE_EMAIL', 'mobile_email', 'MOBILE_EMAIL', 'secondary_email', 'SECONDARY_EMAIL']
};

// Output column name mapping
export const OUTPUT_COLUMN_MAPPING = {
    'MANAERP__Username__c': 'MANAERP__Username__c',
    'kname1': 'LastName',
    'kname2': 'FirstName',
    'fname1': 'MANAERP__Last_Name_Phonetic__c',
    'fname2': 'MANAERP__First_Name_Phonetic__c',
    'student_id': 'MANAERP__External_User_Id__c',
    'student_no': 'Student_ID_number__c',
    'birthday': 'Birthdate',
    'sex': 'GenderIdentity',
    'operate_type_id': 'Student_Category__c',
    'course_type': 'How_to_learn_type__c',
    'branch_id': 'Current_Campus__r:Account:Org_Code__c',
    'main_school_branch_id': 'Current_Main_school__r:Account:Org_Code__c',
    'grade': 'MANAERP__Grade__r:MANAERP__Grade__c:Name',
    'phone': 'Phone',
    'other_phone': 'OtherPhone',
    'main_email': 'Email',
    'sub_email': 'Sub_Email__c',
    'zip_cd': 'MANAERP__Postal_Code__c',
    'pref_id': 'MANAERP__Prefecture__c',
    'address1': 'MANAERP__City__c',
    'address2': 'MANAERP__Street_1__c',
    'address3': 'MANAERP__Street_2__c',
    'entrance_date': 'Date_of_enrollment__c',
    'graduate_date': 'Expected_graduation_date__c',
    'comment': 'Description',
    'dm_sendable': 'DM_sending_flag__c'
};

// Prefecture mappings
export const PREFECTURE_MAPPING = {
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
    '48': 'other',
    // '99': 'その他'
    '99': ''
};

export const DM_SENDABLE_MAPPING = {
    '1': 'TRUE',
    '2': 'FALSE',
    '3': 'FALSE',
    '': 'FALSE',
};

export const GRADE_MAPPING = {
    '16': '小6',
    '21': '中1',
    '22': '中2',
    '23': '中3',
    '31': '高1',
    '32': '高2',
    '33': '高3',
    '39': '-',
    '41': '大1',
    '42': '大2',
    '43': '大3',
    '44': '大4',
    '99': '-'
};
// export const GRADE_MAPPING = {
//     '16': 'a1AHy000006uTCNMA2',
//     '21': 'a1AHy000006uTCSMA2',
//     '22': 'a1AHy000006uTCXMA2',
//     '23': 'a1AHy000006uTCcMAM',
//     '31': 'a1AHy000006uTChMAM',
//     '32': 'a1AHy000006uTCiMAM',
//     '33': 'a1AHy000006uTCmMAM',
//     '39': '卒業',
//     '41': 'a1AHy000006uTCdMAM',
//     '42': 'a1AHy000006uTCrMAM',
//     '43': 'a1AHy000006uTCsMAM',
//     '44': 'a1AHy000006uTCwMAM',
//     '99': 'a1AHy000006uXmfMAE'
// };

export const COURSE_TYPE_MAPPING = {
    '1': '4',
    '2': '2',
    '3': '2',
    '4': '3',
    '5': '1',
    '6': '4',
    '7': '2',
    '8': '1',
    '9': '4'
};

export const OPERATE_TYPE_MAPPING = {
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

export const SEX_MAPPING = {
    '1': '男',
    '2': '女',
    '3': '不明'
};

export const REQUIRED_COLUMNS = [
    'MANAERP__Username__c',
    'kname1',
    'kname2',
    'fname1',
    'fname2',
    'student_id',
    'student_no',
    'birthday',
    'sex',
    'operate_type_id',
    'course_type',
    'branch_id',
    'main_school_branch_id',  // Add this new column
    'grade',
    'phone',           // New column
    'other_phone',     // New column
    'main_email',      // New column
    'sub_email',       // New column
    'zip_cd',
    'pref_id',
    'address1',
    'address2',
    'address3',
    'entrance_date',
    'graduate_date',
    'comment',
    'dm_sendable'
];

// Add this to mappings.js after the other mapping constants

// Branch ID to school name mapping
export const BRANCH_ID_MAPPING = {
    '47': '8901',
    '58': '9256',
    '59': '9257',
    '38': '8861',
    '42': '8873',
    '51': '9100',
    '52': '9200',
    '76': '300157',
    '128': '9511',
    '127': '8987',
    '129': '10901',
    '8': '8320',
    '27': '8654',
    '15': '8440',
    '206': '10300',
    '94': '300181',
    '98': '300185',
    '53': '9250',
    '54': '9252',
    '210': '10100',
    '134': '8876',
    '116': '9510',
    '55': '9253',
    '57': '9255',
    '236': '8991',
    '225': '8947',
    '183': '10317',
    '87': '300172',
    '60': '300024',
    '132': '8874',
    '61': '300040',
    '20': '8463',
    '62': '300059',
    '200': '10600',
    '133': '8875',
    '70': '300151',
    '92': '300177',
    '71': '300152',
    '72': '300153',
    '73': '300154',
    '74': '300155',
    '75': '300156',
    '97': '300184',
    '56': '9254',
    '203': '10903',
    '135': '8877',
    '137': '8881',
    '64': '400070',
    '65': '400080',
    '63': '400060',
    '105': '400601',
    '67': '400100',
    '68': '400101',
    '111': '400901',
    '170': '423005',
    '69': '400102',
    '101': '400201',
    '66': '400090',
    '104': '400501',
    '77': '300158',
    '78': '300159',
    '79': '300160',
    '80': '300161',
    '81': '300163',
    '82': '300166',
    '83': '300167',
    '84': '300168',
    '86': '300170',
    '88': '300173',
    '90': '300175',
    '91': '300176',
    '93': '300180',
    '99': '300186',
    '107': '400700',
    '182': '8900',
    '184': '10200',
    '109': '400800',
    '208': '8990',
    '102': '400301',
    '108': '400710',
    '48': '8906',
    '204': '20300',
    '185': '10302',
    '-1': '0',
    '209': '888888',
    '207': '8992',
    '201': '6721',
    '125': '8925',
    '117': '8905',
    '211': '9506',
    '186': '10906',
    '194': '8924',
    '219': '8946',
    '136': '8880',
    '173': '8942',
    '187': '353',
    '193': '10304',
    '188': '10500',
    '189': '10800',
    '190': '300',
    '191': '10907',
    '192': '10362',
    '195': '9998',
    '196': '10303',
    '197': '9995',
    '198': '10301',
    '122': '8911',
    '147': '8888',
    '148': '8889',
    '172': '8993',
    '130': '8879',
    '123': '8915',
    '124': '8916',
    '175': '8935',
    '121': '8909',
    '114': '8903',
    '115': '8904',
    '112': '8913',
    '145': '8923',
    '205': '6720',
    '199': '10363',
    '30': '8740',
    '45': '8884',
    '146': '8887',
    '113': '8912',
    '159': '8937',
    '19': '8462',
    '24': '8650',
    '131': '8878',
    '228': '987924',
    '140': '300190',
    '103': '400401',
    '141': '300191',
    '162': '315002',
    '229': '987927',
    '166': '314001',
    '95': '300182',
    '96': '300183',
    '100': '300188',
    '176': '313003',
    '106': '400602',
    '169': '423004',
    '164': '309001',
    '163': '311001',
    '165': '313002',
    '179': '423007',
    '180': '423008',
    '238': '313006',
    '242': '313007',
    '153': '987915',
    '150': '987922',
    '167': '987925',
    '178': '987911',
    '202': '987926',
    '212': '987933',
    '220': '987930',
    '143': '352',
    '222': '8949',
    '241': '8953',
    '126': '8930',
    '50': '8952',
    '223': '313005',
    '215': '315003',
    '168': '987928',
    '49': '8950',
    '138': '8951',
    '158': '8936',
    '157': '8934',
    '156': '8933',
    '155': '8932',
    '154': '8931',
    '174': '8940',
    '181': '8943',
    '216': '8944',
    '221': '8948',
    '3': '8240',
    '4': '8250',
    '5': '8260',
    '12': '8360',
    '40': '8871',
    '9': '8330',
    '10': '8340',
    '217': '8351',
    '11': '8350',
    '13': '8420',
    '171': '8410',
    '17': '8460',
    '14': '8430',
    '213': '8431',
    '227': '8732',
    '22': '8620',
    '21': '8464',
    '44': '8883',
    '23': '8640',
    '41': '8872',
    '18': '8461',
    '26': '8652',
    '28': '8720',
    '32': '8760',
    '177': '8731',
    '110': '987932',
    '85': '987916',
    '89': '987923',
    '139': '987917',
    '142': '987929',
    '151': '987913',
    '214': '987914',
    '152': '987912',
    '29': '8730',
    '31': '8750',
    '34': '8830',
    '33': '8820',
    '37': '8860',
    '36': '8850',
    '35': '8840',
    '39': '8870',
    '230': '8370',
    '224': '8421',
    '231': '8470',
    '160': '8938',
    '161': '8939',
    '218': '8945',
    '144': '8921',
    '1': '8220',
    '2': '8230',
    '7': '8280',
    '16': '8450',
    '6': '8270',
    '25': '8651',
    '232': '8670',
    '233': '8770',
    '240': '8371',
    '226': '8422',
    '239': '8811',
    '43': '8882',
    '46': '8885',
    '149': '8922',
    '234': '8810',
    '237': '8660'
  };

// Helper function to pad number with leading zeros to make it 6 digits
export function padToSixDigits(value) {
    if (!value) return value;
    const strValue = String(value);
    return strValue.padStart(6, '0');
}

// Function to get output column names
export function getOutputColumnNames() {
    return REQUIRED_COLUMNS.map(column => OUTPUT_COLUMN_MAPPING[column] || column);
}

// Helper functions for data mappings
export function transformPrefecture(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return PREFECTURE_MAPPING[strValue] || value;
}

export function transformDmSendable(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return DM_SENDABLE_MAPPING[strValue] || value;
}

export function transformGrade(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return GRADE_MAPPING[strValue] || '-';
}

export function transformPhone(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return strValue === '0' ? '' : strValue;
}

export function transformCourseType(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return COURSE_TYPE_MAPPING[strValue] || value;
}

export function transformOperateType(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return OPERATE_TYPE_MAPPING[strValue] || value;
}

export function transformSex(value) {
    // Convert to string to ensure proper lookup
    const strValue = String(value);
    return SEX_MAPPING[strValue] || value;
}

// Add transformation for branch_id and main_school_branch_id
export function transformBranchId(value) {
    // First get the mapped value
    const strValue = String(value);
    const mappedValue = BRANCH_ID_MAPPING[strValue] || value;
    // Then pad it to 6 digits
    return padToSixDigits(mappedValue);
}

export function transformMainSchoolBranchId(value) {
    return padToSixDigits(value);
}

// Helper function to get a field value using the column mappings
export function getFieldValue(record, fieldName) {
    if (!record) return '';
    
    // If the field exists directly, return it
    if (record[fieldName] !== undefined) {
        return record[fieldName];
    }
    
    // Check alternative column names from the mapping
    if (COLUMN_MAPPINGS[fieldName]) {
        for (const altName of COLUMN_MAPPINGS[fieldName]) {
            if (record[altName] !== undefined) {
                return record[altName];
            }
        }
    }
    
    // Field not found
    return '';
}