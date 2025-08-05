# student category 
follow the below rule
```
39 専攻科 => 2: 専攻科
40 専門カレッジ => 10: 専門カレッジ
41 選科 => 4: 選科
42 高認・大学受験 => 9: 高認
43 個別 => 9: 高認
44 高認通信 => 9: 高認
38 本科 => 1: 本科
48 中等部 => 5: 中等部
47 フリースクール => 8: フリースクール
46 聴講生 => 3: 聴講生
51 オンラインカレッジ => 6: オンラインカレッジ
53 BASE => 7: BASE

If there are no student_info_history, follow the 1st letter of student_id
the 1st letter of student_id = 1 => 9: 高認
the 1st letter of student_id = 4 => 1: 本科
the 1st letter of student_id = 5 => Out of migation target

----------------
Out of migation target
45 イベント
49 新潟産業大学

★Please use the latest info
```

current main school and current campus
If current main school is 43 or 46 -> current main school is correct and current campus can be null
If current campus is not NULL -> need to find the main school and fill in current main school field 


Phone and OtherPhone
- tel is not NULL AND portable_tel is not NULL
THEN portable_tel is Phone and tel is Other Phone
- tel is not NULL AND portable_tel is NULL
THEN tel is Phone
- tel is NULL AND portable_tel is not NULL
THEN portable_tel is Phone
- tel is NULL AND portable_tel is NULL
THEN both of them are NULL

Main Email and Sub Email
- email is not NULL AND portable_email is not NULL
THEN portable_email is Main Email and email is Sub Email
-email is not NULL AND portable_email is NULL
THEN email is Main Email
- email is NULL AND portable_email is not NULL
THEN portable_email is Main Email
-email is NULL AND portable_email is NULL
THEN both of them are NULL

==================
Update from FR
==================
HAUFM001 (Student), HAUFM002 (Parent) -> Core Id , Billing Permission flag 
HAUFM003.csv（Relationship (Student-Parent)
HAUFM004.csv（Relationship (Student-Student)）
HAUFM009.csv（Staff）
HAUFM010.csv（School History)
HAUFM012.csv（Enrollment Status）



