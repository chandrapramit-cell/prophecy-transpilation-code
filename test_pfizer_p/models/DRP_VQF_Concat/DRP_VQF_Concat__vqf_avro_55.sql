{{
  config({    
    "materialized": "table",
    "alias": "vqf_avro_55",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH DynamicRename_32 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DRP_VQF_Concat', 'DynamicRename_32') }}

),

Union_35_0 AS (

  SELECT 
    CAST(`Requested By` AS string) AS prophecy_column_5,
    CAST(`Site ID` AS string) AS prophecy_column_10,
    CAST(`Expected Response Date` AS string) AS prophecy_column_24,
    CAST(`Response Provided By` AS string) AS prophecy_column_25,
    CAST(`Data Type` AS string) AS prophecy_column_14,
    CAST(`Change From` AS string) AS prophecy_column_20,
    CAST(studyid AS string) AS prophecy_column_1,
    CAST(`Request Date` AS string) AS prophecy_column_6,
    CAST(`Change To` AS string) AS prophecy_column_21,
    CAST(`Type of Request InquiryChange Request` AS string) AS prophecy_column_9,
    CAST(`Accession If Applicable` AS string) AS prophecy_column_13,
    CAST(FileName AS string) AS prophecy_column_2,
    CAST(`Collection Date in eData Optional` AS string) AS prophecy_column_17,
    CAST(`Response or Resolution Details` AS string) AS prophecy_column_22,
    CAST(`Status 2` AS string) AS prophecy_column_27,
    CAST(Visit AS string) AS prophecy_column_12,
    CAST(`Priority High Medium Low` AS string) AS prophecy_column_7,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(reportrundate AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(reportrundate AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(reportrundate AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(reportrundate AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(reportrundate AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_3,
    CAST(`InquiryRequest Description` AS string) AS prophecy_column_18,
    CAST(`Collection Date in CRF If Available` AS string) AS prophecy_column_16,
    CAST(`Subject ID` AS string) AS prophecy_column_11,
    CAST(`DM Comments` AS string) AS prophecy_column_26,
    CAST(Status AS string) AS prophecy_column_23,
    CAST(`Response Needed By Date` AS string) AS prophecy_column_8,
    CAST(`Expected Vendor Action` AS string) AS prophecy_column_19,
    CAST(`Inquiry Change Request` AS DOUBLE) AS prophecy_column_4,
    CAST(`Additional Identifier eg Lab ID Test Name` AS string) AS prophecy_column_15
  
  FROM DynamicRename_32 AS in0

),

DynamicRename_28 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DRP_VQF_Concat', 'DynamicRename_28') }}

),

Formula_36_0 AS (

  SELECT 
    CAST((
      SUBSTRING(
        CAST((REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(`Subject ID` AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')) AS string), 
        1, 
        4)
    ) AS string) AS `Site ID`,
    *
  
  FROM DynamicRename_28 AS in0

),

Union_35_1 AS (

  SELECT 
    CAST(`Requested By` AS string) AS prophecy_column_5,
    CAST(`Site ID` AS string) AS prophecy_column_10,
    CAST(`Expected Response Date` AS string) AS prophecy_column_24,
    CAST(`Response Provided By` AS string) AS prophecy_column_25,
    CAST(`Data Type` AS string) AS prophecy_column_14,
    CAST(studyid AS string) AS prophecy_column_1,
    CAST(`Request Date` AS string) AS prophecy_column_6,
    CAST(`Type of Request InquiryChange Request` AS string) AS prophecy_column_9,
    CAST(`Accession If Applicable` AS string) AS prophecy_column_13,
    CAST(FileName AS string) AS prophecy_column_2,
    CAST(`Collection Date in eData Optional` AS string) AS prophecy_column_17,
    CAST(`Response or Resolution Details` AS string) AS prophecy_column_22,
    CAST(`Status 2` AS string) AS prophecy_column_27,
    CAST(Visit AS string) AS prophecy_column_12,
    CAST(`Priority High Medium Low` AS string) AS prophecy_column_7,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(reportrundate AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(reportrundate AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(reportrundate AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(reportrundate AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(reportrundate AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_3,
    CAST(`InquiryRequest Description` AS string) AS prophecy_column_18,
    CAST(`Subject ID` AS string) AS prophecy_column_11,
    CAST(`Response Needed By Date` AS string) AS prophecy_column_8,
    CAST(`Inquiry Change Request` AS DOUBLE) AS prophecy_column_4,
    CAST(`Additional Identifier eg Lab ID Test Name` AS string) AS prophecy_column_15
  
  FROM Formula_36_0 AS in0

),

Union_35 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_35_0', 'Union_35_1'], 
      [
        '[{"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "Date"}, {"name": "prophecy_column_18", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_23", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_19", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "Double"}, {"name": "prophecy_column_15", "dataType": "String"}]', 
        '[{"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "Date"}, {"name": "prophecy_column_18", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "Double"}, {"name": "prophecy_column_15", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_35_postRename AS (

  SELECT 
    prophecy_column_20 AS `Change From`,
    prophecy_column_9 AS `Type of Request InquiryChange Request`,
    prophecy_column_19 AS `Expected Vendor Action`,
    prophecy_column_21 AS `Change To`,
    prophecy_column_8 AS `Response Needed By Date`,
    prophecy_column_11 AS `Subject ID`,
    prophecy_column_13 AS `Accession If Applicable`,
    prophecy_column_5 AS `Requested By`,
    prophecy_column_14 AS `Data Type`,
    prophecy_column_16 AS `Collection Date in CRF If Available`,
    prophecy_column_27 AS `Status 2`,
    prophecy_column_1 AS studyid,
    prophecy_column_12 AS Visit,
    prophecy_column_22 AS `Response or Resolution Details`,
    prophecy_column_26 AS `DM Comments`,
    prophecy_column_4 AS `Inquiry Change Request`,
    prophecy_column_10 AS `Site ID`,
    prophecy_column_2 AS FileName,
    prophecy_column_6 AS `Request Date`,
    prophecy_column_15 AS `Additional Identifier eg Lab ID Test Name`,
    prophecy_column_25 AS `Response Provided By`,
    prophecy_column_24 AS `Expected Response Date`,
    prophecy_column_3 AS reportrundate,
    prophecy_column_18 AS `InquiryRequest Description`,
    prophecy_column_7 AS `Priority High Medium Low`,
    prophecy_column_23 AS Status,
    prophecy_column_17 AS `Collection Date in eData Optional`
  
  FROM Union_35 AS in0

),

DynamicRename_39 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['Union_35_postRename'], 
      [
        'Change From', 
        'Type of Request InquiryChange Request', 
        'Expected Vendor Action', 
        'Change To', 
        'Response Needed By Date', 
        'Subject ID', 
        'Accession If Applicable', 
        'Requested By', 
        'Data Type', 
        'Collection Date in CRF If Available', 
        'Status 2', 
        'studyid', 
        'Visit', 
        'Response or Resolution Details', 
        'DM Comments', 
        'Inquiry Change Request', 
        'Site ID', 
        'FileName', 
        'Request Date', 
        'Additional Identifier eg Lab ID Test Name', 
        'Response Provided By', 
        'Expected Response Date', 
        'reportrundate', 
        'InquiryRequest Description', 
        'Priority High Medium Low', 
        'Status', 
        'Collection Date in eData Optional'
      ], 
      'advancedRename', 
      [
        'Change From', 
        'Type of Request InquiryChange Request', 
        'Expected Vendor Action', 
        'Change To', 
        'Response Needed By Date', 
        'Subject ID', 
        'Accession If Applicable', 
        'Requested By', 
        'Data Type', 
        'Collection Date in CRF If Available', 
        'Status 2', 
        'studyid', 
        'Visit', 
        'Response or Resolution Details', 
        'DM Comments', 
        'Inquiry Change Request', 
        'Site ID', 
        'FileName', 
        'Request Date', 
        'Additional Identifier eg Lab ID Test Name', 
        'Response Provided By', 
        'Expected Response Date', 
        'reportrundate', 
        'InquiryRequest Description', 
        'Priority High Medium Low', 
        'Status', 
        'Collection Date in eData Optional'
      ], 
      'Suffix', 
      '', 
      "regexp_replace(column_name, '[ ]', substring('_', 1, 1))"
    )
  }}

),

AlteryxSelect_40 AS (

  SELECT 
    studyid AS study_id,
    FileName AS FileName,
    reportrundate AS reportrundate,
    Inquiry_Change_Request AS Inquiry_Change_Request,
    Requested_By AS Requested_By,
    Request_Date AS Request_Date,
    Priority_High_Medium_Low AS Priority_High_Medium_Low,
    Response_Needed_By_Date AS Response_Needed_By_Date,
    Type_of_Request_InquiryChange_Request AS Type_of_Request_InquiryChange_Request,
    Site_ID AS Site_ID,
    Subject_ID AS Subject_ID,
    Visit AS Visit,
    Accession_If_Applicable AS Accession_If_Applicable,
    Data_Type AS Data_Type,
    Additional_Identifier_eg_Lab_ID_Test_Name AS Additional_Identifier_eg_Lab_ID_Test_Name,
    Collection_Date_in_CRF_If_Available AS Collection_Date_in_CRF_If_Available,
    Collection_Date_in_eData_Optional AS Collection_Date_in_eData_Optional,
    InquiryRequest_Description AS InquiryRequest_Description,
    Expected_Vendor_Action AS Expected_Vendor_Action,
    Change_From AS Change_From,
    Change_To AS Change_To,
    Response_or_Resolution_Details AS Response_or_Resolution_Details,
    Status AS Status,
    Expected_Response_Date AS Expected_Response_Date,
    Response_Provided_By AS Response_Provided_By,
    DM_Comments AS DM_Comments,
    Status_2 AS Status_2
  
  FROM DynamicRename_39 AS in0

),

Filter_44_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_40 AS in0
  
  WHERE (
          NOT (not(contains(Collection_Date_in_eData_Optional, '-')))
          OR isnull(not(contains(Collection_Date_in_eData_Optional, '-')))
        )

),

Filter_54 AS (

  SELECT * 
  
  FROM Filter_44_reject AS in0
  
  WHERE (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Jun'))
                              OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Oct'))
                            )
                            OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Sep'))
                          )
                          OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Aug'))
                        )
                        OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Jul'))
                      )
                      OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Dec'))
                    )
                    OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Jan'))
                  )
                  OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Feb'))
                )
                OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Mar'))
              )
              OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Apr'))
            )
            OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('May'))
          )
          OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Nov'))
        )

),

DateTime_52_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(Collection_Date_in_eData_Optional, 'dd-MMM-yyyy')), 'yyyy-MM-dd')) AS Collection_Date_in_eData_Optional2,
    *
  
  FROM Filter_54 AS in0

),

Filter_44 AS (

  SELECT * 
  
  FROM AlteryxSelect_40 AS in0
  
  WHERE not(contains(Collection_Date_in_eData_Optional, '-'))

),

Formula_71_0 AS (

  SELECT 
    CAST(CASE
      WHEN (
        contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('N/A'))
        OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('NA'))
      )
        THEN NULL
      ELSE Collection_Date_in_eData_Optional
    END AS STRING) AS Collection_Date_in_eData_Optional,
    * EXCEPT (`collection_date_in_edata_optional`)
  
  FROM Filter_44 AS in0

),

DateTime_48_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(Collection_Date_in_eData_Optional, 'ddMMMyyy')), 'yyyy-MM-dd')) AS Collection_Date_in_eData_Optional2,
    *
  
  FROM Formula_71_0 AS in0

),

Filter_54_reject AS (

  SELECT * 
  
  FROM Filter_44_reject AS in0
  
  WHERE (
          NOT (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Jun'))
                                  OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Oct'))
                                )
                                OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Sep'))
                              )
                              OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Aug'))
                            )
                            OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Jul'))
                          )
                          OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Dec'))
                        )
                        OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Jan'))
                      )
                      OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Feb'))
                    )
                    OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Mar'))
                  )
                  OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Apr'))
                )
                OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('May'))
              )
              OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Nov'))
            )
          )
          OR isnull(
               (
                 (
                   (
                     (
                       (
                         (
                           (
                             (
                               (
                                 (
                                   (
                                     contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Jun'))
                                     OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Oct'))
                                   )
                                   OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Sep'))
                                 )
                                 OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Aug'))
                               )
                               OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Jul'))
                             )
                             OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Dec'))
                           )
                           OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Jan'))
                         )
                         OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Feb'))
                       )
                       OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Mar'))
                     )
                     OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Apr'))
                   )
                   OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('May'))
                 )
                 OR contains(coalesce(lower(Collection_Date_in_eData_Optional), ''), lower('Nov'))
               ))
        )

),

DateTime_43_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(Collection_Date_in_eData_Optional, 'yyyy-MM-dd')), 'yyyy-MM-dd')) AS Collection_Date_in_eData_Optional2,
    *
  
  FROM Filter_54_reject AS in0

),

Union_49 AS (

  {{
    prophecy_basics.UnionByName(
      ['DateTime_43_0', 'DateTime_48_0', 'DateTime_52_0'], 
      [
        '[{"name": "Collection_Date_in_eData_Optional2", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "reportrundate", "dataType": "String"}, {"name": "Inquiry_Change_Request", "dataType": "String"}, {"name": "Requested_By", "dataType": "String"}, {"name": "Request_Date", "dataType": "String"}, {"name": "Priority_High_Medium_Low", "dataType": "String"}, {"name": "Response_Needed_By_Date", "dataType": "String"}, {"name": "Type_of_Request_InquiryChange_Request", "dataType": "String"}, {"name": "Site_ID", "dataType": "String"}, {"name": "Subject_ID", "dataType": "String"}, {"name": "Visit", "dataType": "String"}, {"name": "Accession_If_Applicable", "dataType": "String"}, {"name": "Data_Type", "dataType": "String"}, {"name": "Additional_Identifier_eg_Lab_ID_Test_Name", "dataType": "String"}, {"name": "Collection_Date_in_CRF_If_Available", "dataType": "String"}, {"name": "Collection_Date_in_eData_Optional", "dataType": "String"}, {"name": "InquiryRequest_Description", "dataType": "String"}, {"name": "Expected_Vendor_Action", "dataType": "String"}, {"name": "Change_From", "dataType": "String"}, {"name": "Change_To", "dataType": "String"}, {"name": "Response_or_Resolution_Details", "dataType": "String"}, {"name": "Status", "dataType": "String"}, {"name": "Expected_Response_Date", "dataType": "String"}, {"name": "Response_Provided_By", "dataType": "String"}, {"name": "DM_Comments", "dataType": "String"}, {"name": "Status_2", "dataType": "String"}]', 
        '[{"name": "Collection_Date_in_eData_Optional2", "dataType": "String"}, {"name": "Collection_Date_in_eData_Optional", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "reportrundate", "dataType": "String"}, {"name": "Inquiry_Change_Request", "dataType": "String"}, {"name": "Requested_By", "dataType": "String"}, {"name": "Request_Date", "dataType": "String"}, {"name": "Priority_High_Medium_Low", "dataType": "String"}, {"name": "Response_Needed_By_Date", "dataType": "String"}, {"name": "Type_of_Request_InquiryChange_Request", "dataType": "String"}, {"name": "Site_ID", "dataType": "String"}, {"name": "Subject_ID", "dataType": "String"}, {"name": "Visit", "dataType": "String"}, {"name": "Accession_If_Applicable", "dataType": "String"}, {"name": "Data_Type", "dataType": "String"}, {"name": "Additional_Identifier_eg_Lab_ID_Test_Name", "dataType": "String"}, {"name": "Collection_Date_in_CRF_If_Available", "dataType": "String"}, {"name": "InquiryRequest_Description", "dataType": "String"}, {"name": "Expected_Vendor_Action", "dataType": "String"}, {"name": "Change_From", "dataType": "String"}, {"name": "Change_To", "dataType": "String"}, {"name": "Response_or_Resolution_Details", "dataType": "String"}, {"name": "Status", "dataType": "String"}, {"name": "Expected_Response_Date", "dataType": "String"}, {"name": "Response_Provided_By", "dataType": "String"}, {"name": "DM_Comments", "dataType": "String"}, {"name": "Status_2", "dataType": "String"}]', 
        '[{"name": "Collection_Date_in_eData_Optional2", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "reportrundate", "dataType": "String"}, {"name": "Inquiry_Change_Request", "dataType": "String"}, {"name": "Requested_By", "dataType": "String"}, {"name": "Request_Date", "dataType": "String"}, {"name": "Priority_High_Medium_Low", "dataType": "String"}, {"name": "Response_Needed_By_Date", "dataType": "String"}, {"name": "Type_of_Request_InquiryChange_Request", "dataType": "String"}, {"name": "Site_ID", "dataType": "String"}, {"name": "Subject_ID", "dataType": "String"}, {"name": "Visit", "dataType": "String"}, {"name": "Accession_If_Applicable", "dataType": "String"}, {"name": "Data_Type", "dataType": "String"}, {"name": "Additional_Identifier_eg_Lab_ID_Test_Name", "dataType": "String"}, {"name": "Collection_Date_in_CRF_If_Available", "dataType": "String"}, {"name": "Collection_Date_in_eData_Optional", "dataType": "String"}, {"name": "InquiryRequest_Description", "dataType": "String"}, {"name": "Expected_Vendor_Action", "dataType": "String"}, {"name": "Change_From", "dataType": "String"}, {"name": "Change_To", "dataType": "String"}, {"name": "Response_or_Resolution_Details", "dataType": "String"}, {"name": "Status", "dataType": "String"}, {"name": "Expected_Response_Date", "dataType": "String"}, {"name": "Response_Provided_By", "dataType": "String"}, {"name": "DM_Comments", "dataType": "String"}, {"name": "Status_2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_45 AS (

  SELECT * 
  
  FROM Union_49 AS in0
  
  WHERE not(contains(Collection_Date_in_CRF_If_Available, '-'))

),

Formula_72_0 AS (

  SELECT 
    CAST(CASE
      WHEN (
        contains(coalesce(lower(Collection_Date_in_CRF_If_Available), ''), lower('N/A'))
        OR contains(coalesce(lower(Collection_Date_in_CRF_If_Available), ''), lower('NA'))
      )
        THEN NULL
      ELSE Collection_Date_in_CRF_If_Available
    END AS STRING) AS Collection_Date_in_CRF_If_Available,
    * EXCEPT (`collection_date_in_crf_if_available`)
  
  FROM Filter_45 AS in0

),

DateTime_46_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(Collection_Date_in_CRF_If_Available, 'ddMMMyyy')), 'yyyy-MM-dd')) AS Collection_Date_in_CRF_If_Available2,
    *
  
  FROM Formula_72_0 AS in0

),

Filter_45_reject AS (

  SELECT * 
  
  FROM Union_49 AS in0
  
  WHERE (
          NOT (not(contains(Collection_Date_in_CRF_If_Available, '-')))
          OR isnull(not(contains(Collection_Date_in_CRF_If_Available, '-')))
        )

),

DateTime_47_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(Collection_Date_in_CRF_If_Available, 'yyyy-MM-dd')), 'yyyy-MM-dd')) AS Collection_Date_in_CRF_If_Available2,
    *
  
  FROM Filter_45_reject AS in0

),

Union_50 AS (

  {{
    prophecy_basics.UnionByName(
      ['DateTime_46_0', 'DateTime_47_0'], 
      [
        '[{"name": "Collection_Date_in_CRF_If_Available2", "dataType": "String"}, {"name": "Collection_Date_in_CRF_If_Available", "dataType": "String"}, {"name": "Collection_Date_in_eData_Optional2", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "reportrundate", "dataType": "String"}, {"name": "Inquiry_Change_Request", "dataType": "String"}, {"name": "Requested_By", "dataType": "String"}, {"name": "Request_Date", "dataType": "String"}, {"name": "Priority_High_Medium_Low", "dataType": "String"}, {"name": "Response_Needed_By_Date", "dataType": "String"}, {"name": "Type_of_Request_InquiryChange_Request", "dataType": "String"}, {"name": "Site_ID", "dataType": "String"}, {"name": "Subject_ID", "dataType": "String"}, {"name": "Visit", "dataType": "String"}, {"name": "Accession_If_Applicable", "dataType": "String"}, {"name": "Data_Type", "dataType": "String"}, {"name": "Additional_Identifier_eg_Lab_ID_Test_Name", "dataType": "String"}, {"name": "Collection_Date_in_eData_Optional", "dataType": "String"}, {"name": "InquiryRequest_Description", "dataType": "String"}, {"name": "Expected_Vendor_Action", "dataType": "String"}, {"name": "Change_From", "dataType": "String"}, {"name": "Change_To", "dataType": "String"}, {"name": "Response_or_Resolution_Details", "dataType": "String"}, {"name": "Status", "dataType": "String"}, {"name": "Expected_Response_Date", "dataType": "String"}, {"name": "Response_Provided_By", "dataType": "String"}, {"name": "DM_Comments", "dataType": "String"}, {"name": "Status_2", "dataType": "String"}]', 
        '[{"name": "Collection_Date_in_CRF_If_Available2", "dataType": "String"}, {"name": "Collection_Date_in_eData_Optional2", "dataType": "String"}, {"name": "study_id", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "reportrundate", "dataType": "String"}, {"name": "Inquiry_Change_Request", "dataType": "String"}, {"name": "Requested_By", "dataType": "String"}, {"name": "Request_Date", "dataType": "String"}, {"name": "Priority_High_Medium_Low", "dataType": "String"}, {"name": "Response_Needed_By_Date", "dataType": "String"}, {"name": "Type_of_Request_InquiryChange_Request", "dataType": "String"}, {"name": "Site_ID", "dataType": "String"}, {"name": "Subject_ID", "dataType": "String"}, {"name": "Visit", "dataType": "String"}, {"name": "Accession_If_Applicable", "dataType": "String"}, {"name": "Data_Type", "dataType": "String"}, {"name": "Additional_Identifier_eg_Lab_ID_Test_Name", "dataType": "String"}, {"name": "Collection_Date_in_CRF_If_Available", "dataType": "String"}, {"name": "Collection_Date_in_eData_Optional", "dataType": "String"}, {"name": "InquiryRequest_Description", "dataType": "String"}, {"name": "Expected_Vendor_Action", "dataType": "String"}, {"name": "Change_From", "dataType": "String"}, {"name": "Change_To", "dataType": "String"}, {"name": "Response_or_Resolution_Details", "dataType": "String"}, {"name": "Status", "dataType": "String"}, {"name": "Expected_Response_Date", "dataType": "String"}, {"name": "Response_Provided_By", "dataType": "String"}, {"name": "DM_Comments", "dataType": "String"}, {"name": "Status_2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_51 AS (

  SELECT 
    study_id AS study_id,
    FileName AS FileName,
    reportrundate AS report_run_date,
    Inquiry_Change_Request AS Inquiry_Change_Request,
    Requested_By AS Requested_By,
    Request_Date AS Request_Date,
    Priority_High_Medium_Low AS Priority_High_Medium_Low,
    Response_Needed_By_Date AS Response_Needed_By_Date,
    Type_of_Request_InquiryChange_Request AS Type_of_Request_InquiryChange_Request,
    Site_ID AS Site_ID,
    Subject_ID AS Subject_ID,
    Visit AS Visit,
    Accession_If_Applicable AS Accession_If_Applicable,
    Data_Type AS Data_Type,
    Additional_Identifier_eg_Lab_ID_Test_Name AS Additional_Identifier_eg_Lab_ID_Test_Name,
    Collection_Date_in_CRF_If_Available2 AS Collection_Date_in_CRF_If_Available,
    Collection_Date_in_eData_Optional2 AS Collection_Date_in_eData_Optional,
    InquiryRequest_Description AS InquiryRequest_Description,
    Expected_Vendor_Action AS Expected_Vendor_Action,
    Change_From AS Change_From,
    Change_To AS Change_To,
    Response_or_Resolution_Details AS Response_or_Resolution_Details,
    Status AS Status,
    Expected_Response_Date AS Expected_Response_Date,
    Response_Provided_By AS Response_Provided_By,
    DM_Comments AS DM_Comments,
    Status_2 AS Status_2
  
  FROM Union_50 AS in0

)

SELECT *

FROM AlteryxSelect_51
