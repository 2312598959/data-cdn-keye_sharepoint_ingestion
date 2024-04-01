import os
from io import BytesIO
from loguru import logger
import pandas as pd
from datetime import datetime

from core.oss import OSS
from core.sharepoint import SharePoint
from util.date_util import get_formatted_file_name, get_max_date, convert_to_date
from util.excel import verify_excel_sheet
from util.excel import verify_excel_header
from util.excel import verify_excel_data
from util.excel import get_excel_df

validate_config_excel = "ke/check_folder/校验配置表.xlsx"
oss_error_folder = "ke/error_folder"
oss_archive_folder = "ke/archive_folder"
oss_data_folder = "ke/data_folder"
oss_check_log = "ke/check_log"
sp_user_folder = "01 - User Folder"
sp_archive_folder = "Archive Folder"
oss_endpoint = "http://oss-cn-hangzhou.aliyuncs.com"
oss_bucket = "kering-cdn-prod-keye"
access_key_id = "LTAI5tBM1YAfQcdLwK1MtZjm"
access_key_secret = "oooRsXl6Kb2nKIO57sbPml2RqfF6aj"

sp_username = "serv.yechtech@kering.com"
sp_password = "11qqaazz!!QQAAZZ"
sp_website = "https://kering.sharepoint.com"
sp_site = "https://kering.sharepoint.com/sites/Kering_Eyewear/"
sp_library_name = "KEYE DIGITAL WARRANTY CARD BI"

current_date_str = datetime.now().strftime("%Y%m%d")

# Create an OSS instance
oss = OSS(
    temp_creds=None,
    oss_endpoint=oss_endpoint,
    oss_bucket=oss_bucket,
    access_key_id=access_key_id,
    access_key_secret=access_key_secret,
)
bucket = oss.get_bucket()

# Create sharepoint sp instance
sp = SharePoint(
    sharepoint_username=sp_username,
    sharepoint_password=sp_password,
    sharepoint_website=sp_website,
    sharepoint_site=sp_site,
    sharepoint_library_name=sp_library_name,
)
site = sp.get_site()

validate_config_data_stream = oss.get_oss_data_stream(bucket, validate_config_excel)

validate_config_df, validate_sheet_list = get_excel_df(
    data_stream=validate_config_data_stream
)

verify_file = validate_sheet_list[0]
verify_sheet = validate_sheet_list[1]
verify_header = validate_sheet_list[2]
verify_data = validate_sheet_list[3]
rename_header = validate_sheet_list[4]

verify_file_df = validate_config_df[verify_file]

# Get the list of all file names that need validation
verify_file_list = verify_file_df.iloc[:, 0].tolist()
logger.info(f"Validate file name list -> {verify_file_list}")

# Get the list of all floders that need validation
verify_floder_list = [name.split("-")[0] for name in verify_file_list]
logger.info(f"Validate floder name list -> {verify_floder_list}")

# Create dataFrame of validation_result_table
validation_result_table = pd.DataFrame(
    columns=[
        "table_name",
        "file_name",
        "sheet",
        "column_name",
        "file_Validation",
        "sheet_Validation",
        "zero_data_volume",
        "data_amount_Validation",
        "header_validation",
        "field_content_validation",
    ]
)


# Recursively read a sp folder
for sp_folder in verify_floder_list:
    table_name = None
    file_name = None
    sheet = None
    column_name = None
    file_Validation = None
    sheet_Validation = None
    zero_data_volume = None
    data_amount_Validation = None
    header_validation = None
    field_content_validation = None
    column_name_list = []
    column_name_dict = {}

    sp_user_file_folder = f"{sp_user_folder}/{sp_folder}"
    sp_archive_successful_file_folder = f"{sp_archive_folder}/{sp_folder}/success/"
    sp_archive_error_file_folder = f"{sp_archive_folder}/{sp_folder}/error/"

    # Get a list of file names from sp
    sp_file_list = sp.get_sharepoint_file_list(site, sp_user_file_folder)

    (
        sp_formatted_file_list,
        sp_validated_file_list,
        sp_validated_file_dict,
    ) = get_formatted_file_name(sp_file_list, verify_file_list)

    logger.info(f"Start verify file of folder -> '{sp_folder}'")
    success_file_list = []

    table_name = sp_folder

    # Recursively read a sp file
    for sp_file in sp_file_list:
        file_name = sp_file

        (
            sp_file_object,
            sp_data_stream,
        ) = sp.get_sharepoint_data_stream(site, sp_user_file_folder, sp_file)

        # Get sp latest file date
        archive_successful_file_list = sp.get_sharepoint_file_list(
            site, sp_archive_successful_file_folder
        )
        if not archive_successful_file_list:
            logger.info(f"Archive_successful_file_list is none")
        else:
            # Format date_str
            date_str_list = [
                item.split("-")[1].split(".")[0]
                for item in archive_successful_file_list
            ]
            max_date = get_max_date(date_str_list)
            logger.info(
                f"The last validation's maximum file date:'{max_date}' -> '{sp_file}'"
            )

        # Verify file name
        if sp_file in sp_validated_file_list:
            date_spart = sp_file.split("-")[1].split(".")[0]
            date_spart = convert_to_date(date_spart)
            if not archive_successful_file_list or date_spart >= max_date:
                file_Validation = 0
                logger.info(f"Start verify file -> {sp_file}")
            else:
                file_Validation = 1
                logger.info(
                    f"This file's date is before the last verified one ->'{sp_file}'"
                )
                continue
        else:
            file_Validation = 1
            logger.info(f"The file is not within the verification rules -> '{sp_file}'")
            continue

        # Verify sheet
        sp_formatted_file = sp_validated_file_dict[sp_file]
        logger.info(f"sp_formatted_file -> {sp_formatted_file}")

        verify_sheet_df_tmp = validate_config_df[verify_sheet]

        verify_sheet_df = verify_sheet_df_tmp[
            (verify_sheet_df_tmp["文件名"] == sp_formatted_file)
        ]
        verify_sheet_list = verify_sheet_df["sheet页"].tolist()

        for verify_sheet_name in verify_sheet_list:
            sp_file_df, sp_file_sheet_list = get_excel_df(data_stream=sp_data_stream)

            # File contains all target sheets
            if verify_sheet_name in sp_file_sheet_list:
                sheet_Validation = 0
                logger.info(f"Sheet list to be validated -> {verify_sheet_list}")
            else:
                logger.info(f"Sheet validated failed -> {verify_sheet_name}")
                sheet_Validation = 1
                continue

            sheet = verify_sheet_name
            sp_file_df, sp_file_sheet_list = get_excel_df(data_stream=sp_data_stream)
            original_data_df = sp_file_df[verify_sheet_name]

            # verify excel sheet
            compare_result, original_count = verify_excel_sheet(
                sp_formatted_file,
                verify_sheet_name,
                verify_sheet_df,
                original_data_df,
            )

            if original_count == 0:
                zero_data_volume = 1
                logger.info("Empty file")
                continue
            else:
                zero_data_volume = 0
                if compare_result:
                    data_amount_Validation = 1
                    logger.info("Data amount in sheet is abnormal ")
                else:
                    data_amount_Validation = 0
                    logger.info(f"Data amount in sheet is normal -> {original_count}")
                    logger.info(f"Start verify sheet header-> '{verify_sheet_name}'")

            # Verify header
            verify_header_df_tmp = validate_config_df[verify_header]

            verify_header_df = verify_header_df_tmp[
                (verify_header_df_tmp["文件名"] == sp_formatted_file)
                & (verify_header_df_tmp["sheet页"] == verify_sheet_name)
            ]
            if verify_header_df.empty:
                header_validation = 1
                logger.info(f"Missing header validation rules ->'{verify_sheet_name}'")
                continue
            else:
                header_validation = 0
                logger.info(f"Start verify header -> '{verify_sheet_name}'")

            (
                table_data_df,
                original_formatted_column_list,
                column_end_index,
                table_name,
                verify_column_list,
            ) = verify_excel_header(verify_sheet_name, verify_header_df, sp_data_stream)

            original_field_num = table_data_df.shape[1]
            # Compare columns
            if original_field_num >= column_end_index:
                header_validation = 0
                logger.info("Field count is reasonable")
                if original_formatted_column_list == verify_column_list:
                    header_validation = 0
                    logger.info(f"Header validation passed")
                    table_data_df.columns = original_formatted_column_list
                else:
                    header_validation = 1
                    logger.info(
                        f"Header validation failed -> {original_formatted_column_list}"
                    )
                    continue
            else:
                header_validation = 1
                logger.info(f"Header validation failed -> {original_field_num}")
                continue

            # Verify excel data
            verify_data_df_tmp = validate_config_df[verify_data]

            verify_data_df = verify_data_df_tmp[
                (verify_data_df_tmp["表关键词"] == sp_formatted_file.split("-")[0])
            ]

            # column_name_list, column_name_dict = verify_excel_data(
            #     verify_sheet_name, verify_data_df, table_data_df
            # )

            # Upload oss
            parquet_object_file = f"{oss_data_folder}/{table_name}/{current_date_str}/ke_{table_name}.parquet"

            logger.info(f"parquet_object_file -> {parquet_object_file}")

            # Replace columns
            rename_header_df = validate_config_df[rename_header]
            table_columns = rename_header_df[table_name].tolist()

            # Remove missing values
            table_columns = [item for item in table_columns if pd.notnull(item)]

            # Compare columns
            if len(table_columns) == table_data_df.shape[1]:
                table_data_df.columns = table_columns
            else:
                logger.info(
                    f"Length of table columns -> {len(table_columns)}, Number of columns in target data -> {table_data_df.shape[1]}"
                )
                continue

            parquet_data_stream = BytesIO()
            table_data_df.to_parquet(parquet_data_stream, engine="pyarrow")

            # Resetting the pointer of BytesIO
            parquet_data_stream.seek(0)

            # Upload a successful file to OSS in parquet format
            oss.upload_to_oss(
                bucket, object_name=parquet_object_file, data_stream=parquet_data_stream
            )

            # Archive successful files
            excel_successful_object_file = (
                f"{oss_archive_folder}/{current_date_str}/{sp_file}"
            )
            logger.info(f"excel_object_file -> {excel_successful_object_file}")

            # Resetting the pointer of BytesIO
            sp_data_stream.seek(0)

            # Upload file to OSS in excel format
            oss.upload_to_oss(
                bucket,
                object_name=excel_successful_object_file,
                data_stream=sp_data_stream,
            )
            success_file_list.append(sp_file)

    # for column_name in column_name_list:
    #     field_content_validation = column_name_dict[column_name]
    #     validation_result_table = validation_result_table._append(
    #         {
    #             "table_name": table_name,
    #             "file_name": file_name,
    #             "sheet": sheet,
    #             "column_name": column_name,
    #             "file_Validation": file_Validation,
    #             "sheet_Validation": sheet_Validation,
    #             "zero_data_volume": zero_data_volume,
    #             "data_amount_Validation": data_amount_Validation,
    #             "header_validation": header_validation,
    #             "field_content_validation": field_content_validation,
    #         },
    #         ignore_index=True,
    #     )

    # Archive error files
    for sp_file in sp_file_list:
        (
            sp_file_object,
            sp_data_stream,
        ) = sp.get_sharepoint_data_stream(site, sp_user_file_folder, sp_file)
        if sp_file not in success_file_list:
            excel_error_object_file = (
                f"{oss_error_folder}/{sp_folder}/{current_date_str}/{sp_file}"
            )
            logger.info(f"excel_object_file -> {excel_error_object_file}")

            # Resetting the pointer of BytesIO
            sp_data_stream.seek(0)

            # Upload file to OSS in excel format
            oss.upload_to_oss(
                bucket, object_name=excel_error_object_file, data_stream=sp_data_stream
            )
            # Archive sp error file
            sp.archive_sharepoint_file(
                site,
                sp_file_object,
                sp_archive_error_file_folder,
                sp_file,
                sp_user_file_folder,
            )
        else:
            # Archive sp successful file
            sp.archive_sharepoint_file(
                site,
                sp_file_object,
                sp_archive_successful_file_folder,
                sp_file,
                sp_user_file_folder,
            )

    logger.info(f"Files in the folder have been validated -> '{sp_folder}'")

# Validation log upload oss
parquet_validation_log = (
    f"{oss_check_log}/{current_date_str}/raw_ke_t_check_log.parquet"
)


parquet_data_stream = BytesIO()
validation_result_table.to_parquet(parquet_data_stream, engine="pyarrow")

# Resetting the pointer of BytesIO
parquet_data_stream.seek(0)

# Upload a successful file to OSS in parquet format
oss.upload_to_oss(
    bucket, object_name=parquet_validation_log, data_stream=parquet_data_stream
)

# Create success indicator
success_indicator = f"{oss_data_folder}/{current_date_str}/_SUCCESS"
bucket.put_object(success_indicator, "")
logger.info(f"all files have been validated")


file_name = "output.xlsx"
validation_result_table.to_excel(file_name, index=False)
