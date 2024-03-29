import json
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

from fastapi import FastAPI, Request
from core.credentials import Temporary
from core.kms import get_secret


api = FastAPI()

@api.post("/invoke")
async def invoke(incoming_request: Request):
    # Show version
    logger.info("1.0")

    # Check incoming request header
    logger.info(f"Request header -> {incoming_request.headers}")

    # Get incoming request body
    body = await incoming_request.json()
    logger.info(f"Request body -> {body}")

    # Get incoming payload
    payload = body["payload"]
    logger.info(f"payload -> {payload}")

    oss_endpoint = payload["oss_endpoint"]
    oss_bucket = payload["oss_bucket"]
    validate_config_excel = payload["validate_config_excel"]
    oss_error_folder = payload["oss_error_folder"]
    oss_archive_folder = payload["oss_archive_folder"]
    oss_data_folder = payload["oss_data_folder"]
    oss_check_log = payload["oss_check_log"]

    sp_username = payload["sp_username"]
    sp_website = payload["sp_website"]
    sp_site = payload["sp_site"]
    sp_library_name = payload["sp_library_name"]
    sp_user_folder = payload["sp_user_folder"]
    sp_archive_folder = payload["sp_archive_folder"]
    access_key_id = payload.get("access_key_id")
    access_key_secret = payload.get("access_key_secret")

    # Init temporary credentials
    temp_creds = Temporary(
        access_key_id=incoming_request.headers["x-fc-access-key-id"],
        access_key_secret=incoming_request.headers["x-fc-access-key-secret"],
        security_token=incoming_request.headers["x-fc-security-token"],
    )

    try:
        # Get KMS secret
        kms_secret = get_secret(
            temp_creds=temp_creds,
            region=payload["kms_secret_region"],
            name=payload["kms_secret_cdn_ke_fc_sharepoint_dev"],
        )
        sp_password = kms_secret["KE_SHAREPOINT_PASSWORD"]
    except Exception as e:
        logger.info("Failed to kms_secret:", e)
        sp_password = payload.get("sp_password")
    current_date_str = datetime.now().strftime("%Y%m%d")

    # Create an OSS instance
    oss = OSS(
        temp_creds=temp_creds,
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

    # Recursively read a sp folder
    for sp_folder in verify_floder_list:
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

        # Recursively read a sp file
        for sp_file in sp_file_list:
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
                    logger.info(f"Start verify file -> {sp_file}")
                else:
                    logger.info(
                        f"This file's date is before the last verified one ->'{sp_file}'"
                    )
                    continue
            else:
                logger.info(
                    f"The file is not within the verification rules -> '{sp_file}'"
                )
                continue

            # Verify sheet
            sp_formatted_file = sp_validated_file_dict[sp_file]
            logger.info(f"sp_formatted_file -> {sp_formatted_file}")

            verify_sheet_df_tmp = validate_config_df[verify_sheet]

            verify_sheet_df = verify_sheet_df_tmp[
                (verify_sheet_df_tmp["文件名"] == sp_formatted_file)
            ]
            verify_sheet_list = verify_sheet_df["sheet页"].tolist()
            logger.info(f"Sheet list to be validated -> {verify_sheet_list}")

            for verify_sheet_name in verify_sheet_list:
                sp_file_df, sp_file_sheet_list = get_excel_df(
                    data_stream=sp_data_stream
                )
                original_data_df = sp_file_df[verify_sheet_name]

                # verify excel sheet
                compare_result, original_count = verify_excel_sheet(
                    sp_formatted_file,
                    verify_sheet_name,
                    verify_sheet_df,
                    original_data_df,
                )

                if original_count == 0:
                    logger.info("Empty file")
                    continue
                else:
                    if compare_result:
                        logger.info("Data amount in sheet is abnormal ")
                    else:
                        logger.info(
                            f"Data amount in sheet is normal -> {original_count}"
                        )
                        logger.info(
                            f"Start verify sheet header-> '{verify_sheet_name}'"
                        )

                # Verify header
                verify_header_df_tmp = validate_config_df[verify_header]

                verify_header_df = verify_header_df_tmp[
                    (verify_header_df_tmp["文件名"] == sp_formatted_file)
                    & (verify_header_df_tmp["sheet页"] == verify_sheet_name)
                ]
                if verify_header_df.empty:
                    logger.info(
                        f"Missing header validation rules ->'{verify_sheet_name}'"
                    )
                    continue
                else:
                    logger.info(f"Start verify header -> '{verify_sheet_name}'")

                (
                    table_data_df,
                    original_formatted_column_list,
                    column_end_index,
                    table_name,
                    verify_column_list,
                ) = verify_excel_header(
                    verify_sheet_name, verify_header_df, sp_data_stream
                )

                original_field_num = table_data_df.shape[1]
                # Compare columns
                if original_field_num >= column_end_index:
                    logger.info("Field count is reasonable")
                    if original_formatted_column_list == verify_column_list:
                        logger.info("Header validation passed")
                        table_data_df.columns = original_formatted_column_list
                    else:
                        logger.info("Header validation failed")
                        continue
                else:
                    logger.info("Header validation failed")
                    continue

                # Verify excel data
                verify_data_df_tmp = validate_config_df[verify_data]

                verify_data_df = verify_data_df_tmp[
                    (verify_data_df_tmp["表关键词"] == sp_formatted_file.split("-")[0])
                ]

                verify_excel_data(verify_sheet_name, verify_data_df, table_data_df)

                # Upload oss
                parquet_object_file = f"{oss_data_folder}/{table_name}/{current_date_str}/ke_{table_name}.parquet"
                success_indicator = f"{oss_data_folder}/{current_date_str}/_SUCCESS"

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
                    bucket,
                    object_name=parquet_object_file,
                    data_stream=parquet_data_stream,
                )

                # Create success indicator
                bucket.put_object(success_indicator, "")

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
                    bucket,
                    object_name=excel_error_object_file,
                    data_stream=sp_data_stream,
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

    # TODO：sp手工表校验日志

    logger.info(f"all files have been validated")
