from datetime import datetime
from loguru import logger


def is_valid_date_format(date_string, min_year=2020, max_year=2099):
    length_dict = {8: "%Y%m%d", 6: "%Y%m", 4: "%Y"}

    fmt = length_dict.get(len(date_string))

    if fmt:
        try:
            dt = datetime.strptime(date_string, fmt)

            if fmt == "%Y%m%d":
                month = dt.month
                day = dt.day

                if (
                    1 <= month <= 12
                    and 1 <= day <= 31
                    and min_year <= dt.year <= max_year
                ):
                    return (True, "yyyymmdd")
                else:
                    return (False, None)

            elif fmt == "%Y%m":
                month = dt.month

                if 1 <= month <= 12 and min_year <= dt.year <= max_year:
                    return (True, "yyyymm")

            elif fmt == "%Y":
                if min_year <= dt.year <= max_year:
                    return (True, "yyyy")
                else:
                    return (False, None)

        except ValueError:
            pass

    return (False, None)


def get_formatted_file_name(sharepoint_file_names, check_file_name_list):
    formatted_file_names = []
    file_names = []
    file_name_dict = {}
    for file_name in sharepoint_file_names:
        # Check file extension
        if file_name.endswith((".xlsx", ".xlsb")):
            if "-" in file_name:
                split_parts = file_name.split("-")
                if len(split_parts) > 1:
                    split_suffix = split_parts[1].split(".")[0]
                    split_prefix = split_parts[0]
                    is_valid, date_format = is_valid_date_format(
                        date_string=split_suffix
                    )
                    if is_valid:
                        formatted_file_name = f"{split_prefix}-{date_format}"
                        if formatted_file_name in check_file_name_list:
                            logger.info(f"File validate passed -> {file_name}")
                            formatted_file_names.append(formatted_file_name)
                            file_names.append(file_name)
                            file_name_dict[file_name] = formatted_file_name
                        else:
                            logger.info(
                                f"The file is not within the verification scope -> {file_name}"
                            )
                    else:
                        logger.info(
                            f"File extension does not conform to the date format requirement -> {file_name}"
                        )
            else:
                logger.info(
                    f"File extension does not conform to the date format requirement -> {file_name}"
                )
        else:
            logger.info(f"The file format is incorrect -> {file_name}")
    return formatted_file_names, file_names, file_name_dict


def convert_to_date(date_str):
    formats = ["%Y", "%Y%m", "%Y%m%d"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    raise ValueError(f"Cannot recognize the date format -> {date_str}")


def get_max_date(date_str_list):
    dates = [convert_to_date(ds) for ds in date_str_list]
    return max(dates, key=lambda x: x.date())
