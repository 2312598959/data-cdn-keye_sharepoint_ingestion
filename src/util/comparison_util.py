from loguru import logger

compare = {
    ">": lambda x, y: x > y,
    "<": lambda x, y: x < y,
    ">=": lambda x, y: x >= y,
    "<=": lambda x, y: x <= y,
    "==": lambda x, y: x == y,
}


def compare_count(comparison_operator, data_count, target_count):
    comparison_func = compare.get(comparison_operator)

    if comparison_func is not None:
        return comparison_func(data_count, target_count)
    else:
        logger.info("无效的比较运算符")
        return False
