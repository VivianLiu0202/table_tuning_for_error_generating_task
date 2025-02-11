import random
class BaseTableTask:
    """ 基础表格任务类，所有任务继承它 """
    def __init__(self):
        pass

    def get_task_descriptions(self, error_type):
        """ 任务描述，基于任务类型 (error_generation, error_detection, error_correction) """
        raise NotImplementedError  # 交给子类实现

    def get_error_type_descriptions(self, error_type):
        """ 获取错误类型的描述 """
        descriptions = {
            "rule_violation": [
                "Rule violations occur when data entries fail to satisfy predefined integrity constraints, such as uniqueness, referential integrity, or functional dependencies.",
                "A rule violation happens when data contradicts database constraints, leading to inconsistencies and reducing overall data reliability.",
                "Rule violations include duplicate primary keys, conflicting attribute values, and invalid foreign key references within a dataset.",
                "A rule violation represents a logical inconsistency where data fails to meet structural constraints, potentially causing errors in downstream applications."
            ],
            "pattern_violation": [
                "Pattern violations refer to data values that do not conform to expected syntax, structure, or semantic constraints.",
                "A pattern violation occurs when a data entry deviates from its expected format, such as incorrect date formats or improperly structured email addresses.",
                "Pattern violations include formatting errors, misplaced values, and syntax inconsistencies that disrupt data processing and interpretation.",
                "A pattern violation is an incorrect representation of data that fails to match predefined format rules, affecting readability and usability."
            ],
            "outliers": [
                "Outliers are data points that significantly deviate from the expected distribution within a column, either numerically or categorically.",
                "Outliers represent extreme or unexpected values in a dataset that do not conform to the general pattern of the data.",
                "Outliers are anomalies in numerical or categorical data that can distort statistical analysis and impact decision-making.",
                "Outliers occur when data values fall outside the typical range, often due to measurement errors or data entry mistakes."
            ],
            "missing_value": [
                "Missing values refer to data cells that are empty, incorrectly filled, or implicitly missing due to format conversion issues or inconsistent data integration.",
                "A missing value is an absent or null entry in a dataset, which can lead to incomplete analysis and unreliable predictions.",
                "Missing values occur when essential data is unavailable, either due to collection errors, manual omissions, or placeholder values like 'N/A'.",
                "Missing data represents gaps in a dataset where values are expected but not provided, potentially introducing bias in analysis."
            ]
        }
        return random.choice(descriptions.get(error_type, ["Unknown error type."]))

    def generate_instruction(self, error_type):
        """ 生成完整的 instruction，由任务描述 + 错误类型描述 组成 """
        task_desc = self.get_task_descriptions(error_type)
        error_desc = self.get_error_type_descriptions(error_type)
        return f"{task_desc} {error_desc}"

    def construct_input(self, entry, file_path):
        pass

    def construct_output(self, entry):
        pass