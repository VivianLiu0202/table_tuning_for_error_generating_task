import random
from table_task.base_table_task import BaseTableTask

class PromptGenerator:
    @staticmethod
    def generate_instruction(table_task: BaseTableTask, error_type: str) -> str:
        """ 生成完整的 instruction，由任务描述 + 错误类型描述 组成 """
        task_desc = table_task.get_task_descriptions(error_type)
        error_desc = table_task.get_error_type_descriptions(error_type)
        return f"{task_desc or 'Describe the error.'} {error_desc or 'No specific description available.'}"
