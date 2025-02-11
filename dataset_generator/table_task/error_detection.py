import random
import os
import pandas as pd
from dataset_generator.table_serializer import TableSerializer
from dataset_generator.table_task.base_table_task import BaseTableTask

class ErrorDetectionTask(BaseTableTask):
    def __init__(self, sample_size: int = 5):
        """
        初始化错误检测任务
        :param sample_size: 额外随机抽取的行数
        """
        self.sample_size = sample_size
    def get_task_descriptions(self, error_type):
        """ 生成任务描述（错误检测） """
        descriptions = [
            "Identify and highlight the error of type {error_type} in the input table.",
            "Analyze the table and detect a data entry that represents a {error_type} error.",
            "Find a specific incorrect value in the table that leads to a {error_type} issue.",
            "Locate a {error_type} error in the given table and mark the corresponding cell.",
            "Review the provided table and determine which cell contains a {error_type} mistake."
        ]
        return random.choice(descriptions).format(error_type=error_type)
    def construct_input(self, entry, file_path):
        """构造输入表格，格式化为 Markdown"""
        dataset_folder = os.path.dirname(file_path)
        dirty_file = os.path.join(dataset_folder, "dirty.csv")

        if not os.path.exists(dirty_file):
            raise FileNotFoundError(f"❌ dirty.csv 文件未找到: {dirty_file}")

        df = pd.read_csv(dirty_file, dtype=str)
        if "row" not in df.columns:
            raise KeyError("❌ dirty.csv 缺少 `row` 列，无法索引行号！")

        # 解析 tuple_pairs 选出相关行
        selected_rows = self._extract_tuple_rows(entry, df)

        # 额外随机抽取几行
        additional_rows = self._sample_additional_rows(df, exclude_ids=selected_rows.index)

        # 合并表格
        final_df = pd.concat([selected_rows, additional_rows])

        # **打乱表格行顺序**
        final_df = final_df.sample(frac=1, random_state=42).reset_index(drop=True)

        # 转换为 Markdown 格式
        return TableSerializer.serialize_df(final_df)

    def construct_output(self, entry):
        """ 生成错误检测任务的标注数据 """
        return {
            "row_id": entry["row_id"],
            "column": entry["column"],
            "error_type": entry["error_type"],
            "error_value": entry["error_value"]
        }

    def _extract_tuple_rows(self, entry, df):
        """ 根据 tuple_pairs 解析出对应行 """
        tuple_pairs = entry.get("tuple_pairs", "")
        if not tuple_pairs:
            return pd.DataFrame()

        row_ids = [int(x.strip()) for x in tuple_pairs.strip("()").split(",") if x.strip().isdigit()]
        return df[df["row"].astype(int).isin(row_ids)]

    def _sample_additional_rows(self, df, exclude_ids):
        """ 额外随机抽取 sample_size 行，排除已选的行 """
        available_rows = df[~df["row"].astype(int).isin(exclude_ids)]
        return available_rows.sample(n=min(self.sample_size, len(available_rows)), random_state=42)