from huggingface_hub import snapshot_download
import os
os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"

snapshot_download(repo_id="meta-llama/Meta-Llama-3.1-8B", local_dir="./llama3.1-8B", token="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")