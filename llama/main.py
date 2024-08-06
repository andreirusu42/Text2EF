from os import path
from transformers import AutoModelForCausalLM, AutoTokenizer

model_id = "meta-llama/Meta-Llama-3.1-8B-Instruct"
folder = "llama-3.1-8B-Instruct"

tokenizer = AutoTokenizer.from_pretrained(model_id)
tokenizer.save_pretrained(path.join(folder, "tokenizer"))

model = AutoModelForCausalLM.from_pretrained(model_id)
model.save_pretrained(path.join(folder, "model"))
