import os
import sys
import torch
from datetime import datetime
from transformers import AutoConfig, AutoTokenizer, VisionEncoderDecoderModel, DonutProcessor
from pytorch_lightning import LightningModule

if len(sys.argv) <2:
    print("please provide the model name")

model_name = sys.argv[1]

class YourModel(LightningModule):
    def __init__(self, model_name_or_path):
        super().__init__()
        self.model = VisionEncoderDecoderModel.from_pretrained(model_name_or_path)
        self.tokenizer = AutoTokenizer.from_pretrained(model_name_or_path)
        
    def forward(self, pixel_values, decoder_input_ids, labels=None):
        return self.model(pixel_values=pixel_values, decoder_input_ids=decoder_input_ids, labels=labels)

def convert_ckpt_to_huggingface(ckpt_path, save_dir, model_name_or_path):
    # Load checkpoint
    checkpoint = torch.load(ckpt_path, map_location=torch.device('cpu'))
    
    # Load model
    model = YourModel(model_name_or_path)
    
    # Load state dict
    model.load_state_dict(checkpoint['state_dict'])
    
    # Save the model and tokenizer to the Hugging Face format
    model.model.save_pretrained(save_dir)
    model.tokenizer.save_pretrained(save_dir)
    
    # Save the preprocessor config (DonutProcessor)
    processor = DonutProcessor.from_pretrained(model_name_or_path)
    processor.save_pretrained(save_dir)
# Example usage
current_date = datetime.now()
formatted_date = current_date.strftime("%d-%b-%Y")

destination_dir = f"{model_name}-{formatted_date}"

os.makedirs(destination_dir, exist_ok=True)

ckpt_path = f"saved_model/{model_name}-{formatted_date}-v1.ckpt"
save_dir = destination_dir
model_name_or_path = model_name

convert_ckpt_to_huggingface(ckpt_path, save_dir, model_name_or_path)
