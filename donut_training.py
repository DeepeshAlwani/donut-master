import sys
import os
import json
import random
import re
import numpy as np
from typing import Any, List, Tuple
from ast import literal_eval
from pathlib import Path
from datasets import load_dataset
from nltk import edit_distance

import torch
from torch.utils.data import Dataset, DataLoader
from transformers import VisionEncoderDecoderConfig, DonutProcessor, VisionEncoderDecoderModel
import pytorch_lightning as pl
from pytorch_lightning.utilities import rank_zero_only
from pytorch_lightning.callbacks import Callback, EarlyStopping, ModelCheckpoint
from pytorch_lightning.loggers import TensorBoardLogger
from datetime import datetime

# Verify model name argument
if len(sys.argv) < 2:
    print("Please provide the model name you want to train: donut-invoice or donut-dwg")
    sys.exit(1)
model_name = sys.argv[1]
pretrained_model_path = model_name

# Load dataset
if not os.path.isdir("dataset"):
    print("Directory 'dataset' does not exist.")
    sys.exit(1)
dataset = load_dataset("dataset")
example = dataset['train'][0]
image = example['image']
width, height = image.size
ground_truth = example['ground_truth']
print(ground_truth)
literal_eval(ground_truth)['gt_parse']

# Configure model
image_size = [1275, 1650]
max_length = 768
config = VisionEncoderDecoderConfig.from_pretrained(pretrained_model_path)
config.encoder.image_size = image_size
config.decoder.max_length = max_length

processor = DonutProcessor.from_pretrained(pretrained_model_path)
model = VisionEncoderDecoderModel.from_pretrained(pretrained_model_path, config=config)
added_tokens = []

class DonutDataset(Dataset):
    def __init__(self, dataset_name_or_path: str, max_length: int, split: str = "train", ignore_id: int = -100,
                 task_start_token: str = "<s>", prompt_end_token: str = None, sort_json_key: bool = True):
        super().__init__()
        self.max_length = max_length
        self.split = split
        self.ignore_id = ignore_id
        self.task_start_token = task_start_token
        self.prompt_end_token = prompt_end_token if prompt_end_token else task_start_token
        self.sort_json_key = sort_json_key

        self.dataset = load_dataset(dataset_name_or_path, split=self.split)
        self.dataset_length = len(self.dataset)
        self.gt_token_sequences = []

        for sample in self.dataset:
            ground_truth = json.loads(sample["ground_truth"])
            gt_jsons = ground_truth["gt_parses"] if "gt_parses" in ground_truth else [ground_truth["gt_parse"]]
            self.gt_token_sequences.append([
                self.json2token(gt_json) + processor.tokenizer.eos_token for gt_json in gt_jsons
            ])

        self.add_tokens([self.task_start_token, self.prompt_end_token])
        self.prompt_end_token_id = processor.tokenizer.convert_tokens_to_ids(self.prompt_end_token)

    def json2token(self, obj: Any, update_special_tokens_for_json_key: bool = True, sort_json_key: bool = True):
        if isinstance(obj, dict):
            if len(obj) == 1 and "text_sequence" in obj:
                return obj["text_sequence"]
            keys = sorted(obj.keys(), reverse=True) if sort_json_key else obj.keys()
            output = "".join([
                f"<s_{k}>{self.json2token(obj[k])}</s_{k}>" for k in keys
            ])
            return output
        elif isinstance(obj, list):
            return "<sep/>".join([self.json2token(item) for item in obj])
        obj = str(obj)
        return f"<{obj}/>" if f"<{obj}/>" in added_tokens else obj

    def add_tokens(self, list_of_tokens: List[str]):
        newly_added_num = processor.tokenizer.add_tokens(list_of_tokens)
        if newly_added_num > 0:
            model.decoder.resize_token_embeddings(len(processor.tokenizer))
            added_tokens.extend(list_of_tokens)

    def __len__(self) -> int:
        return self.dataset_length

    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        sample = self.dataset[idx]
        pixel_values = processor(sample["image"], random_padding=self.split == "train", return_tensors="pt").pixel_values.squeeze()
        target_sequence = random.choice(self.gt_token_sequences[idx])
        input_ids = processor.tokenizer(target_sequence, add_special_tokens=False, max_length=self.max_length,
                                        padding="max_length", truncation=True, return_tensors="pt")["input_ids"].squeeze(0)
        labels = input_ids.clone()
        labels[labels == processor.tokenizer.pad_token_id] = self.ignore_id
        return pixel_values, labels, target_sequence

processor.image_processor.size = image_size[::-1]
processor.image_processor.do_align_long_axis = False

train_dataset = DonutDataset("dataset", max_length=max_length, split="train",
                             task_start_token="<s_cord-v2>", prompt_end_token="<s_cord-v2>", sort_json_key=False)
val_dataset = DonutDataset("dataset", max_length=max_length, split="validation",
                           task_start_token="<s_cord-v2>", prompt_end_token="<s_cord-v2>", sort_json_key=False)

print(added_tokens)
print("Original number of tokens:", processor.tokenizer.vocab_size)
print("Number of tokens after adding special tokens:", len(processor.tokenizer))
processor.decode([57521])

# Print sample data
pixel_values, labels, target_sequence = train_dataset[0]
print(pixel_values.shape)
for id in labels.tolist()[:30]:
    print(processor.decode([id]) if id != -100 else id)
print(target_sequence)

model.config.pad_token_id = processor.tokenizer.pad_token_id
model.config.decoder_start_token_id = processor.tokenizer.convert_tokens_to_ids(['<s_cord-v2>'])[0]
print("Pad token ID:", processor.decode([model.config.pad_token_id]))
print("Decoder start token ID:", processor.decode([model.config.decoder_start_token_id]))

# Dataloaders
train_dataloader = DataLoader(train_dataset, batch_size=1, shuffle=True, num_workers=0)
val_dataloader = DataLoader(val_dataset, batch_size=1, shuffle=False, num_workers=0)
print(train_dataloader)
batch = next(iter(train_dataloader))
pixel_values, labels, target_sequences = batch
print(pixel_values.shape)
for id in labels.squeeze().tolist()[:30]:
    print(processor.decode([id]) if id != -100 else id)
print(len(train_dataset))
print(len(val_dataset))
batch = next(iter(val_dataloader))
pixel_values, labels, target_sequences = batch
print(pixel_values.shape)
print(target_sequences[0])

class DonutModelPLModule(pl.LightningModule):
    def __init__(self, config, processor, model):
        super().__init__()
        self.config = config
        self.processor = processor
        self.model = model

    def training_step(self, batch, batch_idx):
        pixel_values, labels, _ = batch
        outputs = self.model(pixel_values, labels=labels)
        loss = outputs.loss
        self.log("train_loss", loss)
        return loss

    def validation_step(self, batch, batch_idx, dataset_idx=0):
        pixel_values, labels, answers = batch
        batch_size = pixel_values.shape[0]
        decoder_input_ids = torch.full((batch_size, 1), self.model.config.decoder_start_token_id, device=self.device)
        outputs = self.model.generate(pixel_values, decoder_input_ids=decoder_input_ids, max_length=max_length,
                                      early_stopping=True, pad_token_id=self.processor.tokenizer.pad_token_id,
                                      eos_token_id=self.processor.tokenizer.eos_token_id, use_cache=True, num_beams=1,
                                      bad_words_ids=[[self.processor.tokenizer.unk_token_id]], return_dict_in_generate=True)
        predictions = [
            re.sub(r"<.*?>", "", seq.replace(self.processor.tokenizer.eos_token, "").replace(self.processor.tokenizer.pad_token, ""), count=1).strip()
            for seq in self.processor.tokenizer.batch_decode(outputs.sequences)
        ]
        scores = [
            edit_distance(pred, answer) / max(len(pred), len(answer))
            for pred, answer in zip(predictions, answers)
        ]
        if self.config.get("verbose", False) and scores:
            print(f"Prediction: {predictions[0]}")
            print(f"    Answer: {answers[0]}")
            print(f" Normed ED: {scores[0]}")
        self.log("val_edit_distance", np.mean(scores))
        return scores

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.config.get("lr"))

    def train_dataloader(self):
        return train_dataloader

    def val_dataloader(self):
        return val_dataloader

config = {
    "max_epochs": 8,
    "val_check_interval": 0.5,
    "check_val_every_n_epoch": 1,
    "gradient_clip_val": 1.0,
    "num_training_samples_per_epoch": 40,
    "lr": 5e-5,
    "train_batch_sizes": [1],
    "val_batch_sizes": [1],
    "num_nodes": 1,
    "warmup_steps": 12,
    "result_path": "home/results",
    "verbose": True,
}

model_module = DonutModelPLModule(config, processor, model)
tensorboard_logger = TensorBoardLogger("finetune_logs", name="donut-cord-v2")
early_stop_callback = EarlyStopping(monitor="val_edit_distance", patience=2, verbose=True, mode="min")

current_date = datetime.now()
formatted_date = current_date.strftime("%d-%b-%Y")

checkpoint_callback = ModelCheckpoint(
    monitor="val_edit_distance",
    dirpath="saved_model",
    filename=f"{model_name}-{formatted_date}",
    save_top_k=3,
    mode="min",
)


trainer = pl.Trainer(
    accelerator="gpu", devices=1,
    precision=16,
    max_epochs=config["max_epochs"],
    val_check_interval=config["val_check_interval"],
    check_val_every_n_epoch=config["check_val_every_n_epoch"],
    gradient_clip_val=config["gradient_clip_val"],
    limit_train_batches=config["num_training_samples_per_epoch"],
    num_nodes=config["num_nodes"],
    logger=tensorboard_logger,
    callbacks=[early_stop_callback, checkpoint_callback]
)
trainer.fit(model_module)
