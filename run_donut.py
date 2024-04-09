import os
from pathlib import Path
import cv2
import json
import shutil

class DonutMetadataGenerator:
    def generate(self, data_dir, rst_dir ,files_list, split):
        base_img_dir_path = Path(data_dir).joinpath("images")
        img_dir_path = Path(data_dir).joinpath("images/")
        os.makedirs(Path(rst_dir).joinpath(f"{split}"), exist_ok=True)

        metadata_list = []

        for file_name in files_list:
            file_name_img = base_img_dir_path.joinpath(f"{file_name.stem}.jpg")
            try:
                with open(file_name, "r", encoding="latin1") as json_file:
                    data = json.load(json_file)
                    line = {"gt_parse": data}
                    text = json.dumps(line)
                    if img_dir_path.joinpath(f"{file_name.stem}.jpg").is_file():
                        shutil.copy2(file_name_img, Path(rst_dir).joinpath(f"{split}"))
                        metadata_list.append({
                            "ground_truth": text,
                            "file_name": f"{file_name.stem}.jpg"
                        })
            except json.JSONDecodeError:
                print(f"Skipping file {file_name} as it could not be loaded as JSON.")

        with open(Path(Path(rst_dir).joinpath(f"{split}")).joinpath("metadata.jsonl"), "w") as outfile:
            for entry in metadata_list:
                json.dump(entry, outfile)
                outfile.write("\n")

def main():
    # Convert to Donut format
    base_path = 'downloadfiles'
    rst_path = 'dataset'
    os.makedirs(rst_path, exist_ok=True)
    data_dir_path = Path(base_path).joinpath("json")
    print(data_dir_path)
    files = data_dir_path.glob("*.json")
    files_list = [file for file in files]
    # split files_list array into 3 parts, 80% train, 20% validation
    train_files_list = files_list[:int(len(files_list) * 0.80)]
    print("Train set size:", len(train_files_list))
    validation_files_list = files_list[int(len(files_list) * 0.80):int(len(files_list) * 1.0)]
    print("Validation set size:", len(validation_files_list))

    metadata_generator = DonutMetadataGenerator()
    metadata_generator.generate(base_path,rst_path,train_files_list, "train")
    metadata_generator.generate(base_path,rst_path,validation_files_list, "validation")

if __name__ == '__main__':
    main()
