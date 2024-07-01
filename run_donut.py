import os
from pathlib import Path
import json
import shutil

class DonutMetadataGenerator:
    def generate(self, data_dir, rst_dir, files_list, split):
        base_img_dir_path = Path(data_dir).joinpath("images")
        os.makedirs(Path(rst_dir).joinpath(f"{split}"), exist_ok=True)

        metadata_list = []

        for file_name in files_list:
            # Check for both .jpeg and .jpg extensions
            file_name_img_jpeg = base_img_dir_path.joinpath(f"{file_name.stem}.jpeg")
            file_name_img_jpg = base_img_dir_path.joinpath(f"{file_name.stem}.jpg")

            print(f"Processing JSON: {file_name}")
            print(f"Looking for image: {file_name_img_jpeg} or {file_name_img_jpg}")

            if file_name_img_jpeg.is_file():
                print(f"Found image: {file_name_img_jpeg}")
                image_file = file_name_img_jpeg
            elif file_name_img_jpg.is_file():
                print(f"Found image: {file_name_img_jpg}")
                image_file = file_name_img_jpg
            else:
                print(f"Image file for {file_name.stem} does not exist. Skipping.")
                continue

            try:
                with open(file_name, "r", encoding="latin1") as json_file:
                    data = json.load(json_file)
                    line = {"gt_parse": data}
                    text = json.dumps(line)
                    shutil.copy2(image_file, Path(rst_dir).joinpath(f"{split}"))
                    metadata_list.append({
                        "ground_truth": text,
                        "file_name": image_file.name
                    })
            except json.JSONDecodeError:
                print(f"Skipping file {file_name} as it could not be loaded as JSON.")
            except Exception as e:
                print(f"An error occurred while processing {file_name}: {e}")

        metadata_file_path = Path(rst_dir).joinpath(f"{split}", "metadata.jsonl")
        with open(metadata_file_path, "w") as outfile:
            for entry in metadata_list:
                json.dump(entry, outfile)
                outfile.write("\n")
        
        print(f"Generated {metadata_file_path} with {len(metadata_list)} entries.")

def main():
    # Convert to Donut format
    base_path = 'downloadfiles'
    rst_path = 'dataset'
    os.makedirs(rst_path, exist_ok=True)
    data_dir_path = Path(base_path).joinpath("json")
    print(f"Data directory: {data_dir_path}")
    
    files = list(data_dir_path.glob("*.json"))
    files_list = [file for file in files]
    
    # Split files_list array into 2 parts, 80% train, 20% validation
    split_index = int(len(files_list) * 0.80)
    train_files_list = files_list[:split_index]
    print("Train set size:", len(train_files_list))
    validation_files_list = files_list[split_index:]
    print("Validation set size:", len(validation_files_list))

    metadata_generator = DonutMetadataGenerator()
    metadata_generator.generate(base_path, rst_path, train_files_list, "train")
    metadata_generator.generate(base_path, rst_path, validation_files_list, "validation")

if __name__ == '__main__':
    main()
