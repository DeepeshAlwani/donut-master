import torch
from donut import DonutDataset, DonutModelPLModule
from sconf import Config

def load_model_and_config(model_path, config_path):
    # Load the model and configuration
    model = DonutModelPLModule.load_from_checkpoint(model_path)
    config = Config(config_path)
    
    return model, config

def predict_and_display_json(model, config, prediction_dataset_path):
    # Create a DonutDataset for prediction
    dataset = DonutDataset(
        dataset_name_or_path=prediction_dataset_path,
        donut_model=model.model,
        max_length=config.max_length,
        split="prediction",
        task_start_token=config.task_start_tokens[0],
        prompt_end_token="<s_answer>",
        sort_json_key=config.sort_json_key,
    )

    # Set the model to evaluation mode
    model.eval()

    # Create a DataLoader for the dataset
    dataloader = torch.utils.data.DataLoader(dataset, batch_size=1, shuffle=False)

    # Iterate through the data
    for data in dataloader:
        input_ids = data["input_ids"]
        attention_mask = data["attention_mask"]

        # Perform inference
        with torch.no_grad():
            output = model(input_ids=input_ids, attention_mask=attention_mask)

        # Process the model's output as needed for predictions
        # For example, you can convert the output to JSON and print it
        json_output = output.to_json()
        print(json_output)

if __name__ == "__main__":
    # Paths to the trained model checkpoint and configuration file
    model_path = r"E:\model_trained_on_SOROIE\model_checkpoint.ckpt"
    config_path = r"E:\model_trained_on_SOROIE\config.yaml"

    # Path to the prediction dataset
    prediction_dataset_path = r"D:\drive-download-20231103T115214Z-001\validation"

    model, config = load_model_and_config(model_path, config_path)
    predict_and_display_json(model, config, prediction_dataset_path)
