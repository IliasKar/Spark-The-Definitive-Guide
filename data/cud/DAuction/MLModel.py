import os
import json
import torch
import pandas as pd
import boto3
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from tqdm.notebook import tqdm 

class MLModel:
    def __init__(self, tokenizer_path, model_path, initiators_path, training_dataset_path):
        # Load the saved tokenizer and model
        self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_path)

        # Device configuration
        if torch.cuda.is_available():
            self.device = torch.device("cuda")
            print("Using GPU:", torch.cuda.get_device_name(0))
        else:
            self.device = torch.device("cpu")
            print("Using CPU")
        self.model = self.model.to(self.device)

        # Load the initiators mapping
        with open(initiators_path, 'r') as file:
            self.labels_dict = json.load(file)
        self.labels_dict_inverse = {value: key for key, value in self.labels_dict.items()}

        self.verified_df = pd.read_excel(training_dataset_path)
        # Load verified initiators dictionary
        self.verified_initiators_dict = dict(zip(
        self.verified_df.loc[self.verified_df["Approved"] == "yes"]["HastenerDescr"].to_list(), 
        self.verified_df.loc[self.verified_df["Approved"] == "yes"]["Initiator"].to_list()
        ))

    def predict(self, text_instance):
        if text_instance in self.verified_initiators_dict:
            # Found in verified initiators, return this directly
            return self.verified_initiators_dict[text_instance]
        else:
            # Prepare the text instance for the model
            inputs = self.tokenizer(text_instance, return_tensors="pt", padding=True, truncation=True, max_length=256)
            inputs = inputs.to(self.device)

            # Predict using the model
            with torch.no_grad():
                outputs = self.model(**inputs)
            
            logits = outputs.logits
            probabilities = torch.nn.functional.softmax(logits, dim=-1)
            predicted_class = torch.argmax(probabilities, axis=-1).cpu().numpy()
            predicted_class = int(predicted_class)
            
            # Get the predicted label
            predicted_text = self.labels_dict_inverse[predicted_class]

            return predicted_text
