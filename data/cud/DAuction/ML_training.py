# Databricks notebook source
# DBTITLE 1,Clear GPU VRAM
from numba import cuda
device = cuda.get_current_device()
device.reset()

import os
os.environ["CUDA_LAUNCH_BLOCKING"] = "1"

# Set the environment variable for MLflow
os.environ['MLFLOW_FLATTEN_PARAMS'] = '1'

# COMMAND ----------

# DBTITLE 1,Environment Vars
import os
import json
import torch
import pandas as pd
import boto3
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# AWS boto3 client initialization for accessing SSM parameters
ssm_client = boto3.client("ssm", region_name="eu-central-1")

def connect_to_aws_and_access_ssm(ssm_key):
    """
    Connect to AWS SSM to retrieve a parameter value.

    Args:
        ssm_key (str): The key for the SSM parameter to retrieve.

    Returns:
        str: The value of the SSM parameter.
    """
    if ssm_key is None:
        print("ERROR: ssm key must be supplied.")
        return

    response = ssm_client.get_parameter(Name=ssm_key, WithDecryption=False)
    return response['Parameter']['Value']

# Retrieve environment variables from AWS SSM
env_prefix = connect_to_aws_and_access_ssm('/cfn/bucket_prefix')
pre_release = connect_to_aws_and_access_ssm('/cfn/a360/matching/pre_release')
train_directory = f"/Volumes/{env_prefix}-cdh/derivative/DAuction_ML"


# Paths for model and data
tokenizer_path = f"{train_directory}/trained_model/trained_bert_tokenizer/"
model_path = f"{train_directory}/trained_model/trained_bert/"
initiators_path = f"{train_directory}/initiators.json"
initiators_mappings_path = f"{train_directory}/Initiator_Mapping.xlsx"
training_dataset_path = f"{train_directory}/HastenerTrainData.xlsx"

pre_release == "yes"

# Adjust paths for pre-release mode
if pre_release == "yes":
    # Paths adjusted for pre-release mode
    tokenizer_path = f"{train_directory}/pre_release/trained_model/trained_bert_tokenizer/"
    model_path = f"{train_directory}/pre_release/trained_model/trained_bert/"
    initiators_path = f"{train_directory}/pre_release/initiators.json"
    training_dataset_path = f"{train_directory}/pre_release/HastenerTrainData.xlsx"
    initiators_mappings_path = f"{train_directory}/pre_release/Initiator_Mapping.xlsx"

# COMMAND ----------

# DBTITLE 1,Import Data - Initiators file
df_approved = pd.read_excel(training_dataset_path, index_col=0)

df_approved_train = df_approved.loc[
    (df_approved["Approved"] == "yes" ) & 
    (df_approved["marked_for_testing"] == "no" )
]

df_approved_test = df_approved.loc[
    (df_approved["Approved"] == "yes" ) & 
    (df_approved["marked_for_testing"] == "yes" )
]

#Create the json dict for int labels for initiators
labels_dict = dict(zip(
sorted(df_approved["Initiator"].unique().tolist()),
[i for i in range(len(sorted(df_approved["Initiator"].unique().tolist())))]
)) 

with open(initiators_path , 'w') as json_file:
    json.dump(labels_dict, json_file)


# Inverting the dictionary
labels_dict_inverse = {value: key for key, value in labels_dict.items()}

# COMMAND ----------

# DBTITLE 1,Mark Training Dataset
#Also add the class names as training data
hast_data = list(labels_dict.keys())      
initiators = list(labels_dict.keys()) 
appr = ["yes" for _ in range(len(list(labels_dict.keys()) ))]
tmp_df = pd.DataFrame()
tmp_df["HastenerDescr"] = hast_data
tmp_df["Initiator"] = initiators
tmp_df["Approved"] = appr
tmp_df["marked_for_testing"] = ["no" for _ in range(len(tmp_df))]
tmp_df["version"] = [df_approved_train["version"].iloc[0]  for _ in range(len(tmp_df)) ]

df_approved = pd.concat([tmp_df, df_approved_train])

label_col = "Initiator"
text_col = "HastenerDescr"

def build_int_labels(data):

    df = data.copy()

    df["label"] = df[label_col].map(labels_dict)
    df["label"] = df["label"].astype(int)

    df = df[["HastenerDescr", "label"]]

    return df

df = build_int_labels(df_approved)
out_of_sample = build_int_labels(df_approved_test)

# COMMAND ----------

# DBTITLE 1,TestTrain Split
def build_test_train_data_max_instances(df, 
                        label_column='label', 
                        text_column='HastenerDescr', 
                        min_train_percentage=70, 
                        max_instances=30):

    grouped = df.groupby(label_column)

    train_df = pd.DataFrame()
    test_df = pd.DataFrame()

    for label, group in grouped:
        num_instances = len(group)
        
        if num_instances == 1:
            train_df = pd.concat([group, train_df])
            test_df = pd.concat([group, test_df])   
        else:
            num_instances = min(num_instances, max_instances)
            min_instances = max(1, int(num_instances * min_train_percentage / 100))
            test_instnaces = num_instances - min_instances
            tmp_train_df = group.sample(n=min_instances)
            tmp_test_df = group.sample(n=test_instnaces)
            train_df = pd.concat([tmp_train_df, train_df])
            test_df = pd.concat([tmp_test_df, test_df])    
    
    return train_df, test_df

def build_test_train_data(df, label_column='label', 
                          text_column='HastenerDescr', min_train_percentage=80):
    grouped = df.groupby(label_column)

    train_df = pd.DataFrame()
    test_df = pd.DataFrame()

    for label, group in grouped:
        num_instances = len(group)
        
        if num_instances == 1:
            # If there's only one instance, add it to both train and test datasets
            train_df = pd.concat([group, train_df])
            test_df = pd.concat([group, test_df])
        else:
            # Calculate the number of instances for the train and test datasets
            min_instances = max(1, int(num_instances * min_train_percentage / 100))
            test_instances = num_instances - min_instances

            # Randomly sample instances for train and test datasets
            tmp_train_df = group.sample(n=min_instances)
            tmp_test_df = group.drop(tmp_train_df.index)  # Ensure no overlap between train and test sets

            # Append to the main train and test dataframes
            train_df = pd.concat([tmp_train_df, train_df])
            test_df = pd.concat([tmp_test_df, test_df])    
    
    return test_df, train_df


def build_total_training_data(data, 
                        label_column='label', 
                        text_column='HastenerDescr'):
    df = data.copy()
    grouped = df.groupby(label_column)

    train_df = pd.DataFrame()
    test_df = pd.DataFrame()
    single_instances = pd.DataFrame()
    test_train_list = []

    for label, group in grouped:
        num_instances = len(group)
        if num_instances == 1:
            single_instances = pd.concat([group, single_instances])
        else:
            split_num = round(num_instances / 2)
            tmp_train_df = group.sample(n=split_num)
            tmp_test_df = group.sample(n=split_num)
            train_df = pd.concat([tmp_train_df, train_df])
            test_df = pd.concat([tmp_test_df, test_df])  

    train_df = pd.concat([single_instances, train_df])
    test_df = pd.concat([single_instances, test_df])  

    return test_df, train_df  


# COMMAND ----------

# DBTITLE 1,Download BERT
from transformers import AutoTokenizer, AutoModelForSequenceClassification, \
    TrainingArguments, Trainer
from torch.utils.data import Dataset
import torch

# Load tokenizer and model
model_type =  "bert-base-multilingual-cased"
tokenizer = AutoTokenizer.from_pretrained(model_type)
model = AutoModelForSequenceClassification.from_pretrained(model_type, 
                                                           num_labels=len(labels_dict))

# Function to tokenize batch of texts
def tokenize(batch):
    return tokenizer(batch, padding='max_length', truncation=True, max_length=256)

# Create PyTorch datasets
class TextDataset(Dataset):
    def __init__(self, encodings, labels):
        self.encodings = encodings
        self.labels = labels

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['labels'] = torch.tensor(self.labels[idx])
        return item

    def __len__(self):
        return len(self.labels)

def prepare_datasets(test_df, train_df, 
                     text_col="HastenerDescr", 
                     label_col="label"):

    train_encodings = tokenize(train_df[text_col].tolist())
    test_encodings = tokenize(test_df[text_col].tolist())

    train_dataset = TextDataset(train_encodings, train_df[label_col].tolist())
    test_dataset = TextDataset(test_encodings, test_df[label_col].tolist())

    return test_dataset, train_dataset

# COMMAND ----------

# DBTITLE 1,Setup Training
test_df, train_df = build_total_training_data(df)

data_list = [
    [test_df, train_df], 
    [train_df, test_df]
]

for i, d_list in enumerate(data_list):

    num_epochs = 15 - (i*5)

    # Define Trainer
    training_args = TrainingArguments(
        output_dir='/results',
        num_train_epochs=num_epochs,
        per_device_train_batch_size=16,
        per_device_eval_batch_size=64,
        warmup_steps=500,
        weight_decay=0.01,
        logging_dir='/logs',
        logging_steps=100,
    )

    test_df = d_list[0]
    train_df = d_list[1]
    test_dataset, train_dataset = prepare_datasets(test_df, train_df)

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=test_dataset
    )

    # Train model
    trainer.train()

    # Evaluate model
    results = trainer.evaluate()
    print(results)

# COMMAND ----------

# DBTITLE 1,Classification Report
from sklearn.metrics import precision_recall_fscore_support, accuracy_score, classification_report

# Predict on the test dataset
predictions = trainer.predict(test_dataset)

# Convert logits to probabilities and then to class predictions
probabilities = torch.nn.functional.softmax(torch.from_numpy(predictions.predictions), dim=-1)
predicted_classes = torch.argmax(probabilities, axis=-1).numpy()

# True labels
true_labels = test_df['label'].tolist()

# Calculate Accuracy
accuracy = accuracy_score(true_labels, predicted_classes)
print(f'Accuracy: {accuracy}')

# Calculate Precision, Recall, F1 Score
precision, recall, f1, _ = precision_recall_fscore_support(true_labels, predicted_classes, average='weighted')
print(f'Precision: {precision}')
print(f'Recall: {recall}')
print(f'F1 Score: {f1}')

report = classification_report(true_labels, predicted_classes, target_names=[f'Class {i}' for i in range(len(labels_dict))])
print(report)

# COMMAND ----------

# DBTITLE 1,Save Model
import os
import shutil

if os.path.exists(model_path):
    shutil.rmtree(model_path)
if os.path.exists(tokenizer_path):
    shutil.rmtree(tokenizer_path)

os.makedirs(model_path, exist_ok=True)
os.makedirs(tokenizer_path, exist_ok=True)

model.save_pretrained(model_path)
tokenizer.save_pretrained(tokenizer_path)
