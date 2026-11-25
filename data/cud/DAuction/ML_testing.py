# Databricks notebook source
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

# Adjust paths for pre-release mode
if pre_release == "yes":
    # Paths adjusted for pre-release mode
    tokenizer_path = f"{train_directory}/pre_release/trained_model/trained_bert_tokenizer/"
    model_path = f"{train_directory}/pre_release/trained_model/trained_bert/"
    initiators_path = f"{train_directory}/pre_release/initiators.json"
    training_dataset_path = f"{train_directory}/pre_release/HastenerTrainData.xlsx"
    initiators_mappings_path = f"{train_directory}/pre_release/Initiator_Mapping.xlsx"


# COMMAND ----------

# DBTITLE 1,Import Data

df_approved = pd.read_excel(training_dataset_path, index_col=0)

df_approved_train = df_approved.loc[
    (df_approved["Approved"] == "yes" ) & 
    (df_approved["marked_for_testing"] == "no" )
]

df_approved_test = df_approved.loc[
    (df_approved["Approved"] == "yes" ) & 
    (df_approved["marked_for_testing"] == "yes" )
]

# Reading and processing the JSON file for labels
with open(initiators_path, 'r') as file:
    labels_dict = json.load(file)
    
labels_dict_inverse = {value: key for key, value in labels_dict.items()}


# COMMAND ----------

# DBTITLE 1,Import Trained Model
# Load the saved model and tokenizer
tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)
model = AutoModelForSequenceClassification.from_pretrained(model_path)

# Device configuration
if torch.cuda.is_available():
    device = torch.device("cuda")
    print("Using GPU:", torch.cuda.get_device_name(0))
else:
    device = torch.device("cpu")
    print("Using CPU")

model = model.to(device)

# Reading and processing the JSON file for labels
with open(initiators_path, 'r') as file:
    labels_dict = json.load(file)
    
labels_dict_inverse = {value: key for key, value in labels_dict.items()}

def predict_text(text_instance, tokenizer=tokenizer, model=model, labels_dict_inverse=labels_dict_inverse):
    """
    Predict the class of a given text instance using the loaded model and tokenizer.

    Args:
        text_instance (str): The text instance to predict.
        tokenizer: The tokenizer to use for encoding text.
        model: The pre-trained model for prediction.
        labels_dict_inverse: A dictionary to map prediction indices to labels.

    Returns:
        str: Predicted class label for the given text instance.
    """
    inputs = tokenizer(text_instance, return_tensors="pt", padding=True, truncation=True, max_length=256).to(device)

    with torch.no_grad():
        outputs = model(**inputs)

    logits = outputs.logits
    probabilities = torch.nn.functional.softmax(logits, dim=-1)
    predicted_class = torch.argmax(probabilities, axis=-1).cpu().numpy()
    predicted_class = int(predicted_class)

    return labels_dict_inverse[predicted_class]



# COMMAND ----------

# DBTITLE 1,Classification Report
from sklearn.metrics import precision_recall_fscore_support, accuracy_score, classification_report

unique_hastener_descr = df_approved_test["HastenerDescr"].to_list()
predicted_initiators = [ predict_text(uhd) for uhd in unique_hastener_descr ] 

report = classification_report(df_approved_test["Initiator"].to_list(),predicted_initiators, target_names=list(labels_dict.keys()))
print(report)

validation_set = df_approved_test.copy()
validation_set["Predicted_Initiator"] = predicted_initiators

# COMMAND ----------

# DBTITLE 1,Plot Class Distribution
import matplotlib.pyplot as plt
import numpy as np

# Computing value counts
initiator_counts = validation_set['Initiator'].value_counts().sort_index()
nn_predicted_counts = validation_set['Predicted_Initiator'].value_counts().sort_index()

# Aligning the indices of both series (in case they differ)
all_categories = initiator_counts.index.union(nn_predicted_counts.index)
initiator_counts = initiator_counts.reindex(all_categories, fill_value=0)
nn_predicted_counts = nn_predicted_counts.reindex(all_categories, fill_value=0)

# Setting up the x-axis positions for the bars
n_categories = len(all_categories)
bar_width = 0.35  # Width of the bars
index = np.arange(n_categories)

# Plotting
plt.figure(figsize=(25, 5))

plt.bar(index, initiator_counts, bar_width, label='Initiator')
plt.bar(index + bar_width, nn_predicted_counts, bar_width, label='Predicted_Initiator')

# Customizing the plot
plt.xlabel('Category')
plt.ylabel('Frequency')
plt.title('Frequency of Each Category')
plt.xticks(index + bar_width / 2, all_categories, rotation='vertical')  # Rotate labels vertically
plt.legend()

plt.show()

