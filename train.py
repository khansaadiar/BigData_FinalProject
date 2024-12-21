import boto3
import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, r2_score
import joblib
import os
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('model_training.log'),
        logging.StreamHandler()
    ]
)

# Initialize MinIO client
minio_client = boto3.client('s3',
                             endpoint_url='http://localhost:9000',
                             aws_access_key_id='minioadmin',
                             aws_secret_access_key='minioadmin')

# Bucket name
bucket_name = 'ebay'

def save_model(model):
    joblib.dump(model, os.path.join("models", "shipping_fee_model.joblib"))

def save_evaluation_results(file_key, mae_fee, rmse_fee, r2_fee):
    results = {
        "file_key": file_key,
        "mae_fee": mae_fee,
        "rmse_fee": rmse_fee,
        "r2_fee": r2_fee,
    }

    results_df = pd.DataFrame([results])
    if not os.path.exists("evaluation_results.csv"):
        results_df.to_csv("evaluation_results.csv", index=False)
    else:
        results_df.to_csv("evaluation_results.csv", mode='a', header=False, index=False)

def train_model(data):
    try:
        logging.info("Starting model training...")
        df = data.copy()

        # Assign column names
        df.columns = [
            'b2c_c2c', 'seller_id', 'declared_handling_days',
            'shipment_method_id', 'item_zip', 'buyer_zip',
            'category_id', 'item_price', 'quantity', 'weight',
            'package_size', 'zip_distance', 'shipping_fee',
            'delivery_date', 'acceptance_date', 'timestamp'
        ]

        # Select relevant columns
        selected_columns = [
            "weight", "item_price", "shipment_method_id", 
            "quantity", "declared_handling_days", 
            "zip_distance", "category_id", "shipping_fee"
        ]

        selected_columns = [col for col in selected_columns if col in df.columns]
        df = df[selected_columns]

        # Separate features and target
        X = df.drop(["shipping_fee"], axis=1)
        y_fee = df["shipping_fee"]

        # Split the data
        X_train, X_test, y_train_fee, y_test_fee = train_test_split(X, y_fee, test_size=0.2, random_state=42)

        logging.info("Training model for shipping_fee...")
        
        # Use XGBoost Regressor
        model_fee = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
        model_fee.fit(X_train, y_train_fee)

        # Make predictions
        y_pred_fee = model_fee.predict(X_test)

        # Evaluate the model
        mae_fee = mean_absolute_error(y_test_fee, y_pred_fee)
        rmse_fee = ((y_test_fee - y_pred_fee) ** 2).mean() ** 0.5
        r2_fee = r2_score(y_test_fee, y_pred_fee)

        logging.info("Model for shipping_fee has been trained.")

        # Save the model with a fixed name to replace the previous one
        save_model(model_fee)

        # Save evaluation results
        save_evaluation_results("combined_data.csv", mae_fee, rmse_fee, r2_fee)

        # Print evaluation results
        logging.info(f"Evaluation Results: Shipping Fee - MAE: {mae_fee:.2f}, RMSE: {rmse_fee:.2f}, R²: {r2_fee:.2f}")

    except Exception as e:
        logging.error(f"An error occurred during model training: {str(e)}")

def load_previous_data():
    all_data = []
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join('data', file))
            all_data.append(df)
    return pd.concat(all_data, ignore_index=True) if all_data else None

def auto_train_models():
    processed_files = {}
    combined_data = pd.DataFrame()  # Initialize an empty DataFrame

    while True:
        try:
            logging.info("Checking for new files in MinIO...")
            response = minio_client.list_objects_v2(Bucket=bucket_name)

            files = []
            for obj in response.get('Contents', []):
                if 'batch_' in obj['Key']:
                    files.append(obj['Key'])

            logging.info(f"Objects in bucket: {files}")

            for batch_key in files:
                # Check the size of the file first
                file_size = minio_client.head_object(Bucket=bucket_name, Key=batch_key)['ContentLength']
                if file_size == 0:
                    logging.warning(f"The file {batch_key} is empty. Skipping.")
                    continue

                response = minio_client.get_object(Bucket=bucket_name, Key=batch_key)
                batch_df = pd.read_csv(response['Body'], header=None)  # Load without headers

                # Assign column names
                batch_df.columns = [
                    'b2c_c2c', 'seller_id', 'declared_handling_days',
                    'shipment_method_id', 'item_zip', 'buyer_zip',
                    'category_id', 'item_price', 'quantity', 'weight',
                    'package_size', 'zip_distance', 'shipping_fee',
                    'delivery_date', 'acceptance_date', 'timestamp'
                ]

                logging.info(f"Columns in {batch_key}: {batch_df.columns.tolist()}")

                # Check if the DataFrame is empty after loading
                if batch_df.empty:
                    logging.warning(f"The file {batch_key} is empty after loading. Skipping.")
                    continue

                current_batch_size = len(batch_df)

                # Check if this batch has been processed before
                if batch_key in processed_files:
                    if current_batch_size <= processed_files[batch_key]:
                        logging.info(f"No new data in batch: {batch_key}. Skipping.")
                        continue

                # Append the current batch data to combined_data
                combined_data = pd.concat([combined_data, batch_df], ignore_index=True)
                processed_files[batch_key] = current_batch_size  # Update processed size

            # Check if there is data to train on
            if not combined_data.empty:
                logging.info("Training model on combined data...")
                train_model(combined_data)
                combined_data = pd.DataFrame()  # Reset combined data after training

            # If no new data found, use previous data
            if not files:
                logging.info("No new files to process. Using previous data if available...")
                previous_data = load_previous_data()
                if previous_data is not None:
                    train_model(previous_data)
                else:
                    logging.info("No previous data available.")

            logging.info("Waiting to check for new files...")
            time.sleep(300)

        except Exception as e:
            logging.error(f"An error occurred while checking files: {str(e)}")
            time.sleep(300)

if __name__ == "__main__":
    os.makedirs("models", exist_ok=True)
    os.makedirs("data", exist_ok=True)  # Ensure the data folder exists
    auto_train_models()