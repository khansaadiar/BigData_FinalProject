from flask import Flask, request, jsonify, render_template
import pandas as pd
from joblib import load
import os
import logging
import threading
import time
from datetime import datetime
import random

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)

# Path to dataset and model
csv_file_path = "/Users/mac/Documents/ITS/SEM5/bigdata/fp/BigData_FinalProject/train_data.csv"
model_path = "/Users/mac/Documents/ITS/SEM5/bigdata/fp/BigData_FinalProject/models/shipping_fee_model.joblib"

# Load dataset once at the start
try:
    dataset = pd.read_csv(csv_file_path)
except Exception as e:
    logging.error(f"Could not load dataset: {e}")
    raise FileNotFoundError(f"Could not load dataset: {e}")

# Initialize global variable for the model
shipping_fee_model = None

def load_model():
    global shipping_fee_model
    while True:
        try:
            shipping_fee_model = load(model_path)
            logging.info("Model reloaded successfully.")
        except Exception as e:
            logging.error(f"Error loading model: {e}")
        time.sleep(300)  # Check for updates every 5 minutes

# Start the model loading thread
threading.Thread(target=load_model, daemon=True).start()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/input_data", methods=["GET"])
def input_data():
    try:
        # Randomly select a row
        random_row = dataset.sample(n=1).iloc[0]

        # Prepare data as a dictionary
        row_data = {
            "weight": int(random_row["weight"]),
            "item_price": float(random_row["item_price"]),
            "shipment_method_id": int(random_row["shipment_method_id"]),
            "quantity": int(random_row["quantity"]),
            "declared_handling_days": int(random_row["declared_handling_days"]),
            "zip_distance": float(random_row["zip_distance"]),
            "category_id": int(random_row["category_id"]),
        }

        return jsonify({"success": True, "data": row_data})
    except Exception as e:
        logging.error(f"Failed to fetch input data: {str(e)}")
        return jsonify({"success": False, "error": "Failed to fetch input data."})

@app.route("/predict", methods=["POST"])
def predict():
    if request.content_type != 'application/json':
        return jsonify({"success": False, "error": "Content-Type must be application/json."}), 415

    try:
        data = request.get_json()
        logging.info("Received data: %s", data)

        # Validate input data
        required_keys = [
            "weight", "item_price", "shipment_method_id", 
            "quantity", "declared_handling_days", 
            "zip_distance", "category_id"
        ]
        for key in required_keys:
            if key not in data:
                return jsonify({"success": False, "error": f"Missing key: {key}"}), 400

        # Convert data types
        weight = float(data["weight"])
        item_price = float(data["item_price"])
        shipment_method_id = int(data["shipment_method_id"])
        quantity = int(data["quantity"])
        declared_handling_days = int(data["declared_handling_days"])
        zip_distance = float(data["zip_distance"])
        category_id = int(data["category_id"])

        # Generate random values for fields that should not be zero
        shipping_fee = round(random.uniform(1.0, 100.0), 2)  # Placeholder for shipping fee
        carrier_min_estimate = random.randint(1, 7)
        carrier_max_estimate = random.randint(carrier_min_estimate + 1, carrier_min_estimate + 5)
        item_zip = random.randint(10000, 99999)
        buyer_zip = random.randint(10000, 99999)
        package_size = random.randint(1, 10)
        record_number = len(dataset) + 1  # Assuming this is the new record number

        # Prepare the data to match the CSV structure
        new_data = {
            "b2c_c2c": 0,  # You can change this to a more appropriate random value if needed
            "seller_id": random.randint(1, 10000000),  # Random seller ID
            "declared_handling_days": declared_handling_days,
            "acceptance_scan_timestamp": datetime.now().isoformat() + '-07:00',
            "shipment_method_id": shipment_method_id,
            "shipping_fee": shipping_fee,  # Placeholder, will be updated
            "carrier_min_estimate": carrier_min_estimate,
            "carrier_max_estimate": carrier_max_estimate,
            "item_zip": item_zip,
            "buyer_zip": buyer_zip,
            "category_id": category_id,
            "item_price": item_price,
            "quantity": quantity,
            "payment_datetime": datetime.now().isoformat() + '-07:00',
            "delivery_date": datetime.now().date(),
            "weight": weight,
            "package_size": package_size,
            "record_number": record_number,
            "carrier_average_estimate": round(random.uniform(1.0, 10.0), 2),
            "zip_distance": zip_distance,
            "acceptance_date": datetime.now().date(),
            "payment_date": datetime.now().date()
        }

        # Convert to DataFrame
        new_df = pd.DataFrame([new_data])

        # Append to the training dataset CSV
        new_df.to_csv(csv_file_path, mode='a', header=not os.path.exists(csv_file_path), index=False)

        # Prepare features for prediction
        features = pd.DataFrame([[weight, item_price, shipment_method_id,
                                   quantity, declared_handling_days,
                                   zip_distance, category_id]],
                                columns=["weight", "item_price", "shipment_method_id",
                                         "quantity", "declared_handling_days", "zip_distance", "category_id"])

        # Make prediction
        prediction = shipping_fee_model.predict(features)[0]
        prediction = float(prediction)

        # Update the shipping fee in the new data
        new_data["shipping_fee"] = round(prediction, 2)

        # Update the CSV with the shipping fee
        new_df.to_csv(csv_file_path, mode='a', header=False, index=False)

        return jsonify({"success": True, "shipping_fee": round(prediction, 2)})
    except Exception as e:
        logging.error(f"Prediction failed: {str(e)}")
        return jsonify({"success": False, "error": "Prediction failed."})

if __name__ == "__main__":
    app.run(debug=True)