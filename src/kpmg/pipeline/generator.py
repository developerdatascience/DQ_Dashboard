import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime

fake = Faker()

# Set random seed for reproducibility
np.random.seed(42)

class DataGeneratorPipeline:
    def __init__(self, num_rows: int = 20000):
        self.num_rows = num_rows

    
    def generate_dataset(self):
        # Generate synthetic credit card fraud dataset
        data = {
            "TransactionID": np.arange(1, self.num_rows + 1),
            "UserID": np.random.randint(1000, 5000, size=self.num_rows),
            "TransactionAmount": np.round(np.random.uniform(5, 5000, size=self.num_rows), 2),
            "TransactionDate": pd.date_range(start="2024-01-01", periods=self.num_rows, freq="T"),
            "TransactionType": np.random.choice(["Online", "POS", "ATM Withdrawal"], size=self.num_rows),
            "Merchant": [fake.company() for _ in range(self.num_rows)],
            "Location": [fake.city() for _ in range(self.num_rows)],
            "CardType": np.random.choice(["Visa", "MasterCard", "Amex", "Discover"], size=self.num_rows),
            "IsFraud": np.random.choice([0, 1], size=self.num_rows, p=[0.98, 0.02]),  # 2% fraud cases
        }
        df = pd.DataFrame(data)

        # Introduce missing values randomly
        for col in ["TransactionAmount", "TransactionType", "Merchant"]:
            df.loc[df.sample(frac=0.02).index, col] = np.nan

        # Introduce duplicate rows
        df = pd.concat([df, df.sample(frac=0.02)], ignore_index=True)

        # Introduce inconsistent formats in the "Location" column
        df.loc[df.sample(frac=0.01).index, "Location"] = df["Location"].apply(lambda x: x.lower())

        current_date = datetime.now().strftime("%Y%m%d")

        filename = f"{current_date}.csv"

        # Save to CSV
        file_path = f"inputs/finance/{filename}"
        df.to_csv(file_path, index=False)
