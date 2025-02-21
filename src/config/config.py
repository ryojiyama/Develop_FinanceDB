# src/config/config.py
import os

DB_CONFIG = {
    'host': 'db',
    'database': 'financedb',
    'user': 'postgres',
    'password': os.getenv('DB_PASSWORD', 'your_password'),  # 環境変数から取得
    'port': 5432
}
