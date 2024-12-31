import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import os
import shutil
import logging

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('card_import.log'),
        logging.StreamHandler()
    ]
)

class CardDataImporter:
    def __init__(self):
        # データベース接続情報
        self.db_params = {
            'dbname': 'financedb',
            'user': 'postgres',
            'password': 'password',
            'host': 'db',
            'port': '5432'
        }

        # パス設定
        self.input_dir = os.path.expanduser('/Users/ryoji/Library/CloudStorage/Dropbox/ZapDowonloads/data_fncprc')
        self.processed_dir = os.path.expanduser('/Users/ryoji/Develop_FinanceDB/data/processed')

    def connect_db(self):
        """データベースへの接続を確立"""
        return psycopg2.connect(**self.db_params)

    def preprocess_data(self, df):
        """データの前処理を行う"""
        # 日付形式の統一
        df['transaction_date'] = pd.to_datetime(df['年月日']).dt.strftime('%Y-%m-%d')

        # 金額の正規化（カンマ除去と数値変換）
        df['withdrawal'] = pd.to_numeric(
            df['金額'].str.replace(',', '').str.replace('¥', ''),
            errors='coerce'
        )

        # 説明文の正規化
        df['description'] = df['内容'].str.strip()

        # メモフィールドの処理
        df['memo'] = df['メモ'].fillna('')

        # 必要なカラムの抽出
        processed_df = df[[
            'transaction_date',
            'description',
            'withdrawal',
            'memo'
        ]]

        return processed_df

    def import_to_staging(self, file_path):
        """CSVファイルをステージングテーブルにインポート"""
        try:
            # CSVファイルの読み込み
            df = pd.read_csv(file_path, encoding='utf-8')
            logging.info(f"Reading file: {file_path}")

            # データの前処理
            processed_df = self.preprocess_data(df)

            # データベースへの接続
            conn = self.connect_db()
            cur = conn.cursor()

            # データの挿入
            insert_query = """
                INSERT INTO card_staging
                (transaction_date, description, withdrawal, memo, imported_at)
                VALUES %s
            """

            # データをタプルのリストに変換
            values = [
                (row.transaction_date, row.description, row.withdrawal, row.memo, datetime.now())
                for row in processed_df.itertuples()
            ]

            # バルクインサート実行
            execute_values(cur, insert_query, values)

            # コミット
            conn.commit()
            logging.info(f"Successfully imported {len(values)} records")

            # 処理済みファイルの移動
            self.move_to_processed(file_path)

        except Exception as e:
            logging.error(f"Error importing file {file_path}: {str(e)}")
            if conn:
                conn.rollback()
            raise

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def move_to_processed(self, file_path):
        """処理済みファイルを移動"""
        filename = os.path.basename(file_path)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        new_filename = f"{timestamp}_{filename}"

        # processedディレクトリが存在しない場合は作成
        os.makedirs(self.processed_dir, exist_ok=True)

        new_path = os.path.join(self.processed_dir, new_filename)
        shutil.move(file_path, new_path)
        logging.info(f"Moved processed file to: {new_path}")

    def process_new_files(self):
        """新規ファイルの一括処理"""
        try:
            files = [f for f in os.listdir(self.input_dir) if f.endswith('.csv')]

            if not files:
                logging.info("No new files to process")
                return

            for file in files:
                file_path = os.path.join(self.input_dir, file)
                logging.info(f"Processing file: {file}")
                self.import_to_staging(file_path)

        except Exception as e:
            logging.error(f"Error processing files: {str(e)}")
            raise

if __name__ == "__main__":
    importer = CardDataImporter()
    importer.process_new_files()
