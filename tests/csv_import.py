import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import shutil
from datetime import datetime

class FinanceDataImporter:
    def __init__(self):
        # データベース接続設定
        self.conn = psycopg2.connect(
            dbname="financedb",
            user="postgres",
            password="your_password",  # docker-compose.ymlに設定されているパスワード
            host="db"
        )
        self.cur = self.conn.cursor()

    def import_bank_data(self, csv_path):
        """銀行取引データをインポート"""
        try:
            # CSVファイルを読み込み
            df = pd.read_csv(csv_path, encoding='utf-8')

            # カラム名を英語に変換
            df.columns = [
                'transaction_date', 'withdrawal', 'deposit',
                'description', 'balance', 'memo', 'label'
            ]

            # 日付をデータベース形式に変換
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])

            # 数値データのクリーニング（カンマ除去、NaN処理）
            for col in ['withdrawal', 'deposit', 'balance']:
                df[col] = pd.to_numeric(df[col].fillna(0), errors='coerce')

            # データをステージングテーブルに挿入
            data = [tuple(x) for x in df.values]
            execute_values(
                self.cur,
                """
                INSERT INTO bank_staging
                (transaction_date, withdrawal, deposit, description,
                balance, memo, label)
                VALUES %s
                """,
                data
            )

            self.conn.commit()
            print(f"銀行データをインポートしました: {csv_path}")

            # 処理済みファイルを移動
            self._move_to_processed(csv_path)

        except Exception as e:
            self.conn.rollback()
            print(f"エラーが発生しました: {str(e)}")

    def import_card_data(self, csv_path):
        """カード取引データをインポート"""
        try:
            # CSVファイルを読み込み
            df = pd.read_csv(csv_path, encoding='utf-8')

            # カラム名を英語に変換
            df.columns = ['transaction_date', 'description', 'amount', 'memo']

            # 日付をデータベース形式に変換
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])

            # 金額のクリーニング
            df['amount'] = pd.to_numeric(df['amount'].fillna(0), errors='coerce')

            # データをステージングテーブルに挿入
            data = [tuple(x) for x in df.values]
            execute_values(
                self.cur,
                """
                INSERT INTO card_staging
                (transaction_date, description, amount, memo)
                VALUES %s
                """,
                data
            )

            self.conn.commit()
            print(f"カードデータをインポートしました: {csv_path}")

            # 処理済みファイルを移動
            self._move_to_processed(csv_path)

        except Exception as e:
            self.conn.rollback()
            print(f"エラーが発生しました: {str(e)}")

    def _move_to_processed(self, file_path):
        """処理済みファイルを移動"""
        processed_dir = os.path.join(os.path.dirname(file_path), '../processed')
        os.makedirs(processed_dir, exist_ok=True)

        # タイムスタンプを付加して移動
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = os.path.basename(file_path)
        new_filename = f"{os.path.splitext(filename)[0]}_{timestamp}.csv"

        shutil.move(
            file_path,
            os.path.join(processed_dir, new_filename)
        )

    def close(self):
        """データベース接続を閉じる"""
        self.cur.close()
        self.conn.close()

def main():
    importer = FinanceDataImporter()

    try:
        # 銀行取引データのインポート
        bank_dir = './data/csv/bank'
        for file in os.listdir(bank_dir):
            if file.endswith('.csv'):
                importer.import_bank_data(os.path.join(bank_dir, file))

        # カード取引データのインポート
        card_dir = './data/csv/card'
        for file in os.listdir(card_dir):
            if file.endswith('.csv'):
                importer.import_card_data(os.path.join(card_dir, file))

    finally:
        importer.close()

if __name__ == '__main__':
    main()
