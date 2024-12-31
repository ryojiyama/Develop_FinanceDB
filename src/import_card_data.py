import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

def preprocess_card_csv(file_path):
    """カード明細CSVの前処理を行う"""
    # CSVファイルの読み込み
    df = pd.read_csv(file_path,
                     encoding='shift-jis',  # 文字コードを指定
                     sep='\t',             # タブ区切り
                     names=['transaction_date', 'description', 'withdrawal', 'total_month', 'memo'])

    # 日付の形式を変換 (2024/8/1 → 2024-08-01)
    df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.strftime('%Y-%m-%d')

    # 金額のマイナス値を処理（返品などの場合）
    df['withdrawal'] = df['withdrawal'].fillna(0)

    # メモ欄から請求番号などを抽出
    def extract_reference_number(memo):
        if pd.isna(memo):
            return None
        # 数字が含まれている場合、その部分を抽出
        import re
        numbers = re.findall(r'\d+', str(memo))
        return ' '.join(numbers) if numbers else None

    df['reference_number'] = df['memo'].apply(extract_reference_number)

    return df

def import_to_staging(df, db_params):
    """データフレームをステージングテーブルにインポート"""
    conn = None
    try:
        # データベースに接続
        conn = psycopg2.connect(
            host=db_params['host'],
            database=db_params['database'],
            user=db_params['user'],
            password=db_params['password']
        )
        cur = conn.cursor()

        # データを準備
        data = [
            (row.transaction_date, row.description, row.withdrawal,
             row.total_month, row.memo)
            for idx, row in df.iterrows()
        ]

        # 一括挿入
        insert_query = """
        INSERT INTO card_staging
            (transaction_date, description, withdrawal, total_month, memo)
        VALUES %s
        """
        execute_values(cur, insert_query, data)

        conn.commit()
        print(f"{len(data)}件のデータを取り込みました。")

    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def main():
    # データベース接続情報
    db_params = {
        'host': 'db',  # Docker Composeで定義したサービス名
        'database': 'financedb',
        'user': 'postgres',
        'password': 'password'
    }

    # CSVファイルのパス
    file_path = '/data/csv/card/card_transactions.csv'

    # 処理の実行
    try:
        # データの前処理
        df = preprocess_card_csv(file_path)
        print("データの前処理が完了しました。")

        # ステージングテーブルへのインポート
        import_to_staging(df, db_params)
        print("ステージングテーブルへのインポートが完了しました。")

    except Exception as e:
        print(f"処理中にエラーが発生しました: {str(e)}")

if __name__ == "__main__":
    main()
