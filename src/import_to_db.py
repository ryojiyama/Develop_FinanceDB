import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from pathlib import Path
from datetime import datetime
import sys
import json
from typing import Optional, List, Dict

from config.config import PATHS, DB_CONFIG, LOG_CONFIG, ensure_directories, init

class DatabaseImporter:
    def __init__(self):
        # 初期設定の実行
        init()

        # ロガーの設定
        self.logger = self._setup_logger()

        # データベース接続
        self.conn = self._connect_db()
        self.cur = self.conn.cursor()

    def _setup_logger(self) -> logging.Logger:
        """ロガーの設定"""
        logger = logging.getLogger('db_importer')
        logger.setLevel(getattr(logging, LOG_CONFIG['level']))

        # ログファイルの設定
        log_file = PATHS['logs'] / f'db_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(LOG_CONFIG['format'], datefmt=LOG_CONFIG['date_format']))
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(LOG_CONFIG['format'], datefmt=LOG_CONFIG['date_format']))
        logger.addHandler(console_handler)

        return logger

    def _connect_db(self):
        """データベースに接続"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            self.logger.info("Database connected successfully")
            return conn
        except Exception as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            raise
    def _check_validation_status(self) -> bool:
        """最新の検証結果を確認（重複のみのエラーは許容）"""
        validation_files = list(PATHS['logs'].glob('validation_results_*.json'))
        if not validation_files:
            self.logger.error("No validation results found. Please run the cleansing script first.")
            return False

        latest_validation = max(validation_files, key=lambda x: x.stat().st_mtime)
        with open(latest_validation, 'r', encoding='utf-8') as f:
            results = json.load(f)

        # status が OK ならそのまま通過
        if results.get('status') == 'OK':
            self.logger.info("Validation status: OK")
            return True

        # ERROR の場合、重大なエラーがあるかチェック
        has_critical_errors = False

        # 文字化け、金額異常、残高不整合は重大エラー
        if results.get('description_issues'):
            self.logger.error(f"Critical: Description issues found - {len(results['description_issues'])} issues")
            has_critical_errors = True

        if results.get('amount_issues'):
            self.logger.error(f"Critical: Amount issues found - {len(results['amount_issues'])} issues")
            has_critical_errors = True

        if results.get('balance_issues'):
            self.logger.error(f"Critical: Balance issues found - {len(results['balance_issues'])} issues")
            has_critical_errors = True

        # 重大エラーがあれば中止
        if has_critical_errors:
            self.logger.error("Validation failed with critical errors. Please fix the issues first.")
            return False

        # 日付関連の警告のみ（重複、ギャップ、未来日付）
        if results.get('date_issues'):
            self.logger.warning(f"Date issues detected (duplicates/gaps) - {len(results['date_issues'])} issues")
            self.logger.warning("Proceeding with import as these are non-critical warnings")

        return True

    def import_bank_data(self) -> bool:
        """銀行取引データをインポート"""
        try:
            bank_files = list(PATHS['data']['processed'].glob('processed_*bank*.csv'))
            if not bank_files:
                self.logger.warning("No bank data files found")
                return True

            for file in bank_files:
                df = pd.read_csv(file)

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

                self.logger.info(f"Imported bank data from {file.name}")

            return True

        except Exception as e:
            self.logger.error(f"Error importing bank data: {str(e)}")
            return False

    def import_card_data(self) -> bool:
        """カード取引データをインポート"""
        try:
            card_files = list(PATHS['data']['processed'].glob('processed_*card*.csv'))
            if not card_files:
                self.logger.warning("No card data files found")
                return True

            for file in card_files:
                df = pd.read_csv(file)

                # 必要な列だけを抽出（分割払い情報は除外）
                # CSVの列: transaction_date, description, amount, inst_total, inst_num, inst_amount, memo
                # 必要な列: transaction_date, description, amount, memo
                df_filtered = df[['transaction_date', 'description', 'amount', 'memo']].copy()

                # データをステージングテーブルに挿入
                data = [tuple(x) for x in df_filtered.values]
                execute_values(
                    self.cur,
                    """
                    INSERT INTO card_staging
                    (transaction_date, description, amount, memo)
                    VALUES %s
                    """,
                    data
                )

                self.logger.info(f"Imported card data from {file.name}")

            return True

        except Exception as e:
            self.logger.error(f"Error importing card data: {str(e)}")
            return False

    def archive_processed_files(self) -> None:
        """処理済みファイルをアーカイブ"""
        archive_dir = PATHS['data']['archived'] / datetime.now().strftime("%Y%m%d")
        archive_dir.mkdir(parents=True, exist_ok=True)

        for file in PATHS['data']['processed'].glob('processed_*.csv'):
            new_path = archive_dir / file.name
            file.rename(new_path)
            self.logger.info(f"Archived {file.name}")

    def import_data(self) -> bool:
        """データのインポートを実行"""
        try:
            # 検証結果の確認
            if not self._check_validation_status():
                return False

            # トランザクション開始
            self.logger.info("Starting data import process")

            # 銀行データのインポート
            if not self.import_bank_data():
                raise Exception("Bank data import failed")

            # カードデータのインポート
            if not self.import_card_data():
                raise Exception("Card data import failed")

            # コミット
            self.conn.commit()
            self.logger.info("All data imported successfully")

            # 処理済みファイルのアーカイブ
            self.archive_processed_files()

            return True

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Import process failed: {str(e)}")
            return False

        finally:
            self.cur.close()
            self.conn.close()

def main():
    importer = DatabaseImporter()

    if importer.import_data():
        print("\n✅ データベースへのインポートが正常に完了しました。")
    else:
        print("\n❌ インポート処理中にエラーが発生しました。")
        print("ログファイルを確認して、エラーを修正してください。")
        sys.exit(1)

if __name__ == '__main__':
    main()
