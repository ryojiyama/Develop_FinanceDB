import sys
import logging
from pathlib import Path
import subprocess
from datetime import datetime
import json

from config.config import PATHS, LOG_CONFIG, ensure_directories, init

class DataProcessor:
    def __init__(self):
        # 初期設定の実行
        init()

        # ロガーの設定
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """ロガーの設定"""
        logger = logging.getLogger('data_processor')
        logger.setLevel(getattr(logging, LOG_CONFIG['level']))

        # ログファイルの設定
        log_file = PATHS['logs'] / f'cleansing_process_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(LOG_CONFIG['format'], datefmt=LOG_CONFIG['date_format']))
        logger.addHandler(file_handler)

        # コンソール出力の設定
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(LOG_CONFIG['format'], datefmt=LOG_CONFIG['date_format']))
        logger.addHandler(console_handler)

        return logger

    def run_script(self, script_path: Path) -> bool:
        """Pythonスクリプトを実行する"""
        try:
            self.logger.info(f"Running script: {script_path}")
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                check=True,
                env={'PYTHONPATH': str(Path(__file__).parent.parent)}  # srcディレクトリをPYTHONPATHに追加
            )
            self.logger.info(f"Script output: {result.stdout}")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Script failed with error: {e.stderr}")
            return False

    def process_data(self) -> bool:
        """データの処理を実行する"""
        try:
            # スクリプトの実行パスを設定
            scripts_dir = Path(__file__).parent
            bank_script = scripts_dir / 'convert_bank_csv.py'
            card_script = scripts_dir / 'convert_card_csv.py'
            check_script = scripts_dir / 'final_check.py'

            # 1. 銀行データのクレンジング
            self.logger.info("Step 1: Processing bank data")
            if not self.run_script(bank_script):
                raise Exception("Bank data processing failed")

            # 2. カードデータのクレンジング
            self.logger.info("Step 2: Processing card data")
            if not self.run_script(card_script):
                raise Exception("Card data processing failed")

            # 3. 最終チェック
            self.logger.info("Step 3: Running final validation")
            if not self.run_script(check_script):
                raise Exception("Final validation failed")

            # 検証結果の確認
            latest_validation = self._get_latest_validation_result()
            if latest_validation:
                with open(latest_validation, 'r', encoding='utf-8') as f:
                    validation_results = json.load(f)

                if validation_results.get('status') == 'ERROR':
                    self.logger.error("Validation failed. Check the validation results file for details.")
                    return False

            self.logger.info("All processing steps completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}")
            return False

    def _get_latest_validation_result(self) -> Path:
        """最新の検証結果ファイルを取得する"""
        validation_files = list(PATHS['logs'].glob('validation_results_*.json'))
        if not validation_files:
            return None
        return max(validation_files, key=lambda x: x.stat().st_mtime)

def main():
    processor = DataProcessor()

    if processor.process_data():
        print("\n✅ データ処理が正常に完了しました。")
        print("次のステップ: データベースインポートスクリプト（import_to_db.py）を実行してください。")
    else:
        print("\n❌ データ処理中にエラーが発生しました。")
        print("ログファイルを確認して、エラーを修正してください。")
        sys.exit(1)

if __name__ == '__main__':
    main()
