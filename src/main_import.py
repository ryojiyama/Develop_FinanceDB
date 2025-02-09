import os
import logging
from pathlib import Path
import subprocess
import sys
from datetime import datetime

# ロギングの設定
def setup_logging(log_dir: Path) -> logging.Logger:
    """ロギングの設定を行う"""
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f'import_process_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    # ロガーの設定
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # ファイルハンドラー
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    # コンソールハンドラー
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)

    return logger

def run_script(script_path: Path, logger: logging.Logger) -> bool:
    """Pythonスクリプトを実行する"""
    try:
        logger.info(f"Running script: {script_path}")
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"Script output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Script failed with error: {e.stderr}")
        return False

def main():
    # プロジェクトのルートディレクトリを取得
    project_root = Path(__file__).parent.parent

    # 各ディレクトリのパスを設定
    src_dir = project_root / 'src'
    log_dir = project_root / 'logs'

    # ロギングの設定
    logger = setup_logging(log_dir)
    logger.info("Starting data import process")

    try:
        # 1. データクレンジングスクリプトの実行
        logger.info("Step 1: Running data cleansing scripts")

        # 銀行データのクレンジング
        bank_cleansing_script = src_dir / 'convert_bank_csv.py'
        if not run_script(bank_cleansing_script, logger):
            raise Exception("Bank data cleansing failed")

        # カードデータのクレンジング
        card_cleansing_script = src_dir / 'convert_card_csv.py'
        if not run_script(card_cleansing_script, logger):
            raise Exception("Card data cleansing failed")

        # 2. データインポートスクリプトの実行
        logger.info("Step 2: Running data import script")
        import_script = src_dir / 'csv-import.py'
        if not run_script(import_script, logger):
            raise Exception("Data import failed")

        logger.info("Data import process completed successfully")

    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
