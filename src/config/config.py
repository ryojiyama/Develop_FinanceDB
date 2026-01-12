# src/config/config.py
import os
from pathlib import Path

# プロジェクトルートディレクトリ
PROJECT_ROOT = Path(__file__).parent.parent.parent

# ディレクトリパス設定
PATHS = {
    'root': PROJECT_ROOT,
    'data': {
        'csv': PROJECT_ROOT / 'data/csv',
        'bank': PROJECT_ROOT / 'data/csv/bank',
        'card': PROJECT_ROOT / 'data/csv/card',
        'processed': PROJECT_ROOT / 'data/processed',
        'archived': PROJECT_ROOT / 'data/archived'
    },
    'logs': PROJECT_ROOT / 'logs',
    'src': PROJECT_ROOT / 'src'
}

# データベース設定
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'db'),
    'database': os.getenv('DB_NAME', 'financedb'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password'),
    'port': int(os.getenv('DB_PORT', '5432'))
}

# ログ設定
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S'
}

def ensure_directories():
    """必要なディレクトリを作成"""
    for key, path in PATHS.items():
        if key == 'data':
            for sub_key, sub_path in path.items():
                sub_path.mkdir(parents=True, exist_ok=True)
        elif isinstance(path, Path):
            path.mkdir(parents=True, exist_ok=True)

def init():
    """初期化処理"""
    ensure_directories()
