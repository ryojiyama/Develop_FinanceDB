# /workspace/src/test_db_connection.py
from config.config import DB_CONFIG
import psycopg2

print("🔌 データベース接続テスト")
print(f"接続先: {DB_CONFIG['host']}")
print(f"データベース: {DB_CONFIG['database']}")
print(f"ユーザー: {DB_CONFIG['user']}")
print("")

try:
    conn = psycopg2.connect(**DB_CONFIG)
    print("✅ データベース接続成功！")
    print("config.py のパスワード設定は正しいです")
    conn.close()
except psycopg2.OperationalError as e:
    print("❌ 接続失敗: パスワードが間違っている可能性があります")
    print(f"エラー詳細: {e}")
except Exception as e:
    print(f"❌ 予期しないエラー: {e}")
