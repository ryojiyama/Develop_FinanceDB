# /workspace/src/test_config.py を作成

from config.config import PATHS, DB_CONFIG, LOG_CONFIG, init

# 初期化を実行（ディレクトリ作成）
print("🔧 初期化を実行します...")
init()
print("✅ 初期化完了！\n")

# 1. PATHS の確認
print("📁 ファイルの置き場所:")
print(f"  プロジェクトルート: {PATHS['root']}")
print(f"  銀行CSV: {PATHS['data']['bank']}")
print(f"  カードCSV: {PATHS['data']['card']}")
print(f"  処理済み: {PATHS['data']['processed']}")
print(f"  ログ: {PATHS['logs']}\n")

# 2. DB_CONFIG の確認
print("🗄️ データベース接続情報:")
print(f"  ホスト: {DB_CONFIG['host']}")
print(f"  データベース名: {DB_CONFIG['database']}")
print(f"  ユーザー: {DB_CONFIG['user']}")
print(f"  パスワード: {'*' * 8}")  # パスワードは隠す
print(f"  ポート: {DB_CONFIG['port']}\n")

# 3. LOG_CONFIG の確認
print("📝 ログ設定:")
print(f"  レベル: {LOG_CONFIG['level']}")
print(f"  形式: {LOG_CONFIG['format']}\n")

# 4. ディレクトリが作成されたか確認
print("✅ 作成されたディレクトリ:")
for key, path in PATHS['data'].items():
    exists = "✅" if path.exists() else "❌"
    print(f"  {exists} {key}: {path}")
