import pandas as pd
from pathlib import Path
import csv

def create_test_csv():
    # テストデータの作成
    test_data = [
        # ヘッダー行（3列だけ）
        ['利用日', '利用店名', '利用金額'],

        # 正常なデータ
        ['2024/01/15', 'セブンイレブン渋谷店', '1200', '0', '0', '0', 'テスト用'],
        ['2024/01/15', 'ファミリーマート品川店', '800', '0', '0', '0', 'テスト用'],

        # 重複データ（保持すべきケース）
        ['2024/01/20', 'コナミスポーツクラブ（会費）', '8800', '0', '0', '0', 'テスト用'],
        ['2024/01/20', 'コナミスポーツクラブ（会費）', '8800', '0', '0', '0', 'テスト用'],
        ['2024/01/21', 'id Shopping Mall', '5000', '0', '0', '0', 'テスト用'],
        ['2024/01/21', 'id Shopping Mall', '5000', '0', '0', '0', 'テスト用'],

        # 重複データ（削除すべきケース）
        ['2024/01/25', 'スーパーマーケット', '3000', '0', '0', '0', 'テスト用'],
        ['2024/01/25', 'スーパーマーケット', '3000', '0', '0', '0', 'テスト用'],

        # 分割払いデータ
        ['2024/01/10', '家電量販店', '120000', '120000', '12', '10000', 'テスト用'],
        ['2024/01/11', '家具店', '60000', '60000', '0', '60000', 'テスト用'],  # 分割回数が0（無効）

        # 無効なデータ
        ['2025/01/01', 'FUTURE SHOP', '1500', '0', '0', '0', 'テスト用'],  # 未来の日付
        ['invalid date', 'INVALID DATE SHOP', '2000', '0', '0', '0', 'テスト用'],  # 不正な日付
        ['2024/01/30', 'INVALID AMOUNT SHOP', 'not a number', '0', '0', '0', 'テスト用'],  # 不正な金額

        # 注意書き（無視されるべき行）
        ['', '※ご利用明細は...', '', '', '', '', ''],
        ['', '※キャンペーン情報', '', '', '', '', ''],
    ]

    # プロジェクトのルートディレクトリを取得
    project_root = Path(__file__).parent.parent
    test_dir = project_root / 'data/csv/card'

    # ディレクトリが存在しない場合は作成
    test_dir.mkdir(parents=True, exist_ok=True)

    # CSVファイルの作成
    output_file = test_dir / 'test_card_data.csv'

    # CP932（Shift-JIS）でファイルを書き出し
    with open(output_file, 'w', encoding='cp932', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(test_data)

    print(f"Test CSV file created: {output_file}")

if __name__ == "__main__":
    create_test_csv()
