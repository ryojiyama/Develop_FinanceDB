import pandas as pd
from pathlib import Path
import datetime

def create_test_bank_data():
    """テスト用の銀行取引データを作成"""
    data = [
        # 基本取引（正常データ）
        {
            "transaction_date": "2024-01-05",
            "withdrawal": 5000,
            "deposit": None,
            "description": "コンビニATM引出",
            "balance": 495000,
            "memo": None,
            "label": None
        },
        # 未来日付
        {
            "transaction_date": "2025-06-01",
            "withdrawal": None,
            "deposit": 300000,
            "description": "給与振込",
            "balance": 795000,
            "memo": None,
            "label": None
        },
        # 大口取引
        {
            "transaction_date": "2024-01-10",
            "withdrawal": 1500000,
            "deposit": None,
            "description": "不動産購入",
            "balance": 295000,
            "memo": None,
            "label": None
        },
        # 異常値（少額取引）
        {
            "transaction_date": "2024-01-15",
            "withdrawal": 1,
            "deposit": None,
            "description": "支払い",
            "balance": 293764,
            "memo": None,
            "label": None
        },
        # 文字化けを含む説明
        {
            "transaction_date": "2024-01-20",
            "withdrawal": 3000,
            "deposit": None,
            "description": "カ�ド支払い",
            "balance": 290764,
            "memo": None,
            "label": None
        },
        # 特殊文字を含む説明
        {
            "transaction_date": "2024-01-25",
            "withdrawal": 5000,
            "deposit": None,
            "description": "■お引き落とし",
            "balance": 285764,
            "memo": None,
            "label": None
        },
        # 残高の不整合
        {
            "transaction_date": "2024-01-30",
            "withdrawal": 10000,
            "deposit": None,
            "description": "ATM引出",
            "balance": 265764,  # 期待される残高: 275764
            "memo": None,
            "label": None
        },
        # 同一日付の重複取引
        {
            "transaction_date": "2024-01-30",
            "withdrawal": 10000,
            "deposit": None,
            "description": "ATM引出",
            "balance": 255764,
            "memo": None,
            "label": None
        },
    ]

    # 日付の大きなギャップを作成（2ヶ月以上）
    next_transaction = {
        "transaction_date": "2024-04-01",
        "withdrawal": None,
        "deposit": 50000,
        "description": "振込",
                    "balance": 305764,
        "memo": None,
        "label": None
    }
    data.append(next_transaction)

    return pd.DataFrame(data)

def create_test_card_data():
    """テスト用のカード取引データを作成"""
    data = [
        # 基本取引（正常データ）
        {
            "transaction_date": "2024-01-05",
            "description": "スーパーマーケット",
            "amount": 5000,
            "memo": None
        },
        # 未来日付
        {
            "transaction_date": "2025-06-01",
            "description": "ガソリン",
            "amount": 7000,
            "memo": None
        },
        # 大口取引
        {
            "transaction_date": "2024-01-10",
            "description": "家電量販店",
            "amount": 1200000,
            "memo": None
        },
        # 異常値（少額取引）
        {
            "transaction_date": "2024-01-15",
            "description": "コンビニ",
            "amount": 1,
            "memo": None
        },
        # 文字化けを含む説明
        {
            "transaction_date": "2024-01-20",
            "description": "レストラン�店",
            "amount": 3000,
            "memo": None
        }
    ]

    return pd.DataFrame(data)

def main():
    # 出力ディレクトリの設定
    project_root = Path(__file__).parent.parent
    test_data_dir = project_root / 'data/test'
    test_data_dir.mkdir(parents=True, exist_ok=True)

    # 銀行取引データの作成と保存
    bank_df = create_test_bank_data()
    bank_file = test_data_dir / 'processed_test_bank.csv'
    bank_df.to_csv(bank_file, index=False, encoding='utf-8')

    # カード取引データの作成と保存
    card_df = create_test_card_data()
    card_file = test_data_dir / 'processed_test_card.csv'
    card_df.to_csv(card_file, index=False, encoding='utf-8')

    print(f"テストデータを作成しました：\n{bank_file}\n{card_file}")

if __name__ == "__main__":
    main()
