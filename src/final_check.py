import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime, date
import json
from typing import Dict, List, Tuple

class FinalDataValidator:
    def __init__(self, processed_dir: Path):
        self.processed_dir = processed_dir
        self.validation_results = {
            'date_issues': [],
            'amount_issues': [],
            'description_issues': [],
            'balance_issues': []
        }
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('final_validator')
        logger.setLevel(logging.WARNING)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        log_dir = self.processed_dir.parent / 'logs'
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / f'final_validation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

    def validate_all(self) -> Dict:
        # 処理済みファイルの読み込み
        bank_files = list(self.processed_dir.glob('*bank*.csv'))
        card_files = list(self.processed_dir.glob('*card*.csv'))
        # 検証結果に読み込んだファイル名を追加
        self.validation_results['processed_files'] = {
            '読み込んだ銀行ファイル': [f.name for f in bank_files],
            '読み込んだカードファイル': [f.name for f in card_files]
        }

        self.logger.info(f"検出されたファイル - 銀行: {[f.name for f in bank_files]}, カード: {[f.name for f in card_files]}")

        # ファイルが存在する場合のみデータを読み込む
        bank_data = pd.DataFrame()
        card_data = pd.DataFrame()

        if bank_files:
            bank_dfs = []
            for f in bank_files:
                try:
                    df = pd.read_csv(f)
                    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
                    bank_dfs.append(df)
                except Exception as e:
                    self.logger.error(f"銀行データの読み込みエラー ({f.name}): {str(e)}")
            if bank_dfs:
                bank_data = pd.concat(bank_dfs, ignore_index=True)

        if card_files:
            card_dfs = []
            for f in card_files:
                try:
                    df = pd.read_csv(f)
                    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
                    card_dfs.append(df)
                except Exception as e:
                    self.logger.error(f"カードデータの読み込みエラー ({f.name}): {str(e)}")
            if card_dfs:
                card_data = pd.concat(card_dfs, ignore_index=True)

        if bank_data.empty and card_data.empty:
            self.logger.warning("検証対象のファイルが見つかりませんでした")
            return self.validation_results

        self.logger.info(f"読み込んだレコード数 - 銀行: {len(bank_data)}, カード: {len(card_data)}")

        # 各種検証の実行
        self._validate_dates(bank_data, card_data)
        self._validate_amounts(bank_data, card_data)
        self._validate_descriptions(bank_data, card_data)

        if not bank_data.empty:
            self._validate_balance_continuity(bank_data)

        return self.validation_results

    def _validate_dates(self, bank_df: pd.DataFrame, card_df: pd.DataFrame) -> None:
        today = pd.Timestamp.now().date()

        for df, source in [(bank_df, '銀行'), (card_df, 'カード')]:
            if df.empty:
                continue

            # 未来日付のチェック
            future_dates = df[df['transaction_date'].dt.date > today]
            if not future_dates.empty:
                for _, row in future_dates.iterrows():
                    trans_date = row['transaction_date'].strftime('%Y-%m-%d')
                    msg = f"{source}データに未来日付が存在: {trans_date} - {row['description']}"
                    self.logger.warning(msg)
                    self.validation_results['date_issues'].append(msg)

            # 日付の重複チェック
            date_desc_counts = df.groupby([df['transaction_date'].dt.date, 'description']).size().reset_index(name='count')
            duplicates = date_desc_counts[date_desc_counts['count'] > 1]
            if not duplicates.empty:
                for _, row in duplicates.iterrows():
                    msg = f"{source}データに重複が存在: {row['transaction_date']} - {row['description']} ({row['count']}件)"
                    self.logger.warning(msg)
                    self.validation_results['date_issues'].append(msg)

            # 日付の連続性チェック（30日以上の間隔があるケース）
            df_sorted = df.sort_values('transaction_date')
            date_diff = df_sorted['transaction_date'].diff()
            gaps = df_sorted[date_diff > pd.Timedelta(days=30)]
            if not gaps.empty:
                for _, row in gaps.iterrows():
                    prev_date = (row['transaction_date'] - date_diff[row.name]).strftime('%Y-%m-%d')
                    curr_date = row['transaction_date'].strftime('%Y-%m-%d')
                    msg = f"{source}データにギャップが存在: {prev_date} → {curr_date}"
                    self.logger.warning(msg)
                    self.validation_results['date_issues'].append(msg)

    def _validate_amounts(self, bank_df: pd.DataFrame, card_df: pd.DataFrame) -> None:
        AMOUNT_MIN_THRESHOLD = 100
        AMOUNT_MAX_THRESHOLD = 1000000

        for df, source in [(bank_df, '銀行'), (card_df, 'カード')]:
            if df.empty:
                continue

            # 大口取引のチェック
            if 'withdrawal' in df.columns:
                large_withdrawals = df[
                    (df['withdrawal'].notna()) &
                    (df['withdrawal'] >= AMOUNT_MAX_THRESHOLD)
                ]
                for _, row in large_withdrawals.iterrows():
                    trans_date = row['transaction_date'].strftime('%Y-%m-%d')
                    msg = f"{source}データに大口出金: {trans_date} - {row['withdrawal']:,}円 - {row['description']}"
                    self.logger.warning(msg)
                    self.validation_results['amount_issues'].append(msg)

            if 'deposit' in df.columns:
                large_deposits = df[
                    (df['deposit'].notna()) &
                    (df['deposit'] >= AMOUNT_MAX_THRESHOLD)
                ]
                for _, row in large_deposits.iterrows():
                    trans_date = row['transaction_date'].strftime('%Y-%m-%d')
                    msg = f"{source}データに大口入金: {trans_date} - {row['deposit']:,}円 - {row['description']}"
                    self.logger.warning(msg)
                    self.validation_results['amount_issues'].append(msg)

            # 小数点チェック
            for col in ['withdrawal', 'deposit', 'balance']:
                if col in df.columns:
                    non_integer = df[df[col].notna() & (df[col] % 1 != 0)]
                    if not non_integer.empty:
                        msg = f"{source}データに小数点が存在: {col}列"
                        self.logger.warning(msg)
                        self.validation_results['amount_issues'].append(msg)

    def _validate_descriptions(self, bank_df: pd.DataFrame, card_df: pd.DataFrame) -> None:
        SPECIAL_CHARS = ['■', '□', '◆', '◇', '※', '●', '○', '▲', '▼']

        for df, source in [(bank_df, '銀行'), (card_df, 'カード')]:
            if df.empty:
                continue

            # 文字化けの可能性がある文字列をチェック
            suspicious = df[df['description'].str.contains('�', na=False)]
            if not suspicious.empty:
                for _, row in suspicious.iterrows():
                    trans_date = row['transaction_date'].strftime('%Y-%m-%d')
                    msg = f"{source}データに文字化けの可能性: {trans_date} - {row['description']}"
                    self.logger.warning(msg)
                    self.validation_results['description_issues'].append(msg)

            # 特殊文字のチェック
            for char in SPECIAL_CHARS:
                contains_special = df[df['description'].str.contains(char, na=False)]
                if not contains_special.empty:
                    count = len(contains_special)
                    msg = f"{source}データに特殊文字({char})が{count}件存在"
                    self.logger.warning(msg)
                    self.validation_results['description_issues'].append(msg)

    def _validate_balance_continuity(self, bank_df: pd.DataFrame) -> None:
        if 'balance' not in bank_df.columns:
            return

        df_sorted = bank_df.sort_values(['transaction_date', 'balance'], ascending=[True, False])
        df_sorted['transaction_date'] = pd.to_datetime(df_sorted['transaction_date'])

        current_date = None
        current_balance = None

        for idx, row in df_sorted.iterrows():
            date_str = row['transaction_date'].strftime('%Y-%m-%d')

            if current_date != date_str:
                if current_date is not None and current_balance != row['balance']:
                    expected_balance = current_balance
                    if pd.notna(row['deposit']):
                        expected_balance += row['deposit']
                    if pd.notna(row['withdrawal']):
                        expected_balance -= row['withdrawal']

                    if abs(expected_balance - row['balance']) > 1:
                        msg = (f"日付変更時の残高不整合: {date_str} - "
                              f"前日最終残高: {current_balance:,}円, "
                              f"当日残高: {row['balance']:,}円, "
                              f"取引: {row['description']}")
                        self.logger.warning(msg)
                        self.validation_results['balance_issues'].append(msg)

                current_date = date_str

            if pd.notna(row['deposit']) or pd.notna(row['withdrawal']):
                current_balance = row['balance']

def main():
    project_root = Path(__file__).parent.parent
    processed_dir = project_root / 'data/processed'

    validator = FinalDataValidator(processed_dir)
    results = validator.validate_all()

    results_file = processed_dir.parent / 'logs' / f'validation_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

    has_errors = any(results.values())
    results['status'] = 'ERROR' if has_errors else 'OK'
    results['summary'] = '検証でエラーが検出されました' if has_errors else 'すべての検証をパスしました'

    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    if has_errors:
        print(f"検証完了: エラーが検出されました。詳細は {results_file} を確認してください。")
    else:
        print(f"検証完了: すべての検証をパスしました。結果は {results_file} に保存されました。")

if __name__ == '__main__':
    main()
