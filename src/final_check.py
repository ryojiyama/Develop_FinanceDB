import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime, date
import json
from typing import Dict, List, Tuple

class FinalDataValidator:
    def __init__(self, processed_dir: Path):
        """
        最終データ検証クラスの初期化

        Args:
            processed_dir (Path): 処理済みCSVファイルのディレクトリパス
        """
        self.processed_dir = processed_dir
        self.validation_results = {
            'date_issues': [],
            'amount_issues': [],
            'description_issues': [],
            'balance_issues': []
        }

        # ロガーの設定
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """ロガーの設定"""
        logger = logging.getLogger('final_validator')
        logger.setLevel(logging.WARNING)  # 異常値のみを記録

        # ログフォーマットの設定
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # ファイルハンドラの設定
        log_dir = self.processed_dir.parent / 'logs'
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / f'final_validation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

    def validate_all(self) -> Dict:
        """全ての検証を実行"""
        # 処理済みファイルの読み込み
        bank_files = list(self.processed_dir.glob('*bank*.csv'))
        card_files = list(self.processed_dir.glob('*card*.csv'))

        self.logger.info(f"検出されたファイル - 銀行: {[f.name for f in bank_files]}, カード: {[f.name for f in card_files]}")

        bank_data = pd.concat([pd.read_csv(f) for f in bank_files]) if bank_files else pd.DataFrame()
        card_data = pd.concat([pd.read_csv(f) for f in card_files]) if card_files else pd.DataFrame()

        if bank_data.empty and card_data.empty:
            self.logger.warning("検証対象のファイルが見つかりませんでした")
            return self.validation_results

        self.logger.info(f"読み込んだレコード数 - 銀行: {len(bank_data)}, カード: {len(card_data)}")

        # 日付のチェック
        self._validate_dates(bank_data, card_data)

        # 金額のチェック
        self._validate_amounts(bank_data, card_data)

        # 取引内容のチェック
        self._validate_descriptions(bank_data, card_data)

        # 残高の連続性チェック（銀行データのみ）
        if not bank_data.empty:
            self._validate_balance_continuity(bank_data)

        return self.validation_results

    def _validate_dates(self, bank_df: pd.DataFrame, card_df: pd.DataFrame) -> None:
        """
        日付の検証
        - 日付範囲の重複
        - 未来日付の存在
        - データの連続性
        """
        today = pd.Timestamp.now().date()

        for df, source in [(bank_df, '銀行'), (card_df, 'カード')]:
            if df.empty:
                continue

            # 日付をdatetime型に変換
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])

            # 未来日付のチェック
            future_dates = df[df['transaction_date'].dt.date > today]
            if not future_dates.empty:
                for _, row in future_dates.iterrows():
                    msg = f"{source}データに未来日付が存在: {row['transaction_date'].date()} - {row['description']}"
                    self.logger.warning(msg)
                    self.validation_results['date_issues'].append(msg)

            # 日付の重複チェック
            date_desc_counts = df.groupby(['transaction_date', 'description']).size().reset_index(name='count')
            duplicates = date_desc_counts[date_desc_counts['count'] > 1]
            if not duplicates.empty:
                for _, row in duplicates.iterrows():
                    msg = f"{source}データに重複が存在: {row['transaction_date'].date()} - {row['description']} ({row['count']}件)"
                    self.logger.warning(msg)
                    self.validation_results['date_issues'].append(msg)

            # 日付の連続性チェック（30日以上の間隔があるケース）
            df_sorted = df.sort_values('transaction_date')
            date_diff = df_sorted['transaction_date'].diff()
            gaps = df_sorted[date_diff > pd.Timedelta(days=30)]
            if not gaps.empty:
                for _, row in gaps.iterrows():
                    prev_date = row['transaction_date'] - date_diff[row.name]
                    msg = f"{source}データにギャップが存在: {prev_date.date()} → {row['transaction_date'].date()}"
                    self.logger.warning(msg)
                    self.validation_results['date_issues'].append(msg)

    def _validate_amounts(self, bank_df: pd.DataFrame, card_df: pd.DataFrame) -> None:
        """
        金額の検証
        - 異常に大きな取引
        - 異常に小さな取引
        """
        # 金額の閾値設定
        AMOUNT_MIN_THRESHOLD = 100    # 100円未満を少額とみなす
        AMOUNT_MAX_THRESHOLD = 1000000  # 100万円以上を大口取引とみなす

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
                    msg = f"{source}データに大口出金: {row['transaction_date']} - {row['withdrawal']:,}円 - {row['description']}"
                    self.logger.warning(msg)
                    self.validation_results['amount_issues'].append(msg)

                # 少額取引のチェック
                small_withdrawals = df[
                    (df['withdrawal'].notna()) &
                    (df['withdrawal'] < AMOUNT_MIN_THRESHOLD) &
                    (df['withdrawal'] > 0)
                ]
                for _, row in small_withdrawals.iterrows():
                    msg = f"{source}データに少額出金: {row['transaction_date']} - {row['withdrawal']:,}円 - {row['description']}"
                    self.logger.warning(msg)
                    self.validation_results['amount_issues'].append(msg)

            if 'deposit' in df.columns:
                large_deposits = df[
                    (df['deposit'].notna()) &
                    (df['deposit'] >= AMOUNT_MAX_THRESHOLD)
                ]
                for _, row in large_deposits.iterrows():
                    msg = f"{source}データに大口入金: {row['transaction_date']} - {row['deposit']:,}円 - {row['description']}"
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
        """
        取引内容の検証
        - 文字化けチェック
        - 説明文の一貫性
        - 特殊文字の存在
        """
        # 特殊文字のパターン（必要に応じて追加）
        SPECIAL_CHARS = ['■', '□', '◆', '◇', '※', '●', '○', '▲', '▼']

        for df, source in [(bank_df, '銀行'), (card_df, 'カード')]:
            if df.empty:
                continue

            # 文字化けの可能性がある文字列をチェック
            suspicious = df[df['description'].str.contains('�', na=False)]
            if not suspicious.empty:
                for _, row in suspicious.iterrows():
                    msg = f"{source}データに文字化けの可能性: {row['description']}"
                    self.logger.warning(msg)
                    self.validation_results['description_issues'].append(msg)

            # 特殊文字のチェック
            for char in SPECIAL_CHARS:
                contains_special = df[df['description'].str.contains(char, na=False)]
                if not contains_special.empty:
                    msg = f"{source}データに特殊文字({char})が存在"
                    self.logger.warning(msg)
                    self.validation_results['description_issues'].append(msg)

    def _validate_balance_continuity(self, bank_df: pd.DataFrame) -> None:
        """
        残高の連続性を検証
        - 前回残高と今回残高の整合性
        - 取引金額と残高変動の一致
        """
        if 'balance' not in bank_df.columns:
            return

        # 日付と残高でソート（残高の大きい順）
        df_sorted = bank_df.sort_values(['transaction_date', 'balance'], ascending=[True, False])

        # 前日の最終残高を取得するための処理
        df_sorted['prev_date'] = df_sorted['transaction_date'].shift(1)
        df_sorted['is_new_date'] = df_sorted['transaction_date'] != df_sorted['prev_date']
        df_sorted['prev_balance'] = df_sorted.loc[df_sorted['is_new_date'], 'balance'].shift(1)

        # 同一日内での残高変動を検証
        current_date = None
        current_balance = None

        for idx, row in df_sorted.iterrows():
            date = pd.to_datetime(row['transaction_date']).date()

            if current_date != date:
                # 日付が変わった場合
                if current_date is not None and current_balance != row['balance']:
                    # 前日最終残高と当日開始残高を比較
                    expected_balance = current_balance
                    if pd.notna(row['deposit']):
                        expected_balance += row['deposit']
                    if pd.notna(row['withdrawal']):
                        expected_balance -= row['withdrawal']

                    if abs(expected_balance - row['balance']) > 1:  # 1円の誤差は許容
                        msg = (f"日付変更時の残高不整合: {date} - "
                              f"前日最終残高: {current_balance:,}円, "
                              f"当日残高: {row['balance']:,}円, "
                              f"取引: {row['description']}")
                        self.logger.warning(msg)
                        self.validation_results['balance_issues'].append(msg)

                current_date = date

            # 取引後の残高を更新
            if pd.notna(row['deposit']):
                current_balance = row['balance']
            if pd.notna(row['withdrawal']):
                current_balance = row['balance']

def main():
    # プロジェクトのルートディレクトリを取得
    project_root = Path(__file__).parent.parent
    processed_dir = project_root / 'data/processed'

    # バリデーターの初期化と実行
    validator = FinalDataValidator(processed_dir)
    results = validator.validate_all()

    # 結果の保存
    results_file = processed_dir.parent / 'logs' / f'validation_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

    # エラーの有無を確認
    has_errors = any(results.values())

    # 結果にステータスを追加
    results['status'] = 'ERROR' if has_errors else 'OK'
    results['summary'] = '検証でエラーが検出されました' if has_errors else 'すべての検証をパスしました'

    # 結果をJSONファイルに保存
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # ログにも結果を出力
    if has_errors:
        validator.logger.warning(f"検証完了: エラーが検出されました。詳細は {results_file} を確認してください。")
    else:
        validator.logger.info(f"検証完了: すべての検証をパスしました。結果は {results_file} に保存されました。")

if __name__ == '__main__':
    main()
