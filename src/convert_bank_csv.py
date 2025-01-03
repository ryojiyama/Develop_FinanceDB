import pandas as pd
import os
import sys
import logging
from pathlib import Path
from typing import Optional, Dict, Any

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

def read_csv_with_encoding(file_path: Path) -> pd.DataFrame:
    """
    CSVファイルを適切なエンコーディングで読み込む

    Args:
        file_path (Path): CSVファイルのパス
    Returns:
        pd.DataFrame: 読み込んだデータフレーム
    """
    # 試行するエンコーディングリスト
    encodings = ['cp932', 'utf-8', 'shift_jis', 'euc_jp']

    # 最初の7列の定義（実際のCSVファイルの列数）
    base_columns = [
        'col1',  # 年月日
        'col2',  # お引出し
        'col3',  # お預入れ
        'col4',  # お取り扱い内容
        'col5',  # 残高
        'col6',  # メモ
        'col7',  # ラベル
    ]

    # 追加の列（将来の拡張用）
    extended_columns = [
        'col8',  # 取引種別（ATM/振込/引落等）
        'col9',  # 取引先口座情報
        'col10'  # 取引コード
    ]

    # カラム名の最終的なマッピング
    column_mapping = {
        'col1': 'transaction_date',
        'col2': 'withdrawal',
        'col3': 'deposit',
        'col4': 'description',
        'col5': 'balance',
        'col6': 'memo',
        'col7': 'label',
        'col8': 'transaction_type',
        'col9': 'counter_party',
        'col10': 'transaction_code'
    }

    logger.info(f"Attempting to read file: {file_path}")
    logger.debug(f"Base columns to read: {base_columns}")

    for encoding in encodings:
        try:
            logger.debug(f"Attempting to read with {encoding} encoding")

            # まず7列で読み込む
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                names=base_columns,
                header=0,
                on_bad_lines='warn'
            )

            # 拡張列を追加（Nullで初期化）
            for col in extended_columns:
                df[col] = None

            # カラム名のマッピングを適用
            df = df.rename(columns=column_mapping)

            logger.debug(f"Successfully read file with {encoding} encoding. Columns: {df.columns.tolist()}")
            return df

        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.error(f"Error reading file with {encoding}: {str(e)}")
            continue

    raise ValueError(f"Unable to read file with any of the encodings: {encodings}")

def validate_transaction(row: pd.Series) -> Dict[str, Any]:
    """
    取引データの妥当性をチェックする

    Args:
        row (pd.Series): 検証する取引データの行
    Returns:
        Dict[str, Any]: 検証結果と詳細
    """
    result = {
        'is_valid': True,
        'errors': []
    }

    # 日付のバリデーション
    if pd.isna(row['transaction_date']):
        result['is_valid'] = False
        result['errors'].append("Transaction date is missing")
    else:
        try:
            pd.to_datetime(row['transaction_date'])
        except ValueError:
            result['is_valid'] = False
            result['errors'].append("Invalid date format")

    # 取引金額のバリデーション（整数値として処理）
    if pd.notna(row['withdrawal']):
        try:
            withdrawal = int(float(str(row['withdrawal']).replace(',', '')))
            if withdrawal <= 0:
                result['is_valid'] = False
                result['errors'].append("Withdrawal amount must be positive")
        except ValueError:
            result['is_valid'] = False
            result['errors'].append("Invalid withdrawal amount format")

    if pd.notna(row['deposit']):
        try:
            deposit = int(float(str(row['deposit']).replace(',', '')))
            if deposit <= 0:
                result['is_valid'] = False
                result['errors'].append("Deposit amount must be positive")
        except ValueError:
            result['is_valid'] = False
            result['errors'].append("Invalid deposit amount format")

    # 残高のバリデーション
    if pd.isna(row['balance']):
        result['is_valid'] = False
        result['errors'].append("Balance is missing")
    else:
        try:
            balance = float(str(row['balance']).replace(',', ''))
            if balance < 0:
                result['is_valid'] = False
                result['errors'].append("Balance cannot be negative for regular savings account")
        except ValueError:
            result['is_valid'] = False
            result['errors'].append("Invalid balance format")

    # 取引内容のバリデーション
    if pd.isna(row['description']) or str(row['description']).strip() == '':
        result['is_valid'] = False
        result['errors'].append("Description is missing")

    # お引出しとお預入れの同時存在チェック
    if pd.notna(row['withdrawal']) and pd.notna(row['deposit']):
        result['is_valid'] = False
        result['errors'].append("Both withdrawal and deposit cannot have values")

    return result

def clean_and_validate_data(df: pd.DataFrame, source_filename: str, output_dir: Path) -> pd.DataFrame:
    """
    データのクリーニングとバリデーションを行う

    Args:
        df (pd.DataFrame): 元のデータフレーム
        source_filename (str): 元のファイル名
        output_dir (Path): 出力ディレクトリのパス
    Returns:
        pd.DataFrame: クリーニング済みのデータフレーム
    """
    logger.info(f"Starting validation with {len(df)} records")

    # 実行日を取得
    execution_date = pd.Timestamp.now().date()

    # バリデーション結果を格納するリスト
    validation_results = []
    valid_rows = []

    for idx, row in df.iterrows():
        validation_result = validate_transaction(row)

        if validation_result['is_valid']:
            valid_rows.append(idx)
        else:
            error_message = '; '.join(validation_result['errors'])
            logger.warning(f"Row {idx}: {error_message}")
            validation_results.append({
                'row_index': idx,
                'error_message': error_message,
                'source_file': source_filename,
                'processed_at': pd.Timestamp.now()
            })

    # 有効な行のみを抽出
    valid_df = df.loc[valid_rows].copy()
    logger.info(f"After validation: {len(valid_df)} valid records (removed {len(df) - len(valid_df)} records)")

    # バリデーションエラーの保存
    if validation_results:
        error_df = pd.DataFrame(validation_results)
        error_file = output_dir / 'validation_errors.csv'
        if error_file.exists():
            existing_errors = pd.read_csv(error_file)
            error_df = pd.concat([existing_errors, error_df], ignore_index=True)
        error_df.to_csv(error_file, index=False, encoding='utf-8')
        logger.info(f"Saved {len(validation_results)} validation errors to {error_file}")

    # データのクレンジング
    if not valid_df.empty:
        # 日付形式の統一化
        valid_df['transaction_date'] = pd.to_datetime(valid_df['transaction_date']).dt.strftime('%Y-%m-%d')

        # 金額のクレンジング
        def convert_to_integer(x):
            if pd.isna(x):
                return None
            # カンマを削除し、一度floatに変換してから整数に
            cleaned = str(x).replace(',', '')
            return int(float(cleaned))

        # 金額列を整数に変換
        numeric_columns = ['withdrawal', 'deposit', 'balance']
        for col in numeric_columns:
            if col in valid_df.columns:
                # 一度値を変換
                valid_df[col] = valid_df[col].apply(convert_to_integer)
                # 明示的にint64型に変換（NAを許容）
                valid_df[col] = valid_df[col].astype('Int64')

        # 文字列カラムのクレンジング
        string_columns = ['description', 'memo', 'label']
        for col in string_columns:
            if col in valid_df.columns:
                valid_df[col] = valid_df[col].apply(
                    lambda x: str(x).strip() if pd.notna(x) else None
                )

    return valid_df

def process_bank_csv(input_dir: Path, output_dir: Path) -> None:
    """
    銀行取引CSVファイルを処理する

    Args:
        input_dir (Path): 入力CSVファイルのディレクトリパス
        output_dir (Path): 出力CSVファイルのディレクトリパス
    """
    # ディレクトリの存在確認
    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Directories verified - Input: {input_dir}, Output: {output_dir}")

    # CSVファイルを処理
    csv_files = list(input_dir.glob('*.csv'))
    logger.info(f"Found {len(csv_files)} CSV files to process")

    for csv_file in csv_files:
        try:
            logger.info(f"Processing file: {csv_file.name}")

            # CSVファイルを読み込む
            df = read_csv_with_encoding(csv_file)

            # データのクリーニングとバリデーション
            cleaned_df = clean_and_validate_data(df, csv_file.name, output_dir)

            if not cleaned_df.empty:
                # 処理済みファイルを保存
                output_file = output_dir / f'processed_{csv_file.name}'
                cleaned_df.to_csv(output_file, index=False, encoding='utf-8')
                logger.info(f"Successfully processed and saved: {output_file}")
            else:
                logger.warning(f"No valid records found in {csv_file.name}")

        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {str(e)}", exc_info=True)
            continue

if __name__ == '__main__':
    # プロジェクトのルートディレクトリを取得
    project_root = Path(__file__).parent.parent

    # ログディレクトリの作成
    log_dir = project_root / 'logs'
    log_dir.mkdir(exist_ok=True)

    # ログファイルのパス（タイムスタンプ付き）
    log_file = log_dir / f'bank_processing_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.log'

    # ファイルハンドラーの設定
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    # 入力・出力ディレクトリを設定
    input_directory = project_root / 'data/csv/bank'
    output_directory = project_root / 'data/processed'

    logger.info(f"Starting bank CSV processing. Log file: {log_file}")

    try:
        # CSV処理を実行
        process_bank_csv(input_directory, output_directory)
        logger.info("Bank CSV processing completed successfully")
    except Exception as e:
        logger.error(f"Process failed: {str(e)}", exc_info=True)
