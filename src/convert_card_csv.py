import pandas as pd
import os
import sys
import logging
from pathlib import Path

# ロギングの設定
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

def read_csv_with_encoding(file_path: Path) -> pd.DataFrame:
    """
    7列固定でCSVファイルを読み込む
    Args:
        file_path (Path): CSVファイルのパス
    Returns:
        pd.DataFrame: 読み込んだデータフレーム
    """
    # 7列分の列名を定義
    column_names = [
        'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7'
    ]

    encodings = ['cp932', 'utf-8', 'shift_jis', 'euc_jp']

    for encoding in encodings:
        try:
            logger.debug(f"Attempting to read with {encoding} encoding")
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                names=column_names,  # 7列分の列名を指定
                header=0,           # 1行目をスキップ
                on_bad_lines='warn' # 列数の不一致を警告として扱う
            )
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.error(f"Error reading file with {encoding}: {str(e)}")
            continue

    raise ValueError(f"Unable to read file with any of the encodings: {encodings}")

def ensure_directories(input_dir: Path, output_dir: Path) -> None:
    """必要なディレクトリが存在することを確認する"""
    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Directories verified - Input: {input_dir}, Output: {output_dir}")

def save_removed_duplicates(removed_records: pd.DataFrame, source_file: str, output_dir: Path) -> None:
    """
    削除された重複レコードを記録するCSVファイルに保存する
    Args:
        removed_records (pd.DataFrame): 削除されたレコード
        source_file (str): 元のファイル名
        output_dir (Path): 出力ディレクトリ
    """
    removed_file = output_dir / 'removed_duplicates.csv'

    removed_records = removed_records.copy()
    removed_records['processed_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    removed_records['source_file'] = source_file

    # ファイルが存在する場合は追記、なければ新規作成
    if removed_file.exists():
        existing_records = pd.read_csv(removed_file)
        updated_records = pd.concat([existing_records, removed_records], ignore_index=True)
    else:
        updated_records = removed_records

    # 保存
    updated_records.to_csv(removed_file, index=False, encoding='utf-8')
    logger.info(f"Saved {len(removed_records)} removed records to {removed_file}")

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

    # 注意書き行を除去（transaction_dateが空白または日付以外、かつamountが空白または数値以外）
    def is_valid_row(row):
        try:
            # 日付の検証
            if pd.isna(row['transaction_date']):
                return False

            try:
                transaction_date = pd.to_datetime(str(row['transaction_date'])).date()
            except ValueError:
                return False

            # 未来日付のチェック
            if transaction_date > execution_date:
                logger.warning(f"Skipping future date: {transaction_date}")
                return False

            # 金額の検証
            if pd.isna(row['amount']):
                return False

            try:
                # マイナス記号とカンマを考慮して処理
                amount_str = str(row['amount']).strip()
                # 先頭のマイナス記号を一時的に除去
                is_negative = amount_str.startswith('-')
                if is_negative:
                    amount_str = amount_str[1:]

                # カンマを除去して数値に変換
                amount_val = float(amount_str.replace(',', ''))
                # マイナスだった場合は符号を戻す
                if is_negative:
                    amount_val = -amount_val

            except ValueError:
                return False

            # 分割払いの検証
            inst_total = 0
            if 'inst_total' in row and pd.notna(row['inst_total']):
                try:
                    inst_total = float(str(row['inst_total']).replace(',', ''))
                except ValueError:
                    return False

            # 分割払いの場合のみ（inst_total > 0）、分割回数をチェック
            if inst_total > 0:
                if pd.isna(row['inst_num']):
                    return False
                try:
                    inst_num = float(str(row['inst_num']).replace(',', ''))
                    if inst_num <= 0:
                        logger.warning(f"Invalid installment number {inst_num} for installment payment")
                        return False
                except ValueError:
                    return False

            return True

        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            return False

    # 有効な行のみを抽出
    valid_df = df[df.apply(is_valid_row, axis=1)].copy()
    logger.info(f"After basic validation: {len(valid_df)} records (removed {len(df) - len(valid_df)} records)")

    # 日付形式の統一化
    valid_df['transaction_date'] = pd.to_datetime(valid_df['transaction_date']).dt.strftime('%Y-%m-%d')

    # 金額のクレンジング（カンマ除去と数値化）
    numeric_columns = ['amount', 'inst_total', 'inst_amount']
    for col in numeric_columns:
        if col in valid_df.columns:
            valid_df[col] = valid_df[col].apply(lambda x: str(x).replace(',', '') if pd.notna(x) else x)
            valid_df[col] = pd.to_numeric(valid_df[col], errors='coerce')

    # inst_numの数値化
    if 'inst_num' in valid_df.columns:
        valid_df['inst_num'] = pd.to_numeric(valid_df['inst_num'], errors='coerce')

    # 重複チェックと処理
    duplicates = valid_df[valid_df.duplicated(['transaction_date', 'amount'], keep=False)]
    if not duplicates.empty:
        logger.info(f"\n=== Found {len(duplicates)} duplicate records ===")
        # 重複レコードをグループ化して表示
        for (date, amount), group in duplicates.groupby(['transaction_date', 'amount']):
            descriptions = group['description'].unique()
            logger.info(f"Found duplicates: {date}, Amount: {amount}, {' | '.join(descriptions)}")

        # 特定条件に合致するレコードは残し、それ以外の重複は最初の1件のみ残す
        def keep_record(row):
            description = str(row['description']).lower()

            # 特定条件に合致する場合は保持
            if 'id' in description or 'コナミスポーツクラブ（会費）' in description:
                logger.info(f"Keeping all duplicates for special condition: {row['description']}")
                return True

            # それ以外の場合、重複している場合は最初の1件のみ残す
            same_records = valid_df[(valid_df['transaction_date'] == row['transaction_date']) &
                                  (valid_df['amount'] == row['amount'])]
            if len(same_records) > 1:
                is_first = not valid_df[(valid_df['transaction_date'] == row['transaction_date']) &
                                      (valid_df['amount'] == row['amount']) &
                                      (valid_df.index < row.name)].shape[0] > 0
                if not is_first:
                    logger.info(f"Removing duplicate: {row['transaction_date']} - {row['description']}")
                return is_first

            return True
        # 重複処理前のデータを保持
        before_removal = valid_df.copy()
        final_df = valid_df[valid_df.apply(keep_record, axis=1)].copy()
        # 実際に削除されたデータを特定
        removed_records = before_removal[~before_removal.index.isin(final_df.index)]

        # 削除されたデータがある場合、保存
        if not removed_records.empty:
            save_removed_duplicates(removed_records, source_filename, output_dir)  # 引数として受け取ったsource_filenameを使用

        logger.info(f"Duplicate processing completed: {len(final_df)} records remaining")
    else:
        final_df = valid_df
        logger.info("No duplicates found")

    return final_df

def convert_card_csv_columns(input_dir: Path, output_dir: Path) -> None:
    """
    カード明細CSVファイルのカラム名を変換する関数

    Args:
        input_dir (Path): 入力CSVファイルのディレクトリパス
        output_dir (Path): 出力CSVファイルのディレクトリパス
    """
    # 必要な列のインデックスと新しい名前のマッピング
    column_mapping = {
        0: 'transaction_date',  # 1列目: 取引日
        1: 'description',       # 2列目: 利用店名
        2: 'amount',           # 3列目: 利用金額
        3: 'inst_total',       # 4列目: 分割払い合計額
        4: 'inst_num',         # 5列目: 分割回数
        5: 'inst_amount',      # 6列目: 分割1回の支払額
        6: 'memo'              # 7列目: メモ
    }

    # ディレクトリの存在確認
    ensure_directories(input_dir, output_dir)

    # CSVファイルを処理
    csv_files = list(input_dir.glob('*.csv'))
    logger.info(f"Found {len(csv_files)} CSV files to process")

    for csv_file in csv_files:
        try:
            logger.info(f"Processing file: {csv_file.name}")

            # CSVファイルを読み込む
            df = read_csv_with_encoding(csv_file)
            logger.info(f"Reading file: {csv_file.name}")
            logger.debug(f"Original columns: {df.columns.tolist()}")
            logger.debug(f"Number of columns: {len(df.columns)}")

            # 最初の数行のデータを確認用にログ出力
            logger.debug(f"First few rows of data:\n{df.head()}")

            # 必要な列だけを選択して新しい名前を付ける
            if len(df.columns) < 7:  # 少なくとも7列必要
                raise ValueError(f"CSV file has only {len(df.columns)} columns, but need at least 7 columns")

            # 必要な列だけを選択
            selected_df = df.iloc[:, list(column_mapping.keys())]

            # 新しい列名を設定
            selected_df.columns = list(column_mapping.values())

            # データのクリーニングとバリデーション
            cleaned_df = clean_and_validate_data(selected_df, csv_file.name, output_dir)

            # 処理対象をcleaned_dfに置き換え
            df = cleaned_df

            logger.info(f"Cleaned data shape: {df.shape}")
            logger.debug(f"Selected and renamed columns: {df.columns.tolist()}")

            # 処理済みファイルを保存
            output_file = output_dir / f'processed_{csv_file.name}'
            df.to_csv(output_file, index=False, encoding='utf-8')

            logger.info(f"Successfully processed and saved: {output_file}")

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
    log_file = log_dir / f'card_processing_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.log'

    # ロギングの設定
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    console_handler = logging.StreamHandler(sys.stdout)

    # フォーマッターの設定
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # ルートロガーの設定
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    logger = logging.getLogger(__name__)

    # 入力・出力ディレクトリを設定
    input_directory = project_root / 'data/csv/card'
    output_directory = project_root / 'data/processed'

    logger.info(f"Starting CSV conversion process. Log file: {log_file}")

    try:
        # カラム名変換を実行
        convert_card_csv_columns(input_directory, output_directory)
        logger.info("CSV conversion process completed successfully")
    except Exception as e:
        logger.error(f"Process failed: {str(e)}", exc_info=True)
