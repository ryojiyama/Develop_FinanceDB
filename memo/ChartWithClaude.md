読み込むCSVのことで質問があります。現在カード会社からの明細とそのカードに紐づいている口座の明細の２種類があり、微妙に形式が違います。また、口座の明細に支出入の全てが記されているのですが、カードの明細も合わせないと、支出の詳細がわかりません。どのような解決法があるかアプローチを日本語で説明願えますか？
口座の明細をメインに、カード明細をマージしようと思います。口座側の項目"ミツイスミトモカード (カ"とカード明細の合計が一致するか近似値になるはずなのでそれで照合は可能でしょうか？

credit_ransactionsの他に口座の明細のテーブルが必要なのですが、そのテーブルの設計はどうするべきでしょうか？


テーブルの構成に関して手伝ってください。現在以下のカラムを考えています。
計算対象、日付、内容、金額(円)、口座、大項目、中項目、メモ
これはいらないという項目、逆に必要な項目を教えてください。

計算対象：口座振替、カード明細の合計などCSVファイルを読み込んだ際に合計したくない項目を除区ための項目

読み込む予定のCSVの内容です。
銀行取引明細：年月日、お引出し、お預入れ、お取り扱い内容、残高、メモ、ラベル(空欄)
カード明細:年月日、内容、金額、メモ
これらのCSVをクレンジングする時に、お取り扱い内容、および内容のみでカテゴリや定期生の判断を行い、別カラムとしてレコードに追加したい。
また、予算管理や集計用フラグは将来的に追加できるようにしたい。
そして大項目を中項目によって細分化したいのですが、現在は大項目のみで分類を行い、将来的に中項目での細分化を行えるようにしたい。

これまでの会話をマークダウン形式でまとめてダウンロードできるようにしてください。

CREATE TABLE accounts (
    id SERIAL PRIMAL KEY,
    account_name VARCHAR(50) NOT NULL,
    account_type VARCHAR(20) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    transaction_date DATE NOT NULL,
    description TEXT NOT NULL,
    withdrawal INTEGER,
    deposit INTEGER,
    amount INTEGER,
    barance INTEGER,
    account_id INTEGER REFERENCES accounts(id),
    category_id INTEGER REFERENCES categories(id),
    transaction_type VARCHAR(20),
    is_regular BOOLEAN,
    calculation_target BOOLEAN DEFAULT true,
    memo TEXT,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

INSERT INTO categories (category_name, is_expense, display_order) VALUES
    ('食費', true, 1),
    ('光熱費', true, 2),
    ('交通費', true, 3),
    ('日用品', true, 4),
    ('住居費', true, 5);

INSERT INTO categories (category_name, is_expense, display_order) VALUES
    ('給与', false, 101),
    ('賞与', false, 102),
    ('その他収入', false, 103);

INSERT INTO accounts (account_name, account_type) VALUES
    ('メイン口座', '銀行'),
    ('クレジットカード', 'クレジットカード');

SELECT
    t.transaction_date,
    t.description,
    c.category_name,
    t.withdrawal,
    t.deposit,
    t.barance
FROM trandactions t
JOIN categories c ON t.categories_id = c.id
ORDER BY t.trandaction_date;

-- 全ての取引を確認
SELECT
    t.transaction_date,
    t.description,
    c.category_name,
    t.withdrawal,
    t.deposit,
    t.balance
FROM transactions t
JOIN categories c ON t.category_id = c.id
ORDER BY t.transaction_date;

SELECT
    t.transaction_date,
    t.description,
    t.balance
FROM transactions t
JOIN categories c
    ON t.category_id = c.id
ORDER BY t.transaction_date;

SQL Table	CSV Table
transaction_date	山中　亮司　様
description	5302-32**~
amount	空白
Memo	空白
SQLとCSVのテーブル構造の対照表です。まずはCSVのカラムタイトルを変更する処理をPythonで作成してください。
# 条件
1. ~/Dropbox/FinanceData/
├── bank_statements/    # 銀行取引CSVファイル
├── card_statements/    # カード取引CSVファイル
└── processed/         # 処理済みファイル
のcard_statemnetsに入っているCSVファイルを選択
2. csv Tableの列が元のカラム名です。これを"SQL Table"のカラム名に変更する。

　
        0: 'transaction_date',  # 1列目
        1: 'description',       # 2列目
        2: 'amount',           # 3列目
        6: 'memo'              # 7列目
に下記の項目を加えたいです。
3: inst_total
4: inst_num
5. inst_amount


transaction_date	description	amount	inst_total	inst_num	inst_amount	memo
2024-01-10	家電量販店	120000	120000.0	12.0	10000.0	テスト用


def clean_and_validate_data(df: pd.DataFrame, source_filename: str, output_dir: Path) -> pd.DataFrame:
    """
    データのクリーニングとバリデーションを行う
    Args:
        df (pd.DataFrame): 元のデータフレーム
    Returns:
        pd.DataFrame: クリーニング済みのデータフレーム
    """
    logger.info(f"Starting validation with {len(df)} records")
    logger.info("Initial data sample:")
    logger.info(f"\n{df.head()}")

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
    logger.info("Date format standardized")

    # 金額のクレンジング（カンマ除去と数値化）
    numeric_columns = ['amount', 'inst_total', 'inst_amount']
    for col in numeric_columns:
        if col in valid_df.columns:
            valid_df[col] = valid_df[col].apply(lambda x: str(x).replace(',', '') if pd.notna(x) else x)
            valid_df[col] = pd.to_numeric(valid_df[col], errors='coerce')
            logger.info(f"Cleaned {col} column")

    # inst_numの数値化
    if 'inst_num' in valid_df.columns:
        valid_df['inst_num'] = pd.to_numeric(valid_df['inst_num'], errors='coerce')
        logger.info("Converted inst_num to numeric")

    # 重複チェックと処理
    duplicates = valid_df[valid_df.duplicated(['transaction_date', 'amount'], keep=False)]
    if not duplicates.empty:
        logger.info(f"\n=== Found {len(duplicates)} duplicate records ===")
        # 重複レコードをグループ化して表示
        for (date, amount), group in duplicates.groupby(['transaction_date', 'amount']):
            logger.info(f"\nDuplicate set found:")
            logger.info(f"Date: {date}, Amount: {amount}")
            for _, row in group.iterrows():
                logger.info(f"Description: {row['description']}")

        # 削除されるレコードを保持するためのリスト
        records_to_remove = []

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
                    records_to_remove.append(row)
                return is_first

            return True

        # 条件に基づいて重複レコードをフィルタリング
        final_df = valid_df[valid_df.apply(keep_record, axis=1)].copy()

        # 削除されたレコードを保存
        if records_to_remove:
            removed_df = pd.DataFrame(records_to_remove)
            save_removed_duplicates(removed_df, csv_file.name, output_dir)

        logger.info(f"\n=== Duplicate processing summary ===")
        logger.info(f"Original records: {len(valid_df)}")
        logger.info(f"Records after duplicate removal: {len(final_df)}")
        logger.info(f"Removed duplicates: {len(valid_df) - len(final_df)}")
    else:
        final_df = valid_df
        logger.info("No duplicates found")

    return final_df


transaction_date	description	amount	inst_total	inst_num	inst_amount	memo	processed_at	source_file
2024-01-29	飲料自販機／ｉＤ	130					2025-01-02 01:43:27	Vpass_2024-01.csv
2025-01-01	FUTURE SHOP	1500	0.0	0.0	0.0	テスト用	2025-01-02 01:43:27	test_card_data.csv

うーん、このロジックは正しいです。何故なら実際のcsvは正常な処理をされ、アウトプットされています。問題はremoved.duplicates.csvの記述がログファイルや結果と違っていることです。
