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
