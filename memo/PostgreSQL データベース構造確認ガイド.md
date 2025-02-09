# PostgreSQL データベース構造確認ガイド

## 1. PostgreSQLへの接続

### 1.1 接続の確認
```bash
# PostgreSQLがインストールされているか確認
psql --version
```

### 1.2 dbサービスへの接続
```bash
# 別コンテナ(dbサービス)に接続
psql -h db -U postgres -d financedb
```

接続パラメータの説明：
- `-h db`: ホスト名（docker-compose.ymlで定義されているサービス名）
- `-U postgres`: ユーザー名
- `-d financedb`: データベース名

## 2. データベース一覧の確認

### 2.1 標準的な方法
```sql
\l
```
※バージョンによってはエラーが発生する場合があります

### 2.2 代替方法1（シンプル）
```sql
SELECT datname, datowner
FROM pg_database;
```

### 2.3 代替方法2（詳細）
```sql
SELECT datname AS "Name",
       pg_catalog.pg_get_userbyid(datdba) AS "Owner"
FROM pg_catalog.pg_database
ORDER BY 1;
```

## 3. テーブル一覧の確認
```sql
\dt
```

出力例：
```
            List of relations
 Schema |     Name     | Type  |  Owner
--------+--------------+-------+----------
 public | accounts     | table | postgres
 public | bank_staging | table | postgres
 public | card_staging | table | postgres
 public | categories   | table | postgres
 public | transactions | table | postgres
```

## 4. 各テーブルの構造確認
```sql
-- accounts: 口座情報テーブル
CREATE TABLE public.accounts (
    id integer NOT NULL DEFAULT nextval('accounts_id_seq'::regclass),
    account_name character varying(50) NOT NULL,
    account_type character varying(20) NOT NULL,
    is_active boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT accounts_pkey PRIMARY KEY (id)
);

-- bank_staging: 銀行取引ステージングテーブル
CREATE TABLE public.bank_staging (
    id integer NOT NULL DEFAULT nextval('bank_staging_id_seq'::regclass),
    transaction_date date,
    withdrawal integer,
    deposit integer,
    description text,
    balance integer,
    memo text,
    label text,
    processed boolean DEFAULT false,
    imported_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    error_message text,
    CONSTRAINT bank_staging_pkey PRIMARY KEY (id)
);

-- card_staging: カード取引ステージングテーブル
CREATE TABLE public.card_staging (
    id integer NOT NULL DEFAULT nextval('card_staging_id_seq'::regclass),
    transaction_date date,
    description text,
    amount integer,
    memo text,
    processed boolean DEFAULT false,
    imported_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    error_message text,
    CONSTRAINT card_staging_pkey PRIMARY KEY (id)
);

-- categories: カテゴリマスターテーブル
CREATE TABLE public.categories (
    id integer NOT NULL DEFAULT nextval('categories_id_seq'::regclass),
    category_name character varying(50) NOT NULL,
    is_expense boolean NOT NULL,
    display_order integer,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT categories_pkey PRIMARY KEY (id)
);

-- transactions: 取引メインテーブル
CREATE TABLE public.transactions (
    id integer NOT NULL DEFAULT nextval('transactions_id_seq'::regclass),
    transaction_date date NOT NULL,
    description text NOT NULL,
    withdrawal integer,
    deposit integer,
    amount integer,
    balance integer,
    account_id integer,
    category_id integer,
    transaction_type character varying(20),
    is_regular boolean,
    calculation_target boolean DEFAULT true,
    memo text,
    source_file text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone,
    CONSTRAINT transactions_pkey PRIMARY KEY (id),
    CONSTRAINT transactions_account_id_fkey FOREIGN KEY (account_id) REFERENCES accounts(id),
    CONSTRAINT transactions_category_id_fkey FOREIGN KEY (category_id) REFERENCES categories(id)
);

-- シーケンス定義
CREATE SEQUENCE accounts_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
CREATE SEQUENCE bank_staging_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
CREATE SEQUENCE card_staging_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
CREATE SEQUENCE categories_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
CREATE SEQUENCE transactions_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;

-- テーブル所有権の設定
ALTER TABLE public.accounts OWNER TO postgres;
ALTER TABLE public.bank_staging OWNER TO postgres;
ALTER TABLE public.card_staging OWNER TO postgres;
ALTER TABLE public.categories OWNER TO postgres;
ALTER TABLE public.transactions OWNER TO postgres;

-- シーケンス所有権の設定
ALTER SEQUENCE accounts_id_seq OWNER TO postgres;
ALTER SEQUENCE bank_staging_id_seq OWNER TO postgres;
ALTER SEQUENCE card_staging_id_seq OWNER TO postgres;
ALTER SEQUENCE categories_id_seq OWNER TO postgres;
ALTER SEQUENCE transactions_id_seq OWNER TO postgres;
```

## 5. 参考：各テーブルのレコード数確認
```sql
SELECT COUNT(*) FROM accounts;
SELECT COUNT(*) FROM categories;
SELECT COUNT(*) FROM transactions;
SELECT COUNT(*) FROM bank_staging;
SELECT COUNT(*) FROM card_staging;
```

## 6. セッション終了
```sql
\q
```

## 注意点
- パスワードを求められた場合は、docker-compose.ymlに設定されているパスワードを入力
- エラーが発生した場合は、PostgreSQLのバージョンや設定を確認
- テーブル構造の確認は、適切な権限が必要
