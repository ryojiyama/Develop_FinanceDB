import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_staging_table():
    conn_params = {
        "host": "db",
        "port": "5432",
        "database": "financedb",
        "user": "postgres",
        "password": "password"
    }
    
    # ステージングテーブル作成用のSQL
    create_staging_sql = """
    -- 銀行取引ステージングテーブル
    CREATE TABLE IF NOT EXISTS bank_staging (
        id SERIAL PRIMARY KEY,
        transaction_date DATE,           -- 年月日
        withdrawal INTEGER,              -- お引出し
        deposit INTEGER,                 -- お預入れ
        description TEXT,                -- お取り扱い内容
        balance INTEGER,                 -- 残高
        memo TEXT,                       -- メモ
        label TEXT,                      -- ラベル
        processed BOOLEAN DEFAULT false, -- 処理済みフラグ
        imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        error_message TEXT
    );

    -- カード取引ステージングテーブル
    CREATE TABLE IF NOT EXISTS card_staging (
        id SERIAL PRIMARY KEY,
        transaction_date DATE,           -- 年月日
        description TEXT,                -- 内容
        amount INTEGER,                  -- 金額
        memo TEXT,                       -- メモ
        processed BOOLEAN DEFAULT false, -- 処理済みフラグ
        imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        error_message TEXT
    );
    """
    
    try:
        conn = psycopg2.connect(**conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cur:
            cur.execute(create_staging_sql)
            print("Staging tables created successfully!")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    create_staging_table()