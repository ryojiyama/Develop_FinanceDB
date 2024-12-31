FROM python:3
ENV PYTHONUNBUFFERED 1

# システムパッケージのインストール
RUN apt-get update \
    && apt-get install -y postgresql-client \
    && apt-get install -y nodejs npm \
    && rm -rf /var/lib/apt/lists/*

# Pythonパッケージのインストール
RUN pip install django psycopg2 pandas \
    # データ可視化パッケージ
    matplotlib \
    seaborn \
    plotly \
    # Jupyter関連パッケージ
    jupyterlab \
    ipykernel \
    # GUI開発用パッケージ
    flet \
    # 開発支援パッケージ
    black \
    flake8 \
    pytest

# Jupyter kernel の設定
RUN python -m ipykernel install --user --name=finance-kernel --display-name="Finance Project Python"

# 作業ディレクトリの設定
WORKDIR /workspace
