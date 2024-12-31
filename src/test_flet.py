import flet as ft

def main(page: ft.Page):
    # ページの基本設定
    page.title = "家計簿アプリテスト"
    page.window_width = 800
    page.window_height = 600
    page.padding = 50

    # テキスト表示用の変数
    display_text = ft.Text(size=20)

    # ボタンクリック時の処理
    def button_clicked(e):
        display_text.value = "ボタンがクリックされました！"
        page.update()

    # 画面要素の配置
    page.add(
        ft.Text("家計簿アプリケーション テスト", size=30, weight=ft.FontWeight.BOLD),
        ft.ElevatedButton(text="TestButton", on_click=button_clicked),
        display_text
    )

# アプリケーションの起動
ft.app(target=main, view=ft.WEB_BROWSER, port=8550)
