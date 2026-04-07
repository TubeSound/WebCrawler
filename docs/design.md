---
marp: true
paginate: true
theme: default
size: 16:9
---

# Webクローリング／Markdown化
## リバースエンジニアリング設計書

### 対象
- `crawl_page_links.py`
- `webpage2markdown.py`

### 目的
- Webサイトを巡回して対象ページ URL を収集する
- Webページを Markdown / JSON に変換し、分類・分析・RAG に利用できる形へ整形する

### 成果物
- ページ一覧データ
- ページ構造付き JSON
- section 単位 Markdown

---

# 1. 全体アーキテクチャ

## システムの役割

### クローリング機能
- 開始 URL を起点にページ遷移し、対象ページ URL を収集する
- 同一ドメイン内のみを巡回し、不要 URL を除外する

### 前処理機能
- HTML を取得し、不要要素を除去する
- section と見出し構造を保ちながら Markdown 化する
- テーブル、リンク、フォーム、選択要素を構造化 JSON として保持する

## 処理の流れ

1. 設定ファイルから開始 URL、許可ドメイン、巡回上限を読み込む
2. Playwright でページへアクセスし、リンクと操作可能要素から URL を取得する
3. 巡回結果を JSONL として保存する
4. 各ページ HTML を取得し、BeautifulSoup で前処理する
5. section 単位で Markdown と JSON を生成する

### 図の挿入候補
- 「設定 -> クローラ -> URL一覧 -> HTML取得 -> Markdown/JSON出力」のフロー図

---

# 2. クローリング設計

## 主要ロジック

### URL収集
- `a[href]` からリンクを取得
- 相対 URL は絶対 URL に正規化
- 許可ドメイン外は除外

### 操作要素対応
- `button`
- `role="button"`
- `aria-controls`
- `aria-expanded`
- `tabindex="0"`

これらの要素をクリックし、動的に表示されたリンクや遷移先 URL も収集する。

### 巡回制御
- BFS 形式で未訪問 URL を順次巡回
- `max_pages` で上限を制御
- エラーになった URL は保存対象から除外
- 対象外ドメインは集計して記録

## 主な出力

### URL一覧
- URL
- title
- domain

### 補助情報
- 除外ドメイン
- エラー URL

### 図の挿入候補
- 「開始 URL から同一ドメインを幅優先で巡回する図」

---

# 3. Markdown化／データ整形設計

## HTML前処理

以下の不要要素を除去する。

- `script`
- `style`
- `noscript`
- `svg`
- `iframe`

さらに、装飾アイコンや重複テキストを削減し、解析しやすい HTML に整形する。

## Markdown生成

### section単位で整形
- HTML の `section` を単位に Markdown ブロック化
- 各 block に `heading_tags` を保持
- block 同士は `---` で区切って 1 本の Markdown に連結

### 変換方針
- `h1 -> #`
- `h2 -> ##`
- `h3 -> ###`
- `li -> -`
- テーブルは Markdown に含めず JSON 側へ分離

## JSON出力項目

- `url`
- `title`
- `meta`
- `body_text`
- `markdown_blocks`
- `markdown_text`
- `tables`
- `links`
- `select_elements`
- `forms`
- `page_type_hints`

## 想定利用

- ページ分類
- トピック分析
- RAG 用チャンク生成
- FAQ / 手順 / サービス紹介ページの構造分析

### 図の挿入候補
- 「HTML -> 前処理 -> section抽出 -> Markdown/JSON」の変換図
