# 蝦皮訂單轉 Google Sheets 系統

使用 Python、Pandas、Streamlit 解析蝦皮訂單 CSV，並可選讀取 Google Sheet 以 **thefuzz.token_set_ratio** 做模糊比對。

## 環境

```bash
cd ShopeeOrderSync
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 執行

```bash
streamlit run app.py
```

## 階段一：預處理與驗證

- **訂單總手續費（內部）**：同訂單多列時，只取**第一列**三項手續費加總，避免重複加總。
- **`商品原價` 為單價**：數量展開後單價不變；手續費以**最大餘數法**整數分攤至 **`單件實扣手續費`**。
- **驗證**：逐訂單檢查金額加總（容差 0.01）與手續費加總（須與整數訂單總手續費完全一致）。
- **會計對帳明細**：畫面底部 **`📊 查看會計對帳明細`** expander 可檢視逐訂單對帳表。

## 階段二：Google Sheets 與模糊比對

### appsetting.json（連線設定）

專案根目錄放置 **`appsetting.json`**（可複製 `appsetting.example.json` 後修改），範例：

```json
{
  "report": {
    "defaultPassword": "請填你的預設解鎖密碼"
  },
  "history": {
    "keepBatches": 100
  },
  "googleSheets": {
    "activeProfile": "prod",
    "profiles": {
      "prod": {
        "spreadsheetUrl": "https://docs.google.com/spreadsheets/d/…/edit?…",
        "worksheetName": "預定(大陸現貨)",
        "serviceAccountJsonPath": "credentials.json"
      },
      "test": {
        "spreadsheetUrl": "https://docs.google.com/spreadsheets/d/…/edit?…",
        "worksheetName": "測試工作表",
        "serviceAccountJsonPath": "credentials.json"
      }
    }
  }
}
```

- **`report.defaultPassword`**：報表解鎖密碼預設值；畫面載入時會自動帶入，你仍可臨時手動修改。
- **`history.keepBatches`**：歷史批次保留上限（最少 5，預設 100）；超過上限時自動清理更舊批次。
- **`profitCompare`**：利潤比對設定的永久值（`fxCnyToTwd`、`cardExtraPct`、`amountTolerancePct`）；側欄調整後會自動回寫，下次開啟仍沿用。
- **`activeProfile`**：預設環境（例如 `prod` / `test`）。
- 側欄可切換環境；切換時會清除本次上傳與審核狀態，避免不同表單資料混用。
- **`serviceAccountJsonPath`**：可為相對於專案根目錄的路徑，或本機絕對路徑。
- **`spreadsheetUrl`**：請貼上瀏覽器網址列的**完整**連結（須含 `https://` 與 `/spreadsheets/d/...`），避免只貼片段導致無法解析。
- 側欄不再輸入網址／金鑰路徑，改由此檔讀取。

雲端表版面：**第 1 列**為財務面板（略）；**第 2 列**為標題列，須含 **`品名`**、**`款式細項`**、**`平台`**、**`買家`**、**`賣場售價`**、**`賣場手續費`**；**第 3 列**為合併標題區（略）；**第 4 列起**為資料。程式將 `品名` + 空白 + `款式細項` 合併為比對字串。

**比對規則：**

- 蝦皮側使用 **`清洗後簡體名稱`**；雲端側對 `品名`+`款式細項` 合併後套用**與階段一完全相同**的正規化（標點／括號／Emoji／雜訊片語等 → OpenCC **繁轉簡**），欄位 **`_雲端正規化簡體比對用`** 僅供比對。
- UI 下拉選單摘要仍顯示 **`_雲端合併比對字串`**（表上原文合併，不經 OpenCC）。
- 使用 **`thefuzz.fuzz.token_set_ratio`** 在兩條「簡體正規化字串」之間計分。
- **現貨**：只與 `平台 == 預現貨` 的列比對。
- **預定**：只與 `平台` 為空白（去空白後為空）的列比對。
- **未知**：不比對前先過濾平台（全表列皆可比對）。
- 模糊比對：先取分數最高的 **3 種不同** `_雲端合併比對字串`，再將表中與這 3 種字串**完全相同**的所有列號一併列入下拉選單（同名多列庫存可全選）。
- 逐筆審核：**預設「略過不寫入」**；可手動輸入行號並即時顯示該列 `平台`／`買家`；若多筆訂單選同一列會 **🚨 警告**並鎖定 **確認寫入**；列上已有買家／售價／手續費時須勾選 **強制覆蓋** 才可寫入（寫入 API 仍待接線）。

## CSV 編碼

- 自動偵測會優先 `strict` 解碼並以「必要欄位命中數」選編碼；台灣常見 **Big5** 可於側欄手動指定。
- 建議需含 Emoji 時以 **UTF-8（含 BOM）** 匯出。

## 輸出欄位（重點）

- `合併原始名稱`、`清洗後簡體名稱`
- `現貨預定標記`：現貨 / 預定 / 未知
- `單件實扣手續費`（整數）

## 選用加速

若比對很慢，可另裝 `python-Levenshtein` 加速 thefuzz（非必須）：

```bash
pip install python-Levenshtein
```
