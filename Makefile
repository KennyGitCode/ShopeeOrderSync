# 各專案獨立一份 Makefile；慣用目標名稱相同（如 make run），recipe 依該專案技術而定即可。
# Windows 若無 make：choco install make，或「Git for Windows」內建 mingw32-make（指令為 mingw32-make run）。
# 規則：recipe 行開頭必須是 Tab，不能是空白。

# 若終端機裡的 python 不是你要的環境，可指定：make run PY=py 或 make run PY=.venv\Scripts\python
PY ?= python

.PHONY: help run install check

help:
	@echo 可用目標：
	@echo   make run      啟動 Streamlit（app.py）
	@echo   make install  安裝依賴（需已啟用 venv 或已將 pip 指向正確環境）
	@echo   make check    檢查目前 $(PY) 能否載入 streamlit
	@echo
	@echo 建議先：python -m venv .venv 並啟用後再 make install、make run
	@echo 若 make run 失敗：先 make check；仍失敗請在同一終端機執行 $(PY) -m pip install -r requirements.txt

check:
	@$(PY) -c "import streamlit; print('streamlit', streamlit.__version__)"

run: check
	$(PY) -m streamlit run app.py

install:
	$(PY) -m pip install -r requirements.txt
