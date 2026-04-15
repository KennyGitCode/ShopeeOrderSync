import React, { useMemo, useState } from "react";
import { aggregateReport, type ReportOrder, type RankedOrder } from "./reportAggregator";
import {
  runDualWriteSync,
  type DualWriteApi,
  type DualWriteSyncResult,
  type SyncStage,
} from "./dualWriteSync";

export interface SyncPreviewDashboardProps {
  orders: ReportOrder[];
  onConfirmSync?: (payload: {
    startDate: string;
    endDate: string;
    filteredOrders: ReportOrder[];
  }) => void;
  syncApi?: DualWriteApi;
  onSyncResult?: (result: DualWriteSyncResult) => void;
}

function toDateInputValue(value: string | Date | number): string {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toISOString().slice(0, 10);
}

function inferDefaultRange(orders: ReportOrder[]): { startDate: string; endDate: string } {
  const validDates = orders
    .map((o) => toDateInputValue(o.訂單日期))
    .filter((d) => d !== "")
    .sort();
  const today = new Date().toISOString().slice(0, 10);
  if (validDates.length === 0) {
    return { startDate: today, endDate: today };
  }
  return {
    startDate: validDates[0],
    endDate: validDates[validDates.length - 1],
  };
}

function money(n: number): string {
  return new Intl.NumberFormat("zh-TW", {
    style: "currency",
    currency: "TWD",
    maximumFractionDigits: 0,
  }).format(n || 0);
}

function pct(n: number): string {
  return `${((n || 0) * 100).toFixed(1)}%`;
}

function renderOrderLine(item: RankedOrder, accentClass = "text-slate-700"): JSX.Element {
  return (
    <li key={`${item.index}-${item.商品名稱}-${item.訂單日期}`} className="rounded-lg border border-slate-200 bg-white px-3 py-2">
      <div className="flex items-center justify-between gap-3">
        <p className="truncate text-sm font-medium text-slate-800">{item.商品名稱 || "（未命名商品）"}</p>
        <span className={`shrink-0 text-sm font-semibold ${accentClass}`}>{money(item.最終利潤)}</span>
      </div>
      <div className="mt-1 flex items-center justify-between text-xs text-slate-500">
        <span>{item.訂單日期 || "日期未知"}</span>
        <span>手續費率 {pct(item.手續費率)}</span>
      </div>
    </li>
  );
}

export function SyncPreviewDashboard({
  orders,
  onConfirmSync,
  syncApi,
  onSyncResult,
}: SyncPreviewDashboardProps): JSX.Element {
  const defaults = useMemo(() => inferDefaultRange(orders), [orders]);
  const [startDate, setStartDate] = useState(defaults.startDate);
  const [endDate, setEndDate] = useState(defaults.endDate);
  const [syncStage, setSyncStage] = useState<SyncStage>("idle");
  const [isSyncing, setIsSyncing] = useState(false);
  const [syncMessage, setSyncMessage] = useState("");

  const summary = useMemo(() => {
    if (!startDate || !endDate) return null;
    try {
      return aggregateReport({ orders, startDate, endDate });
    } catch {
      return null;
    }
  }, [orders, startDate, endDate]);

  const hasWarningFee = (summary?.averageFeeRate || 0) > 0.1;
  const isInvalidRange = !summary;
  const canSync = Boolean(summary) && !isSyncing;

  async function handleSyncClick(): Promise<void> {
    if (!summary) return;

    if (syncApi) {
      setSyncMessage("");
      const result = await runDualWriteSync({
        orders,
        startDate,
        endDate,
        api: syncApi,
        setLoading: setIsSyncing,
        onStageChange: (stage) => {
          setSyncStage(stage);
          const stageText: Record<SyncStage, string> = {
            idle: "",
            aggregating: "正在彙整資料...",
            "writing-details": "正在寫入訂單明細...",
            "writing-summary": "正在寫入結算摘要...",
            "clearing-local": "正在清空本地已上傳資料...",
            completed: "同步與結算成功。",
          };
          setSyncMessage(stageText[stage]);
        },
      });
      onSyncResult?.(result);
      if (!result.ok) {
        setSyncMessage(result.userMessage);
      }
      return;
    }

    if (!onConfirmSync) return;
    onConfirmSync({
      startDate,
      endDate,
      filteredOrders: summary.filteredOrders,
    });
  }

  return (
    <section className="space-y-6 rounded-2xl bg-slate-50 p-6">
      <header className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-900">同步前結算戰報</h2>
        <p className="mt-1 text-sm text-slate-500">請先檢視日期區間與戰報，確認後再批次同步至雲端。</p>

        <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
          <label className="space-y-1">
            <span className="text-xs font-medium text-slate-600">開始日期</span>
            <input
              type="date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm outline-none transition hover:border-blue-400 focus:border-blue-500"
            />
          </label>
          <label className="space-y-1">
            <span className="text-xs font-medium text-slate-600">結束日期</span>
            <input
              type="date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm outline-none transition hover:border-blue-400 focus:border-blue-500"
            />
          </label>
        </div>
        {isInvalidRange ? (
          <p className="mt-3 text-sm font-medium text-red-600">日期區間無效，請確認開始與結束日期。</p>
        ) : null}
      </header>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <article className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <p className="text-xs font-medium text-slate-500">總實售收入</p>
          <p className="mt-2 text-2xl font-bold text-slate-900">{money(summary?.totalRevenue || 0)}</p>
        </article>
        <article className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <p className="text-xs font-medium text-slate-500">總利潤</p>
          <p className="mt-2 text-2xl font-bold text-emerald-600">{money(summary?.totalProfit || 0)}</p>
        </article>
        <article className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <p className="text-xs font-medium text-slate-500">平均手續費率</p>
          <p className={`mt-2 text-2xl font-bold ${hasWarningFee ? "text-orange-600" : "text-slate-900"}`}>
            {pct(summary?.averageFeeRate || 0)}
          </p>
        </article>
      </div>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
        <article className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <h3 className="text-sm font-semibold text-slate-900">利潤金雞母 Top 3</h3>
          <ul className="mt-3 space-y-2">
            {(summary?.topMVP || []).length > 0 ? (
              (summary?.topMVP || []).map((item) => renderOrderLine(item, "text-emerald-600"))
            ) : (
              <li className="rounded-lg border border-dashed border-slate-200 px-3 py-4 text-sm text-slate-500">此區間沒有資料。</li>
            )}
          </ul>
        </article>

        <article className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
          <h3 className="text-sm font-semibold text-slate-900">手續費刺客 Top 3</h3>
          <ul className="mt-3 space-y-2">
            {(summary?.feeAssassins || []).length > 0 ? (
              (summary?.feeAssassins || []).map((item) => (
                <li key={`${item.index}-fee-${item.商品名稱}`} className="rounded-lg border border-orange-100 bg-orange-50 px-3 py-2">
                  <div className="flex items-center justify-between gap-3">
                    <p className="truncate text-sm font-medium text-slate-800">{item.商品名稱 || "（未命名商品）"}</p>
                    <span className="shrink-0 text-sm font-bold text-orange-600">{pct(item.手續費率)}</span>
                  </div>
                  <p className="mt-1 text-xs text-slate-500">收入 {money(item.實售收入)}｜手續費 {money(item.單件實扣手續費)}</p>
                </li>
              ))
            ) : (
              <li className="rounded-lg border border-dashed border-slate-200 px-3 py-4 text-sm text-slate-500">此區間沒有資料。</li>
            )}
          </ul>
        </article>

        <article className="rounded-xl border border-red-200 bg-white p-4 shadow-sm">
          <h3 className="text-sm font-semibold text-red-600">做白工警報</h3>
          <ul className="mt-3 space-y-2">
            {(summary?.lowProfitWarnings || []).length > 0 ? (
              (summary?.lowProfitWarnings || []).map((item) => (
                <li key={`${item.index}-warn-${item.商品名稱}`} className="rounded-lg border border-red-200 bg-red-50 px-3 py-2">
                  <div className="flex items-center justify-between gap-2">
                    <p className="truncate text-sm font-medium text-slate-800">{item.商品名稱 || "（未命名商品）"}</p>
                    <span className="shrink-0 text-xs font-bold text-red-600">{pct(item.利潤率)}</span>
                  </div>
                  <p className="mt-1 text-xs text-red-600">利潤 {money(item.最終利潤)}，請優先檢查售價、成本與手續費。</p>
                </li>
              ))
            ) : (
              <li className="rounded-lg border border-dashed border-slate-200 px-3 py-4 text-sm text-slate-500">沒有低利潤或虧損訂單。</li>
            )}
          </ul>
        </article>
      </div>

      <footer className="flex flex-col items-start justify-between gap-3 rounded-xl border border-slate-200 bg-white p-4 shadow-sm md:flex-row md:items-center">
        <p className="text-sm text-slate-600">
          本次區間共 {summary?.filteredOrders.length || 0} 筆，預定 {summary?.statusCount.預定.count || 0} 筆，現貨{" "}
          {summary?.statusCount.現貨.count || 0} 筆。
        </p>
        {syncMessage ? (
          <p
            className={`text-xs font-medium ${
              syncStage === "completed" ? "text-emerald-600" : "text-slate-600"
            }`}
          >
            {syncMessage}
          </p>
        ) : null}
        <button
          type="button"
          onClick={() => void handleSyncClick()}
          disabled={!canSync}
          className="rounded-lg bg-emerald-600 px-5 py-2.5 text-sm font-semibold text-white transition hover:bg-emerald-700 active:scale-95 disabled:cursor-not-allowed disabled:bg-slate-300"
        >
          {isSyncing ? "同步中..." : "確認數據無誤，批次同步至雲端"}
        </button>
      </footer>
    </section>
  );
}

export default SyncPreviewDashboard;

