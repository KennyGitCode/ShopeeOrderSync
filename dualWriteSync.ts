import {
  aggregateReport,
  type ReportOrder,
  type ReportSummary,
  type StatusCountSummary,
} from "./reportAggregator";

export interface SummarySnapshotRow {
  executedAt: string;
  startDate: string;
  endDate: string;
  totalOrders: number;
  totalRevenue: number;
  totalProfit: number;
  averageFeeRate: number;
  preorderCount: number;
  inStockCount: number;
  preorderCountShare: number;
  inStockCountShare: number;
  preorderRevenueShare: number;
  inStockRevenueShare: number;
}

export interface WriteOrderDetailsResult {
  writtenCount: number;
}

export interface WriteSummarySnapshotResult {
  writtenCount: number;
  snapshotId?: string;
}

export interface DualWriteApi {
  writeOrderDetails: (orders: ReportOrder[]) => Promise<WriteOrderDetailsResult>;
  writeSummarySnapshot: (row: SummarySnapshotRow) => Promise<WriteSummarySnapshotResult>;
  clearLocalUploadedData?: (orders: ReportOrder[]) => Promise<void> | void;
}

export type SyncStage =
  | "idle"
  | "aggregating"
  | "writing-details"
  | "writing-summary"
  | "clearing-local"
  | "completed";

export interface DualWriteSyncArgs {
  orders: ReportOrder[];
  startDate: string | Date | number;
  endDate: string | Date | number;
  api: DualWriteApi;
  setLoading?: (isLoading: boolean) => void;
  onStageChange?: (stage: SyncStage) => void;
}

export interface DualWriteSyncSuccess {
  ok: true;
  summary: ReportSummary;
  snapshot: SummarySnapshotRow;
  detailWrite: WriteOrderDetailsResult;
  summaryWrite: WriteSummarySnapshotResult;
}

export interface DualWriteSyncFailure {
  ok: false;
  stage: SyncStage;
  error: Error;
  userMessage: string;
}

export type DualWriteSyncResult = DualWriteSyncSuccess | DualWriteSyncFailure;

function toDateInput(value: string | Date | number): string {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toISOString().slice(0, 10);
}

function safeDivide(n: number, d: number): number {
  if (!Number.isFinite(n) || !Number.isFinite(d) || d === 0) return 0;
  return n / d;
}

function buildSummarySnapshot(
  summary: ReportSummary,
  startDate: string | Date | number,
  endDate: string | Date | number,
  executedAt = new Date(),
): SummarySnapshotRow {
  const status: StatusCountSummary = summary.statusCount;
  const totalOrders = status.totalCount;
  const preorderCount = status.預定.count;
  const inStockCount = status.現貨.count;
  return {
    executedAt: executedAt.toISOString(),
    startDate: toDateInput(startDate),
    endDate: toDateInput(endDate),
    totalOrders,
    totalRevenue: summary.totalRevenue,
    totalProfit: summary.totalProfit,
    averageFeeRate: summary.averageFeeRate,
    preorderCount,
    inStockCount,
    preorderCountShare: safeDivide(preorderCount, totalOrders),
    inStockCountShare: safeDivide(inStockCount, totalOrders),
    preorderRevenueShare: status.預定.revenueShare,
    inStockRevenueShare: status.現貨.revenueShare,
  };
}

function asError(err: unknown): Error {
  if (err instanceof Error) return err;
  return new Error(typeof err === "string" ? err : "Unknown error");
}

function buildFailure(stage: SyncStage, err: unknown): DualWriteSyncFailure {
  const error = asError(err);
  const userMessageByStage: Record<SyncStage, string> = {
    idle: "同步未開始。",
    aggregating: "結算資料彙整失敗，請檢查日期區間與訂單資料格式。",
    "writing-details": "訂單明細寫入失敗，請稍後重試。",
    "writing-summary": "結算摘要快照寫入失敗，請稍後重試。",
    "clearing-local": "雲端寫入成功，但清空本地資料失敗，請手動確認。",
    completed: "流程已完成。",
  };
  return {
    ok: false,
    stage,
    error,
    userMessage: userMessageByStage[stage],
  };
}

export async function runDualWriteSync({
  orders,
  startDate,
  endDate,
  api,
  setLoading,
  onStageChange,
}: DualWriteSyncArgs): Promise<DualWriteSyncResult> {
  let stage: SyncStage = "idle";
  setLoading?.(true);
  try {
    stage = "aggregating";
    onStageChange?.(stage);
    const summary = aggregateReport({ orders, startDate, endDate });
    const snapshot = buildSummarySnapshot(summary, startDate, endDate);

    stage = "writing-details";
    onStageChange?.(stage);
    const detailWrite = await api.writeOrderDetails(summary.filteredOrders);

    stage = "writing-summary";
    onStageChange?.(stage);
    const summaryWrite = await api.writeSummarySnapshot(snapshot);

    stage = "clearing-local";
    onStageChange?.(stage);
    await api.clearLocalUploadedData?.(summary.filteredOrders);

    stage = "completed";
    onStageChange?.(stage);
    return {
      ok: true,
      summary,
      snapshot,
      detailWrite,
      summaryWrite,
    };
  } catch (err) {
    return buildFailure(stage, err);
  } finally {
    setLoading?.(false);
  }
}

