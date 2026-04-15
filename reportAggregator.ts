export type OrderStatus = "預定" | "現貨";

export interface ReportOrder {
  商品名稱: string;
  訂單日期: string | Date | number;
  實售收入: number;
  單件成本: number;
  單件實扣手續費: number;
  最終利潤: number;
  來源判定: OrderStatus;
}

export interface RankedOrder {
  index: number;
  商品名稱: string;
  訂單日期: string;
  實售收入: number;
  單件成本: number;
  單件實扣手續費: number;
  最終利潤: number;
  利潤率: number;
  手續費率: number;
  來源判定: OrderStatus;
}

export interface StatusCountItem {
  count: number;
  revenue: number;
  revenueShare: number;
}

export interface StatusCountSummary {
  預定: StatusCountItem;
  現貨: StatusCountItem;
  totalCount: number;
}

export interface ReportSummary {
  filteredOrders: ReportOrder[];
  totalRevenue: number;
  totalProfit: number;
  averageFeeRate: number; // totalFee / totalRevenue
  topMVP: RankedOrder[];
  feeAssassins: RankedOrder[];
  lowProfitWarnings: RankedOrder[];
  statusCount: StatusCountSummary;
}

const EMPTY_STATUS: StatusCountSummary = {
  預定: { count: 0, revenue: 0, revenueShare: 0 },
  現貨: { count: 0, revenue: 0, revenueShare: 0 },
  totalCount: 0,
};

function toDate(value: ReportOrder["訂單日期"]): Date | null {
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function toIsoDate(value: ReportOrder["訂單日期"]): string {
  const d = toDate(value);
  if (!d) return "";
  return d.toISOString().slice(0, 10);
}

function inRange(date: Date, start: Date, end: Date): boolean {
  return date.getTime() >= start.getTime() && date.getTime() <= end.getTime();
}

function safeDivide(numerator: number, denominator: number): number {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator === 0) {
    return 0;
  }
  return numerator / denominator;
}

function toRankedOrder(order: ReportOrder, index: number): RankedOrder {
  const feeRate = safeDivide(order.單件實扣手續費, order.實售收入);
  const profitRate = safeDivide(order.最終利潤, order.實售收入);
  return {
    index,
    商品名稱: order.商品名稱,
    訂單日期: toIsoDate(order.訂單日期),
    實售收入: order.實售收入,
    單件成本: order.單件成本,
    單件實扣手續費: order.單件實扣手續費,
    最終利潤: order.最終利潤,
    利潤率: profitRate,
    手續費率: feeRate,
    來源判定: order.來源判定,
  };
}

export interface AggregateReportArgs {
  orders: ReportOrder[];
  startDate: string | Date | number;
  endDate: string | Date | number;
}

export function aggregateReport({
  orders,
  startDate,
  endDate,
}: AggregateReportArgs): ReportSummary {
  const start = toDate(startDate);
  const end = toDate(endDate);

  if (!start || !end) {
    throw new Error("Invalid startDate or endDate.");
  }
  if (start.getTime() > end.getTime()) {
    throw new Error("startDate must be less than or equal to endDate.");
  }

  const filteredOrders = orders.filter((o) => {
    const d = toDate(o.訂單日期);
    return d ? inRange(d, start, end) : false;
  });

  const totalRevenue = filteredOrders.reduce((sum, o) => sum + o.實售收入, 0);
  const totalProfit = filteredOrders.reduce((sum, o) => sum + o.最終利潤, 0);
  const totalFee = filteredOrders.reduce((sum, o) => sum + o.單件實扣手續費, 0);
  const averageFeeRate = safeDivide(totalFee, totalRevenue);

  const ranked = filteredOrders.map((o, idx) => toRankedOrder(o, idx));

  const topMVP = [...ranked]
    .sort((a, b) => b.最終利潤 - a.最終利潤)
    .slice(0, 3);

  const feeAssassins = [...ranked]
    .sort((a, b) => b.手續費率 - a.手續費率)
    .slice(0, 3);

  const lowProfitWarnings = ranked.filter(
    (r) => r.最終利潤 < 0 || r.利潤率 < 0.05,
  );

  const statusCount: StatusCountSummary = JSON.parse(JSON.stringify(EMPTY_STATUS));
  statusCount.totalCount = filteredOrders.length;
  for (const order of filteredOrders) {
    statusCount[order.來源判定].count += 1;
    statusCount[order.來源判定].revenue += order.實售收入;
  }
  statusCount.預定.revenueShare = safeDivide(statusCount.預定.revenue, totalRevenue);
  statusCount.現貨.revenueShare = safeDivide(statusCount.現貨.revenue, totalRevenue);

  return {
    filteredOrders,
    totalRevenue,
    totalProfit,
    averageFeeRate,
    topMVP,
    feeAssassins,
    lowProfitWarnings,
    statusCount,
  };
}

