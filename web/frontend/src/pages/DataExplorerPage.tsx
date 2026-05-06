import { useEffect, useState, useCallback } from "react";
import { useTranslation } from "react-i18next";
import { Card } from "../components/ui/Card";
import { Tabs } from "../components/ui/Tabs";
import { Badge } from "../components/ui/Badge";
import { Table } from "../components/ui/Table";
import { Select } from "../components/ui/Select";
import { SearchInput } from "../components/ui/SearchInput";
import { DatePicker } from "../components/ui/DatePicker";
import { MultiSelect } from "../components/ui/MultiSelect";
import { EChartsWrapper } from "../components/ui/EChartsWrapper";
import { get, del, searchStocks, fetchStockQuotes, fetchSectors, fetchSectorStocks, fetchAltData, fetchFactorValues } from "../api/client";
import type { StockQuote, SectorInfo, AltDataResponse } from "../api/types";

const DATA_TABS = [
  { key: "quotes", label: "Stock Quotes" },
  { key: "sectors", label: "Sectors" },
  { key: "altData", label: "Alt Data" },
  { key: "factors", label: "Factor Values" },
  { key: "cache", label: "Cache" },
];

const ALT_DATA_TYPES = [
  "northbound", "margin", "pledge", "insider", "analyst",
  "shareholder", "dividend", "valuation", "balance_sheet",
  "earnings_guidance", "institutional", "repurchase", "visit",
];

const OVERLAY_OPTIONS = [
  { value: "ma5", label: "MA5" },
  { value: "ma20", label: "MA20" },
  { value: "boll", label: "BOLL" },
  { value: "vwap", label: "VWAP" },
];

function computeMA(data: number[], period: number): (number | null)[] {
  return data.map((_, i) => {
    if (i < period - 1) return null;
    let sum = 0;
    for (let j = i - period + 1; j <= i; j++) sum += data[j];
    return sum / period;
  });
}

function StockQuotesTab() {
  const { t } = useTranslation();
  const [query, setQuery] = useState("");
  const [searchResults, setSearchResults] = useState<{ symbol: string; name: string }[]>([]);
  const [selectedSymbol, setSelectedSymbol] = useState("");
  const [selectedName, setSelectedName] = useState("");
  const [startDate, setStartDate] = useState("2024-01-01");
  const [endDate, setEndDate] = useState("");
  const [quotes, setQuotes] = useState<StockQuote[]>([]);
  const [overlays, setOverlays] = useState<string[]>(["ma20"]);
  const [loading, setLoading] = useState(false);

  const handleSearch = useCallback((q: string) => {
    setQuery(q);
    if (q.length >= 2) {
      searchStocks(q, 8).then(setSearchResults).catch(() => setSearchResults([]));
    } else {
      setSearchResults([]);
    }
  }, []);

  const selectStock = (symbol: string, name: string) => {
    setSelectedSymbol(symbol);
    setSelectedName(name);
    setSearchResults([]);
    setLoading(true);
    fetchStockQuotes(symbol, { start: startDate, end: endDate || undefined })
      .then((res) => setQuotes(res.data || []))
      .catch(() => setQuotes([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    if (selectedSymbol) {
      setLoading(true);
      fetchStockQuotes(selectedSymbol, { start: startDate, end: endDate || undefined })
        .then((res) => setQuotes(res.data || []))
        .catch(() => setQuotes([]))
        .finally(() => setLoading(false));
    }
  }, [startDate, endDate, selectedSymbol]);

  const lastQuote = quotes[quotes.length - 1];

  const chartOption = quotes.length > 0 ? {
    tooltip: { trigger: "axis", axisPointer: { type: "cross" } },
    legend: { data: ["K线", "Volume", ...overlays.map(o => o.toUpperCase())], textStyle: { color: "#9ca3af" } },
    grid: [
      { left: 60, right: 20, top: 40, height: "55%" },
      { left: 60, right: 20, top: "72%", height: "18%" },
    ],
    xAxis: [
      { type: "category", data: quotes.map(q => q.date), gridIndex: 0, axisLine: { lineStyle: { color: "#374151" } }, axisLabel: { color: "#6b7280", fontSize: 10 } },
      { type: "category", data: quotes.map(q => q.date), gridIndex: 1, axisLabel: { show: false } },
    ],
    yAxis: [
      { type: "value", gridIndex: 0, scale: true, splitLine: { lineStyle: { color: "#1f2937" } }, axisLabel: { color: "#6b7280", fontSize: 10 } },
      { type: "value", gridIndex: 1, scale: true, splitLine: { show: false }, axisLabel: { show: false } },
    ],
    dataZoom: [
      { type: "inside", xAxisIndex: [0, 1], start: Math.max(0, 100 - (200 / quotes.length) * 100) },
      { type: "slider", xAxisIndex: [0, 1], bottom: 10, height: 16, borderColor: "#374151", fillerColor: "rgba(59,130,246,0.2)", handleStyle: { color: "#60a5fa" } },
    ],
    series: [
      {
        name: "K线",
        type: "candlestick",
        xAxisIndex: 0,
        yAxisIndex: 0,
        data: quotes.map(q => [q.open, q.close, q.low, q.high]),
        itemStyle: { color: "#10b981", color0: "#ef4444", borderColor: "#10b981", borderColor0: "#ef4444" },
      },
      {
        name: "Volume",
        type: "bar",
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: quotes.map(q => [q.volume, q.close >= q.open ? "#10b981" : "#ef4444"]),
        itemStyle: { color: (params: any) => params.data?.[1] || "#10b981" },
        encode: { y: 0 },
      },
      ...(overlays.includes("ma5") ? [{
        name: "MA5",
        type: "line",
        xAxisIndex: 0,
        yAxisIndex: 0,
        data: computeMA(quotes.map(q => q.close), 5),
        smooth: true,
        lineStyle: { width: 1, color: "#f59e0b" },
        symbol: "none",
      }] : []),
      ...(overlays.includes("ma20") ? [{
        name: "MA20",
        type: "line",
        xAxisIndex: 0,
        yAxisIndex: 0,
        data: computeMA(quotes.map(q => q.close), 20),
        smooth: true,
        lineStyle: { width: 1, color: "#8b5cf6" },
        symbol: "none",
      }] : []),
    ],
  } : undefined;

  return (
    <div className="flex gap-4">
      {/* Left sidebar */}
      <div className="w-72 space-y-4 shrink-0">
        <Card>
          <p className="text-xs text-zinc-500 uppercase mb-2">{t("dataExplorer.search")}</p>
          <SearchInput value={query} onChange={handleSearch} placeholder="600519 / 茅台" />
          {searchResults.length > 0 && (
            <div className="mt-2 bg-zinc-800 border border-zinc-700 rounded-md max-h-48 overflow-auto">
              {searchResults.map((r) => (
                <button
                  key={r.symbol}
                  onClick={() => selectStock(r.symbol, r.name)}
                  className="w-full px-3 py-2 text-left hover:bg-zinc-700 text-sm flex justify-between"
                >
                  <span className="text-zinc-300">{r.symbol}</span>
                  <span className="text-zinc-500">{r.name}</span>
                </button>
              ))}
            </div>
          )}
        </Card>
        <Card>
          <p className="text-xs text-zinc-500 uppercase mb-2">{t("dataExplorer.dateRange")}</p>
          <div className="flex gap-2">
            <DatePicker value={startDate} onChange={setStartDate} className="flex-1" />
            <DatePicker value={endDate} onChange={setEndDate} className="flex-1" />
          </div>
        </Card>
        <Card>
          <p className="text-xs text-zinc-500 uppercase mb-2">{t("dataExplorer.overlays")}</p>
          <MultiSelect
            options={OVERLAY_OPTIONS}
            values={overlays}
            onChange={setOverlays}
            placeholder="Select overlays..."
          />
        </Card>
        {lastQuote && (
          <Card title={selectedName || selectedSymbol}>
            <div className="space-y-1 text-sm">
              <div className="flex justify-between"><span className="text-zinc-500">Open</span><span className="text-zinc-300">{lastQuote.open?.toFixed(2)}</span></div>
              <div className="flex justify-between"><span className="text-zinc-500">High</span><span className="text-emerald-400">{lastQuote.high?.toFixed(2)}</span></div>
              <div className="flex justify-between"><span className="text-zinc-500">Low</span><span className="text-red-400">{lastQuote.low?.toFixed(2)}</span></div>
              <div className="flex justify-between"><span className="text-zinc-500">Close</span><span className="text-zinc-200">{lastQuote.close?.toFixed(2)}</span></div>
              <div className="flex justify-between"><span className="text-zinc-500">Volume</span><span className="text-zinc-400">{(lastQuote.volume / 1e4).toFixed(0)}万</span></div>
              <div className="flex justify-between"><span className="text-zinc-500">Change</span>
                <span className={lastQuote.change >= 0 ? "text-emerald-400" : "text-red-400"}>
                  {(lastQuote.change * 100).toFixed(2)}%
                </span>
              </div>
            </div>
          </Card>
        )}
      </div>

      {/* Right main chart area */}
      <div className="flex-1">
        {loading && <p className="text-zinc-500 text-sm">{t("common.loading")}</p>}
        {!loading && chartOption && <EChartsWrapper option={chartOption} height={520} />}
        {!loading && !chartOption && selectedSymbol && <p className="text-zinc-500 text-sm">{t("dataExplorer.noData")}</p>}
        {!selectedSymbol && (
          <div className="flex items-center justify-center h-96 text-zinc-600 text-sm">
            {t("dataExplorer.searchHint")}
          </div>
        )}
      </div>
    </div>
  );
}

function SectorsTab() {
  const { t } = useTranslation();
  const [sectors, setSectors] = useState<SectorInfo[]>([]);
  const [selectedSector, setSelectedSector] = useState<string | null>(null);
  const [sectorStocks, setSectorStocks] = useState<{ symbol: string; name: string }[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchSectors()
      .then(setSectors)
      .catch(() => setSectors([]))
      .finally(() => setLoading(false));
  }, []);

  const handleSectorClick = (row: Record<string, unknown>) => {
    const id = row.sector_id as string;
    setSelectedSector(id);
    fetchSectorStocks(id)
      .then((res) => setSectorStocks(res.stocks || []))
      .catch(() => setSectorStocks([]));
  };

  if (loading) return <p className="text-zinc-500 text-sm">{t("common.loading")}</p>;

  return (
    <div className="flex gap-4">
      <div className="flex-1">
        <Table
          columns={[
            { key: "sector_name", label: "Sector", sortable: true },
            { key: "stock_count", label: "Stocks", align: "right", sortable: true },
          ]}
          data={sectors as unknown as Record<string, unknown>[]}
          onRowClick={handleSectorClick}
          pageSize={20}
        />
      </div>
      {selectedSector && (
        <div className="w-72">
          <Card title={selectedSector}>
            {sectorStocks.length > 0 ? (
              <div className="max-h-96 overflow-auto space-y-1">
                {sectorStocks.map((s) => (
                  <div key={s.symbol} className="flex justify-between text-sm px-2 py-1 hover:bg-zinc-800 rounded">
                    <span className="text-zinc-300 font-mono text-xs">{s.symbol}</span>
                    <span className="text-zinc-500">{s.name}</span>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-zinc-500 text-sm">{t("common.noData")}</p>
            )}
          </Card>
        </div>
      )}
    </div>
  );
}

function AltDataTab() {
  const { t } = useTranslation();
  const [dataType, setDataType] = useState("northbound");
  const [symbol, setSymbol] = useState("");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [data, setData] = useState<AltDataResponse | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchData = () => {
    setLoading(true);
    fetchAltData(dataType, { symbol: symbol || undefined, start: startDate || undefined, end: endDate || undefined })
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  };

  return (
    <div className="space-y-4">
      <div className="flex gap-3 items-end">
        <div className="w-48">
          <p className="text-xs text-zinc-500 uppercase mb-1">Data Type</p>
          <Select
            options={ALT_DATA_TYPES.map(t => ({ value: t, label: t }))}
            value={dataType}
            onChange={setDataType}
          />
        </div>
        <div className="w-48">
          <p className="text-xs text-zinc-500 uppercase mb-1">Symbol</p>
          <SearchInput value={symbol} onChange={setSymbol} placeholder="Filter symbol..." />
        </div>
        <DatePicker value={startDate} onChange={setStartDate} />
        <DatePicker value={endDate} onChange={setEndDate} />
        <button
          onClick={fetchData}
          className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
        >
          {t("common.search")}
        </button>
      </div>

      {loading && <p className="text-zinc-500 text-sm">{t("common.loading")}</p>}
      {data && data.rows.length > 0 && (
        <Card>
          <p className="text-xs text-zinc-500 mb-2">
            {data.total} rows ({data.columns.length} cols) {data.has_more ? "(showing first 100)" : ""}
          </p>
          <div className="overflow-x-auto max-h-96 overflow-y-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-zinc-700 sticky top-0 bg-zinc-900">
                  {data.columns.map((col) => (
                    <th key={col} className="px-3 py-2 text-left text-xs text-zinc-400 uppercase">{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.rows.map((row, i) => (
                  <tr key={i} className="border-b border-zinc-800 hover:bg-zinc-800/50">
                    {data.columns.map((col) => (
                      <td key={col} className="px-3 py-1 text-zinc-300 text-xs">{row[col] ?? "-"}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}
      {data && data.rows.length === 0 && <p className="text-zinc-500 text-sm">{t("common.noData")}</p>}
    </div>
  );
}

function FactorValuesTab() {
  const { t } = useTranslation();
  const [factorList, setFactorList] = useState<{ value: string; label: string }[]>([]);
  const [selectedFactors, setSelectedFactors] = useState<string[]>([]);
  const [symbols, setSymbols] = useState("");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [data, setData] = useState<{ factors: string[]; data: Record<string, unknown>[] } | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    get<{ name: string }[]>("/factors")
      .then((factors) => setFactorList(factors.map(f => ({ value: f.name, label: f.name }))))
      .catch(() => setFactorList([]));
  }, []);

  const fetchValues = () => {
    if (selectedFactors.length === 0) return;
    setLoading(true);
    fetchFactorValues({
      factors: selectedFactors.join(","),
      symbols: symbols || undefined,
      start: startDate || undefined,
      end: endDate || undefined,
    })
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  };

  const columns = data?.factors ? ["symbol", "date", ...data.factors] : [];

  return (
    <div className="space-y-4">
      <div className="flex gap-3 items-end">
        <div className="w-64">
          <p className="text-xs text-zinc-500 uppercase mb-1">Factors</p>
          <MultiSelect options={factorList} values={selectedFactors} onChange={setSelectedFactors} placeholder="Select factors..." />
        </div>
        <div className="w-48">
          <p className="text-xs text-zinc-500 uppercase mb-1">Symbols</p>
          <SearchInput value={symbols} onChange={setSymbols} placeholder="SH600519,SZ000001" />
        </div>
        <DatePicker value={startDate} onChange={setStartDate} />
        <DatePicker value={endDate} onChange={setEndDate} />
        <button
          onClick={fetchValues}
          disabled={selectedFactors.length === 0}
          className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-30"
        >
          Query
        </button>
      </div>

      {loading && <p className="text-zinc-500 text-sm">{t("common.loading")}</p>}
      {data && data.data.length > 0 && (
        <Card>
          <p className="text-xs text-zinc-500 mb-2">{data.data.length} rows</p>
          <div className="overflow-x-auto max-h-96 overflow-y-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-zinc-700 sticky top-0 bg-zinc-900">
                  {columns.map((col) => (
                    <th key={col} className="px-3 py-2 text-left text-xs text-zinc-400 uppercase">{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.data.slice(0, 200).map((row, i) => (
                  <tr key={i} className="border-b border-zinc-800 hover:bg-zinc-800/50">
                    {columns.map((col) => (
                      <td key={col} className="px-3 py-1 text-zinc-300 text-xs">
                        {typeof row[col] === "number" ? (row[col] as number).toFixed(4) : row[col] ?? "-"}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </div>
  );
}

interface CacheEntry {
  type: string;
  file_count: number;
  total_size_mb: number;
  latest: string | null;
  ttl_days: number;
}

function CacheTab() {
  const { t } = useTranslation();
  const [cache, setCache] = useState<CacheEntry[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = () => {
    setLoading(true);
    get<CacheEntry[]>("/data/cache-status")
      .then(setCache)
      .catch(() => setCache([]))
      .finally(() => setLoading(false));
  };

  useEffect(refresh, []);

  const deleteExpired = (type: string) => {
    del<{ deleted: number }>(`/data/cache/${type}/expired`)
      .then(() => refresh())
      .catch(() => {});
  };

  if (loading) return <p className="text-zinc-500 text-sm">{t("common.loading")}</p>;

  return (
    <Table
      columns={[
        { key: "type", label: "Type", sortable: true },
        { key: "file_count", label: "Files", align: "right", sortable: true },
        { key: "total_size_mb", label: "Size (MB)", align: "right", sortable: true },
        { key: "latest", label: "Latest", render: (row) => (row.latest ? new Date(row.latest as string).toLocaleDateString() : "-") },
        { key: "ttl_days", label: "TTL (days)", align: "right" },
        { key: "actions", label: "", render: (row) => (
          <button onClick={() => deleteExpired(row.type as string)} className="px-2 py-1 text-xs bg-red-900/50 text-red-300 rounded hover:bg-red-800/50">
            {t("common.deleteExpired")}
          </button>
        )},
      ]}
      data={cache as unknown as Record<string, unknown>[]}
      pageSize={20}
    />
  );
}

export function DataExplorerPage() {
  const { t } = useTranslation();
  const [activeTab, setActiveTab] = useState("quotes");

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-zinc-100">{t("dataExplorer.title")}</h1>
        <Tabs tabs={DATA_TABS} activeKey={activeTab} onChange={setActiveTab} />
      </div>

      {activeTab === "quotes" && <StockQuotesTab />}
      {activeTab === "sectors" && <SectorsTab />}
      {activeTab === "altData" && <AltDataTab />}
      {activeTab === "factors" && <FactorValuesTab />}
      {activeTab === "cache" && <CacheTab />}
    </div>
  );
}
