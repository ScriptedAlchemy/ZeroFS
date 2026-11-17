import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import { statsRates, type RateMetric } from "../../lib/stats-rates";
import type { StatsSnapshot } from "../../lib/grpc/gen/admin_pb";
import { formatSize } from "../../lib/format";

interface IOChartProps {
  history: StatsSnapshot[];
  metric: RateMetric;
}

export function IOChart({ history, metric }: IOChartProps) {
  const fontFamily = "'Geist Mono', 'SF Mono', ui-monospace, monospace";
  const data = statsRates(history, metric);
  const title = metric === "throughput" ? "Throughput" : "IOPS";
  const formatRate = (rate: number) => {
    if (metric === "iops") return `${Number(rate.toFixed(2))} ops/s`;
    return rate > 0 && rate < 1 ? `${Number(rate.toPrecision(2))} B/s` : `${formatSize(rate)}/s`;
  };
  const formatTime = (time: number) => new Date(time).toLocaleTimeString();

  return (
    <div className="card-surface rounded-lg p-5">
      <div className="flex items-center justify-between mb-4">
        <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">{title}</p>
        <div className="flex items-center gap-4 text-xs">
          <span className="flex items-center gap-1.5">
            <span className="w-2.5 h-0.5 rounded-full bg-blue-400 inline-block" />
            <span className="text-muted">Read</span>
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-2.5 h-0.5 rounded-full bg-emerald-400 inline-block" />
            <span className="text-muted">Write</span>
          </span>
        </div>
      </div>
      {data.some((p) => p.read !== null || p.write !== null) ? (
        <ResponsiveContainer width="100%" height={200} style={{ marginLeft: -10 }}>
          <AreaChart data={data} margin={{ top: 0, right: 0, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id={`read-${metric}`} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#60a5fa" stopOpacity={0.15} />
                <stop offset="100%" stopColor="#60a5fa" stopOpacity={0} />
              </linearGradient>
              <linearGradient id={`write-${metric}`} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#34d399" stopOpacity={0.15} />
                <stop offset="100%" stopColor="#34d399" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#3d444d" vertical={false} />
            <XAxis
              dataKey="time"
              type="number"
              domain={["dataMin", "dataMax"]}
              tick={{ fontSize: 10, fill: "#656c76", fontFamily }}
              axisLine={false}
              tickLine={false}
              tickCount={5}
              tickFormatter={formatTime}
            />
            <YAxis
              tickFormatter={formatRate}
              width={70}
              tick={{ fontSize: 10, fill: "#656c76", fontFamily }}
              axisLine={false}
              tickLine={false}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "#2a313c",
                border: "1px solid #3d444d",
                borderRadius: 6,
                fontSize: 11,
                fontFamily,
                boxShadow: "0 4px 24px rgba(0,0,0,0.3)",
              }}
              formatter={(value) => formatRate(Number(value))}
              labelFormatter={(value) => formatTime(Number(value))}
            />
            <Area
              type="monotone"
              dataKey="read"
              name="Read"
              stroke="#60a5fa"
              fill={`url(#read-${metric})`}
              strokeWidth={1.5}
              dot={data.length === 2}
              isAnimationActive={false}
            />
            <Area
              type="monotone"
              dataKey="write"
              name="Write"
              stroke="#34d399"
              fill={`url(#write-${metric})`}
              strokeWidth={1.5}
              dot={data.length === 2}
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      ) : (
        <p className="h-[200px] flex items-center justify-center text-sm text-muted-foreground">
          Waiting for two timestamped samples.
        </p>
      )}
    </div>
  );
}
