import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

const TransactionChart = ({ data, title = "Transaction Volume", className }) => {
    if (!data || data.length === 0) {
        // Mock data for display if empty
        data = [
            { name: 'Mon', value: 400 },
            { name: 'Tue', value: 300 },
            { name: 'Wed', value: 300 },
            { name: 'Thu', value: 200 },
            { name: 'Fri', value: 278 },
            { name: 'Sat', value: 189 },
            { name: 'Sun', value: 239 },
        ];
    }

    return (
        <Card className={cn(
            "min-w-0 col-span-4 transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5",
            className
        )}>
            <CardHeader>
                <CardTitle>{title}</CardTitle>
            </CardHeader>
            <CardContent className="pl-2">
                <div style={{ width: '100%', height: 200, minWidth: 0 }}>
                    <ResponsiveContainer width="100%" height="100%" debounce={1}>
                        <AreaChart data={data}>
                            <defs>
                                <linearGradient id="colorValue" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="5%" stopColor="#D40000" stopOpacity={0.2} />
                                    <stop offset="95%" stopColor="#D40000" stopOpacity={0} />
                                </linearGradient>
                            </defs>
                            <CartesianGrid strokeDasharray="3 3" className="stroke-muted/50" vertical={false} />
                            <XAxis
                                dataKey="name"
                                stroke="#888888"
                                fontSize={12}
                                tickLine={false}
                                axisLine={false}
                            />
                            <YAxis
                                stroke="#888888"
                                fontSize={12}
                                tickLine={false}
                                axisLine={false}
                                tickFormatter={(value) => `$${value}`}
                            />
                            <Tooltip
                                contentStyle={{
                                    backgroundColor: 'rgba(255, 255, 255, 0.8)',
                                    borderRadius: '12px',
                                    border: 'none',
                                    boxShadow: '0 4px 12px rgba(0, 0, 0, 0.1)',
                                    backdropFilter: 'blur(8px)'
                                }}
                                itemStyle={{ color: '#1a1a1a' }}
                            />
                            <Area
                                type="monotone"
                                dataKey="value"
                                stroke="#D40000"
                                strokeWidth={2}
                                fillOpacity={1}
                                fill="url(#colorValue)"
                            />
                        </AreaChart>
                    </ResponsiveContainer>
                </div>
            </CardContent>
        </Card>
    );
};

export default TransactionChart;
