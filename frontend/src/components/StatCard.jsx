import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { ArrowUpRight, ArrowDownRight, Minus } from "lucide-react";

const StatCard = ({ title, value, icon: Icon, trend, trendValue, className }) => {
    return (
        <Card className={cn(
            "transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5",
            className
        )}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">
                    {title}
                </CardTitle>
                {Icon && (
                    <div className="p-2 bg-zinc-100 dark:bg-zinc-800 rounded-full">
                        <Icon className="h-4 w-4 text-[#D40000]" />
                    </div>
                )}
            </CardHeader>
            <CardContent>
                <div className="text-2xl font-bold tracking-tight">{value}</div>
                {(trend || trendValue) && (
                    <div className="flex items-center text-xs mt-1">
                        {trend === 'up' && <ArrowUpRight className="h-4 w-4 text-emerald-500 mr-1" />}
                        {trend === 'down' && <ArrowDownRight className="h-4 w-4 text-rose-500 mr-1" />}
                        {trend === 'neutral' && <Minus className="h-4 w-4 text-zinc-400 mr-1" />}

                        <span className={cn(
                            "font-medium",
                            trend === 'up' && "text-emerald-600",
                            trend === 'down' && "text-rose-600",
                            trend === 'neutral' && "text-zinc-500"
                        )}>
                            {trendValue}
                        </span>
                        {trend !== 'neutral' && <span className="text-muted-foreground ml-1">from last month</span>}
                    </div>
                )}
            </CardContent>
        </Card>
    );
};

export default StatCard;
