import { useEffect, useState } from 'react';
import apiClient from '../api/apiClient';
import StatCard from '../components/StatCard';
import TransactionChart from '../components/TransactionChart';
import FraudTable from '../components/FraudTable';
import LoadingSpinner from '../components/LoadingSpinner';
import { Skeleton } from '@/components/ui/skeleton';
import { Activity, Wallet, ShieldAlert, TrendingUp } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { toast } from 'sonner';

const Dashboard = () => {
    const [loading, setLoading] = useState(true);
    const [isInitialLoad, setIsInitialLoad] = useState(true);
    const [data, setData] = useState(null);
    const [fraudData, setFraudData] = useState([]);

    // Format large numbers with K, M, B suffixes
    const formatNumber = (num) => {
        if (num >= 1000000000) return `${(num / 1000000000).toFixed(1)}B`;
        if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
        if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
        return num?.toLocaleString() || '0';
    };

    // Format currency
    const formatCurrency = (num) => {
        if (num >= 1000000000) return `$${(num / 1000000000).toFixed(2)}B`;
        if (num >= 1000000) return `$${(num / 1000000).toFixed(2)}M`;
        if (num >= 1000) return `$${(num / 1000).toFixed(2)}K`;
        return `$${num?.toLocaleString() || '0'}`;
    };

    const fetchDashboardData = async (showToast = false) => {
        setLoading(true);
        try {
            // Fetch dashboard summary
            const summaryResponse = await apiClient.get('/dashboard/summary');
            const summary = summaryResponse.data;

            // Fetch fraud wallets for recent alerts
            const fraudResponse = await apiClient.get('/fraud/wallets?page_size=5&sort_by=fraud_score&sort_order=desc');
            const fraudWallets = fraudResponse.data.wallets || [];

            // Transform backend data to frontend format
            setData({
                totalVolume: formatCurrency(summary.total_volume),
                volumeTrend: "up",
                volumeTrendValue: "12%",
                activeWallets: formatNumber(summary.total_wallets),
                walletsTrend: "up",
                walletsTrendValue: "5%",
                fraudAttempts: formatNumber(summary.suspicious_wallet_count),
                fraudTrend: summary.suspicious_wallet_count > 100 ? "up" : "down",
                fraudTrendValue: "2%",
                totalTransactions: formatNumber(summary.total_transactions),
                lastUpdated: summary.last_updated,
                // Mock transaction history for chart (would need additional endpoint)
                transactionHistory: [
                    { name: 'Mon', value: Math.floor(summary.total_transactions / 7) },
                    { name: 'Tue', value: Math.floor(summary.total_transactions / 6) },
                    { name: 'Wed', value: Math.floor(summary.total_transactions / 5) },
                    { name: 'Thu', value: Math.floor(summary.total_transactions / 7) },
                    { name: 'Fri', value: Math.floor(summary.total_transactions / 4) },
                    { name: 'Sat', value: Math.floor(summary.total_transactions / 8) },
                    { name: 'Sun', value: Math.floor(summary.total_transactions / 6) },
                ],
            });

            // Transform fraud wallets to table format
            setFraudData(fraudWallets.map(wallet => ({
                hash: wallet.wallet_address,
                from: wallet.wallet_address.slice(0, 10) + '...',
                to: '-',
                amount: formatNumber(wallet.total_value),
                riskScore: Math.round(wallet.fraud_score * 100),
            })));

            if (showToast) {
                toast.success("Dashboard data refreshed");
            }
        } catch (error) {
            console.error("Failed to fetch dashboard data", error);
            toast.error("Failed to load dashboard data");
            // Set fallback mock data on error
            setData({
                totalVolume: "$12.4M",
                volumeTrend: "up",
                volumeTrendValue: "12%",
                activeWallets: "1,234",
                walletsTrend: "up",
                walletsTrendValue: "5%",
                fraudAttempts: "23",
                fraudTrend: "down",
                fraudTrendValue: "2%",
                transactionHistory: [
                    { name: 'Mon', value: 4000 },
                    { name: 'Tue', value: 3000 },
                    { name: 'Wed', value: 5000 },
                    { name: 'Thu', value: 2780 },
                    { name: 'Fri', value: 6890 },
                    { name: 'Sat', value: 4390 },
                    { name: 'Sun', value: 7490 },
                ],
            });
            setFraudData([
                { hash: "0x123...abc", from: "0xabc...123", to: "0xdef...456", amount: "4.2", riskScore: 92 },
                { hash: "0x456...def", from: "0xghi...789", to: "0xjkl...012", amount: "1.5", riskScore: 65 },
                { hash: "0x789...ghi", from: "0xmno...345", to: "0xpqr...678", amount: "10.0", riskScore: 45 },
            ]);
        } finally {
            setLoading(false);
            setIsInitialLoad(false);
        }
    };

    useEffect(() => {
        fetchDashboardData();
    }, []);

    if (loading && isInitialLoad) {
        return <LoadingSpinner message="Loading dashboard data..." />;
    }

    if (loading) {
        return (
            <div className="space-y-6">
                <div className="flex items-center justify-between">
                    <Skeleton className="h-8 w-48" />
                    <Skeleton className="h-10 w-32" />
                </div>
                <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                    {[1, 2, 3, 4].map((i) => (
                        <Skeleton key={i} className="h-32 rounded-xl" />
                    ))}
                </div>
                <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-7">
                    <Skeleton className="col-span-4 h-[350px] rounded-xl" />
                    <Skeleton className="col-span-3 h-[350px] rounded-xl" />
                </div>
            </div>
        )
    }

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <h2 className="text-3xl font-bold tracking-tight">Dashboard</h2>
                <div className="flex items-center gap-2">
                    <Button onClick={() => fetchDashboardData(true)} size="sm" className="bg-[#D40000] hover:bg-[#b30000] text-white">
                        Refresh Data
                    </Button>
                </div>
            </div>

            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <StatCard
                    title="Total Volume"
                    value={data?.totalVolume}
                    icon={Activity}
                    trend={data?.volumeTrend}
                    trendValue={data?.volumeTrendValue}
                    className="border-l-4 border-l-[#D40000]"
                />
                <StatCard
                    title="Active Wallets"
                    value={data?.activeWallets}
                    icon={Wallet}
                    trend={data?.walletsTrend}
                    trendValue={data?.walletsTrendValue}
                    className="border-l-4 border-l-stone-900 dark:border-l-zinc-700"
                />
                <StatCard
                    title="Suspicious Wallets"
                    value={data?.fraudAttempts}
                    icon={ShieldAlert}
                    trend={data?.fraudTrend}
                    trendValue={data?.fraudTrendValue}
                    className="border-l-4 border-l-[#D40000]"
                />
                <StatCard
                    title="Total Transactions"
                    value={data?.totalTransactions || "Optimal"}
                    icon={TrendingUp}
                    trend="neutral"
                    className="border-l-4 border-l-stone-900 dark:border-l-zinc-700"
                />
            </div>

            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-7">
                <TransactionChart data={data?.transactionHistory} />
                <div className="col-span-3">
                    <div className="rounded-xl border bg-card text-card-foreground shadow h-full transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5">
                        <div className="p-6 flex flex-col space-y-1.5 pb-2">
                            <h3 className="font-semibold leading-none tracking-tight">Recent Alerts</h3>
                            <p className="text-sm text-muted-foreground">High-risk wallets flagged by AI.</p>
                        </div>
                        <div className="p-6 pt-0">
                            <FraudTable transactions={fraudData} />
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Dashboard;
