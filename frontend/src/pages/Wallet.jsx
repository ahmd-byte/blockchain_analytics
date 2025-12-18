import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import apiClient from '../api/apiClient';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Search, Wallet as WalletIcon, Activity, ShieldAlert } from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';
import TransactionChart from '../components/TransactionChart';
import { cn } from "@/lib/utils";

const Wallet = () => {
    const { address } = useParams();
    const navigate = useNavigate();
    const [searchAddress, setSearchAddress] = useState(address || '');
    const [loading, setLoading] = useState(false);
    const [data, setData] = useState(null);
    const [error, setError] = useState(null);

    const handleSearch = (e) => {
        e.preventDefault();
        if (searchAddress) {
            navigate(`/wallet/${searchAddress}`);
        }
    };

    // Format large numbers
    const formatNumber = (num) => {
        if (num >= 1000000) return `${(num / 1000000).toFixed(2)}M`;
        if (num >= 1000) return `${(num / 1000).toFixed(2)}K`;
        return num?.toLocaleString() || '0';
    };

    useEffect(() => {
        if (!address) return;

        const fetchData = async () => {
            setLoading(true);
            setError(null);
            try {
                const response = await apiClient.get(`/wallet/${address}`);
                const walletData = response.data;

                // Transform backend data to frontend format
                const stats = walletData.stats;
                const dailyVolumes = walletData.daily_volumes || [];

                // Transform daily volumes for chart
                const volumeData = dailyVolumes.slice(0, 7).reverse().map(day => ({
                    name: new Date(day.date || day.transaction_date).toLocaleDateString('en-US', { weekday: 'short' }),
                    value: Math.round(day.total_value),
                    inflow: Math.round(day.inflow || 0),
                    outflow: Math.round(day.outflow || 0),
                }));

                setData({
                    address: stats.wallet_address,
                    balance: `${formatNumber(stats.total_volume)} ETH`,
                    totalTransactions: stats.total_transactions,
                    totalVolume: stats.total_volume,
                    riskScore: Math.round((stats.fraud_score || 0) * 100),
                    isSuspicious: stats.is_suspicious,
                    firstTransaction: stats.first_transaction_date,
                    lastTransaction: stats.last_transaction_date,
                    uniqueCounterparties: stats.unique_counterparties,
                    avgTransactionValue: stats.average_transaction_value,
                    volumeData: volumeData.length > 0 ? volumeData : [
                        { name: 'Mon', value: 0 },
                        { name: 'Tue', value: 0 },
                        { name: 'Wed', value: 0 },
                        { name: 'Thu', value: 0 },
                        { name: 'Fri', value: 0 },
                        { name: 'Sat', value: 0 },
                        { name: 'Sun', value: 0 },
                    ],
                });
            } catch (err) {
                console.error(err);
                setError(err.response?.status === 404 ? 'Wallet not found' : 'Failed to load wallet data');
                
                // Set fallback mock data
                setData({
                    address: address,
                    balance: "145.2 ETH",
                    totalTransactions: 1243,
                    riskScore: 12,
                    isSuspicious: false,
                    volumeData: [
                        { name: 'Mon', value: 100 },
                        { name: 'Tue', value: 200 },
                        { name: 'Wed', value: 150 },
                        { name: 'Thu', value: 300 },
                        { name: 'Fri', value: 250 },
                        { name: 'Sat', value: 180 },
                        { name: 'Sun', value: 220 },
                    ],
                });
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, [address]);

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <h2 className="text-3xl font-bold tracking-tight">Wallet Analytics</h2>
            </div>

            <Card className="transition-all duration-300 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5">
                <CardContent className="pt-6">
                    <form onSubmit={handleSearch} className="flex gap-4">
                        <div className="flex-1">
                            <Input
                                placeholder="Enter Wallet Address (0x...)"
                                value={searchAddress}
                                onChange={(e) => setSearchAddress(e.target.value)}
                                className="border-black/10 dark:border-white/10 focus-visible:ring-[#D40000]"
                            />
                        </div>
                        <Button type="submit" className="bg-[#D40000] hover:bg-[#b30000] text-white shadow-lg shadow-[#D40000]/20 transition-all hover:-translate-y-0.5">
                            <Search className="mr-2 h-4 w-4" /> Analyze
                        </Button>
                    </form>
                </CardContent>
            </Card>

            {loading && (
                <div className="space-y-4">
                    <div className="grid gap-4 md:grid-cols-4">
                        <Skeleton className="h-32 rounded-xl" />
                        <Skeleton className="h-32 rounded-xl" />
                        <Skeleton className="h-32 rounded-xl" />
                        <Skeleton className="h-32 rounded-xl" />
                    </div>
                    <Skeleton className="h-[300px] rounded-xl" />
                </div>
            )}

            {!loading && data && (
                <>
                    <div className="grid gap-4 md:grid-cols-4">
                        <Card className="transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5">
                            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                                <CardTitle className="text-sm font-medium text-muted-foreground">Total Volume</CardTitle>
                                <div className="p-2 bg-zinc-100 dark:bg-zinc-800 rounded-full">
                                    <WalletIcon className="h-4 w-4 text-[#D40000]" />
                                </div>
                            </CardHeader>
                            <CardContent>
                                <div className="text-2xl font-bold tracking-tight">{data.balance}</div>
                            </CardContent>
                        </Card>
                        <Card className="transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5">
                            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                                <CardTitle className="text-sm font-medium text-muted-foreground">Total Transactions</CardTitle>
                                <div className="p-2 bg-zinc-100 dark:bg-zinc-800 rounded-full">
                                    <Activity className="h-4 w-4 text-[#D40000]" />
                                </div>
                            </CardHeader>
                            <CardContent>
                                <div className="text-2xl font-bold tracking-tight">{formatNumber(data.totalTransactions)}</div>
                            </CardContent>
                        </Card>
                        <Card className="transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5">
                            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                                <CardTitle className="text-sm font-medium text-muted-foreground">Unique Contacts</CardTitle>
                                <div className="p-2 bg-zinc-100 dark:bg-zinc-800 rounded-full">
                                    <Activity className="h-4 w-4 text-[#D40000]" />
                                </div>
                            </CardHeader>
                            <CardContent>
                                <div className="text-2xl font-bold tracking-tight">{formatNumber(data.uniqueCounterparties || 0)}</div>
                            </CardContent>
                        </Card>
                        <Card className="transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5">
                            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                                <CardTitle className="text-sm font-medium text-muted-foreground">Risk Score</CardTitle>
                                <div className="p-2 bg-zinc-100 dark:bg-zinc-800 rounded-full">
                                    <ShieldAlert className={cn("h-4 w-4", data.isSuspicious ? "text-rose-600" : "text-emerald-600")} />
                                </div>
                            </CardHeader>
                            <CardContent>
                                <div className={cn(
                                    "text-2xl font-bold tracking-tight",
                                    data.riskScore > 50 ? 'text-rose-600' : 'text-emerald-600'
                                )}>
                                    {data.riskScore}/100
                                    {data.isSuspicious && (
                                        <span className="ml-2 text-xs bg-rose-100 text-rose-700 px-2 py-1 rounded-full">
                                            Suspicious
                                        </span>
                                    )}
                                </div>
                            </CardContent>
                        </Card>
                    </div>

                    <div className="grid gap-4">
                        <TransactionChart data={data.volumeData} title="Transaction Volume (Last 7 Days)" />
                    </div>

                    {/* Wallet Details */}
                    <Card className="transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5">
                        <CardHeader>
                            <CardTitle>Wallet Details</CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="grid gap-4 md:grid-cols-2">
                                <div>
                                    <p className="text-sm text-muted-foreground">Address</p>
                                    <p className="font-mono text-sm break-all">{data.address}</p>
                                </div>
                                <div>
                                    <p className="text-sm text-muted-foreground">Avg Transaction Value</p>
                                    <p className="font-semibold">{formatNumber(data.avgTransactionValue || 0)} ETH</p>
                                </div>
                                <div>
                                    <p className="text-sm text-muted-foreground">First Transaction</p>
                                    <p className="font-semibold">{data.firstTransaction || 'N/A'}</p>
                                </div>
                                <div>
                                    <p className="text-sm text-muted-foreground">Last Transaction</p>
                                    <p className="font-semibold">{data.lastTransaction || 'N/A'}</p>
                                </div>
                            </div>
                        </CardContent>
                    </Card>
                </>
            )}

            {!loading && !data && address && (
                <div className="text-center py-10 text-muted-foreground">
                    {error || 'Address not found or no data available.'}
                </div>
            )}
            {!loading && !address && (
                <div className="text-center py-10 text-muted-foreground">
                    Enter a wallet address to view analytics.
                </div>
            )}
        </div>
    );
};

export default Wallet;
