import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import apiClient from '../api/apiClient';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Search, Wallet as WalletIcon, ArrowUpRight, ArrowDownRight, Activity } from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';
import TransactionChart from '../components/TransactionChart';
import FraudTable from '../components/FraudTable';
import { cn } from "@/lib/utils";

const Wallet = () => {
    const { address } = useParams();
    const navigate = useNavigate();
    const [searchAddress, setSearchAddress] = useState(address || '');
    const [loading, setLoading] = useState(false);
    const [data, setData] = useState(null);

    const handleSearch = (e) => {
        e.preventDefault();
        if (searchAddress) {
            navigate(`/wallet/${searchAddress}`);
        }
    };

    useEffect(() => {
        if (!address) return;

        const fetchData = async () => {
            setLoading(true);
            try {
                // const response = await apiClient.get(`/wallet/${address}`);
                // setData(response.data);

                await new Promise(resolve => setTimeout(resolve, 800));
                setData({
                    address: address,
                    balance: "145.2 ETH",
                    totalTransactions: 1243,
                    riskScore: 12,
                    volumeData: [
                        { name: 'Jan', value: 100 },
                        { name: 'Feb', value: 200 },
                        { name: 'Mar', value: 150 },
                        { name: 'Apr', value: 300 },
                    ],
                    recentTransactions: [
                        { hash: "0xabc...123", from: address, to: "0xdef...456", amount: "1.2", riskScore: 10 },
                        { hash: "0xdef...456", from: "0xghi...789", to: address, amount: "5.0", riskScore: 5 },
                    ]
                });

            } catch (error) {
                console.error(error);
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
                    <div className="grid gap-4 md:grid-cols-3">
                        <Skeleton className="h-32 rounded-xl" />
                        <Skeleton className="h-32 rounded-xl" />
                        <Skeleton className="h-32 rounded-xl" />
                    </div>
                    <Skeleton className="h-[300px] rounded-xl" />
                </div>
            )}

            {!loading && data && (
                <>
                    <div className="grid gap-4 md:grid-cols-3">
                        <Card className="transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5">
                            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                                <CardTitle className="text-sm font-medium text-muted-foreground">Balance</CardTitle>
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
                                <div className="text-2xl font-bold tracking-tight">{data.totalTransactions}</div>
                            </CardContent>
                        </Card>
                        <Card className="transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5">
                            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                                <CardTitle className="text-sm font-medium text-muted-foreground">Risk Score</CardTitle>
                                <div className="p-2 bg-zinc-100 dark:bg-zinc-800 rounded-full">
                                    <Activity className="h-4 w-4 text-[#D40000]" />
                                </div>
                            </CardHeader>
                            <CardContent>
                                <div className={cn(
                                    "text-2xl font-bold tracking-tight",
                                    data.riskScore > 50 ? 'text-rose-600' : 'text-emerald-600'
                                )}>
                                    {data.riskScore}/100
                                </div>
                            </CardContent>
                        </Card>
                    </div>

                    <div className="grid gap-4 md:grid-cols-7">
                        <div className="col-span-4">
                            <TransactionChart data={data.volumeData} title="Wallet Activity" />
                        </div>
                        <div className="col-span-3">
                            <div className="rounded-xl border bg-card text-card-foreground shadow h-full transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5 bg-white dark:bg-zinc-950">
                                <div className="p-6 flex flex-col space-y-1.5 pb-2">
                                    <h3 className="font-semibold leading-none tracking-tight">Recent Transactions</h3>
                                </div>
                                <div className="p-6 pt-0">
                                    <FraudTable transactions={data.recentTransactions} />
                                </div>
                            </div>
                        </div>
                    </div>
                </>
            )}

            {!loading && !data && address && (
                <div className="text-center py-10 text-muted-foreground">
                    Address not found or no data available.
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
