import { useState, useEffect } from 'react';
import apiClient from '../api/apiClient';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from '@/components/ui/skeleton';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { toast } from 'sonner';
import { ChevronLeft, ChevronRight, RefreshCw } from 'lucide-react';

const Fraud = () => {
    const [loading, setLoading] = useState(true);
    const [data, setData] = useState([]);
    const [pagination, setPagination] = useState({
        page: 1,
        pageSize: 10,
        totalCount: 0,
        suspiciousCount: 0,
    });
    const [filter, setFilter] = useState('all'); // 'all', 'suspicious', 'high-risk'

    // Format large numbers
    const formatNumber = (num) => {
        if (num >= 1000000) return `${(num / 1000000).toFixed(2)}M`;
        if (num >= 1000) return `${(num / 1000).toFixed(2)}K`;
        return num?.toLocaleString() || '0';
    };

    // Get risk level color and label
    const getRiskLevel = (score) => {
        if (score >= 90) return { color: 'bg-rose-500', textColor: 'text-rose-700', label: 'Critical' };
        if (score >= 70) return { color: 'bg-orange-500', textColor: 'text-orange-700', label: 'High' };
        if (score >= 40) return { color: 'bg-yellow-500', textColor: 'text-yellow-700', label: 'Medium' };
        return { color: 'bg-emerald-500', textColor: 'text-emerald-700', label: 'Low' };
    };

    const fetchFraudData = async (page = 1) => {
        setLoading(true);
        try {
            let params = `?page=${page}&page_size=${pagination.pageSize}&sort_by=fraud_score&sort_order=desc`;
            
            if (filter === 'suspicious') {
                params += '&is_suspicious=true';
            } else if (filter === 'high-risk') {
                params += '&min_fraud_score=0.7';
            }

            const response = await apiClient.get(`/fraud/wallets${params}`);
            const fraudData = response.data;

            // Transform backend data to frontend format
            const transformedData = (fraudData.wallets || []).map(wallet => ({
                walletAddress: wallet.wallet_address,
                fraudScore: Math.round(wallet.fraud_score * 100),
                isSuspicious: wallet.is_suspicious,
                txCount: wallet.tx_count,
                totalValue: wallet.total_value,
                riskCategory: wallet.risk_category,
                lastActivity: wallet.last_activity,
                flaggedReason: wallet.flagged_reason,
            }));

            setData(transformedData);
            setPagination(prev => ({
                ...prev,
                page: fraudData.page,
                totalCount: fraudData.total_count,
                suspiciousCount: fraudData.suspicious_count,
            }));

            toast.success("Fraud data loaded successfully");
        } catch (error) {
            console.error("Failed to fetch fraud data", error);
            toast.error("Failed to load fraud data");
            
            // Set fallback mock data
            setData([
                { walletAddress: "0x987...xyz", fraudScore: 98, isSuspicious: true, txCount: 5000, totalValue: 150000, riskCategory: 'critical' },
                { walletAddress: "0x654...abc", fraudScore: 95, isSuspicious: true, txCount: 3200, totalValue: 500000, riskCategory: 'critical' },
                { walletAddress: "0x321...def", fraudScore: 88, isSuspicious: true, txCount: 1500, totalValue: 25000, riskCategory: 'high' },
                { walletAddress: "0x111...ghi", fraudScore: 75, isSuspicious: true, txCount: 800, totalValue: 10000, riskCategory: 'high' },
                { walletAddress: "0x222...jkl", fraudScore: 60, isSuspicious: false, txCount: 500, totalValue: 5000, riskCategory: 'medium' },
                { walletAddress: "0x333...mno", fraudScore: 12, isSuspicious: false, txCount: 100, totalValue: 1000, riskCategory: 'low' },
            ]);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchFraudData(1);
    }, [filter]);

    const handlePageChange = (newPage) => {
        fetchFraudData(newPage);
    };

    const totalPages = Math.ceil(pagination.totalCount / pagination.pageSize);

    return (
        <div className="space-y-6">
            <div className="flex flex-col gap-2">
                <h2 className="text-3xl font-bold tracking-tight">Fraud Detection</h2>
                <p className="text-muted-foreground">Monitor high-risk wallets and suspicious activities.</p>
            </div>

            {/* Stats Cards */}
            <div className="grid gap-4 md:grid-cols-3">
                <Card className="transition-all duration-300 hover:-translate-y-1 hover:shadow-xl border-black/5 dark:border-white/5">
                    <CardHeader className="pb-2">
                        <CardTitle className="text-sm font-medium text-muted-foreground">Total Wallets Analyzed</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{formatNumber(pagination.totalCount)}</div>
                    </CardContent>
                </Card>
                <Card className="transition-all duration-300 hover:-translate-y-1 hover:shadow-xl border-black/5 dark:border-white/5 border-l-4 border-l-rose-500">
                    <CardHeader className="pb-2">
                        <CardTitle className="text-sm font-medium text-muted-foreground">Suspicious Wallets</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold text-rose-600">{formatNumber(pagination.suspiciousCount)}</div>
                    </CardContent>
                </Card>
                <Card className="transition-all duration-300 hover:-translate-y-1 hover:shadow-xl border-black/5 dark:border-white/5">
                    <CardHeader className="pb-2">
                        <CardTitle className="text-sm font-medium text-muted-foreground">Detection Rate</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">
                            {pagination.totalCount > 0 
                                ? ((pagination.suspiciousCount / pagination.totalCount) * 100).toFixed(1) 
                                : 0}%
                        </div>
                    </CardContent>
                </Card>
            </div>

            {/* Filter Buttons */}
            <div className="flex gap-2">
                <Button 
                    variant={filter === 'all' ? 'default' : 'outline'} 
                    onClick={() => setFilter('all')}
                    className={filter === 'all' ? 'bg-[#D40000] hover:bg-[#b30000]' : ''}
                >
                    All Wallets
                </Button>
                <Button 
                    variant={filter === 'suspicious' ? 'default' : 'outline'} 
                    onClick={() => setFilter('suspicious')}
                    className={filter === 'suspicious' ? 'bg-[#D40000] hover:bg-[#b30000]' : ''}
                >
                    Suspicious Only
                </Button>
                <Button 
                    variant={filter === 'high-risk' ? 'default' : 'outline'} 
                    onClick={() => setFilter('high-risk')}
                    className={filter === 'high-risk' ? 'bg-[#D40000] hover:bg-[#b30000]' : ''}
                >
                    High Risk (70%+)
                </Button>
                <Button variant="outline" onClick={() => fetchFraudData(pagination.page)} className="ml-auto">
                    <RefreshCw className="h-4 w-4 mr-2" /> Refresh
                </Button>
            </div>

            {loading ? (
                <Card>
                    <CardHeader>
                        <Skeleton className="h-6 w-32" />
                    </CardHeader>
                    <CardContent>
                        <Skeleton className="h-[400px] w-full" />
                    </CardContent>
                </Card>
            ) : (
                <Card className="transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-black/5 border-black/5 dark:border-white/5">
                    <CardHeader>
                        <CardTitle>Fraud Wallet Feed</CardTitle>
                        <CardDescription>Real-time analysis of blockchain wallets ranked by risk score.</CardDescription>
                    </CardHeader>
                    <CardContent>
                        <Table>
                            <TableHeader>
                                <TableRow>
                                    <TableHead>Wallet Address</TableHead>
                                    <TableHead>Risk Score</TableHead>
                                    <TableHead>Risk Level</TableHead>
                                    <TableHead>Transactions</TableHead>
                                    <TableHead>Total Value</TableHead>
                                    <TableHead>Status</TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {data.map((wallet, index) => {
                                    const risk = getRiskLevel(wallet.fraudScore);
                                    return (
                                        <TableRow key={index} className="hover:bg-muted/50">
                                            <TableCell className="font-mono text-sm">
                                                {wallet.walletAddress.length > 20 
                                                    ? `${wallet.walletAddress.slice(0, 10)}...${wallet.walletAddress.slice(-8)}`
                                                    : wallet.walletAddress
                                                }
                                            </TableCell>
                                            <TableCell>
                                                <div className="flex items-center gap-2">
                                                    <div className="w-16 h-2 bg-zinc-200 dark:bg-zinc-700 rounded-full overflow-hidden">
                                                        <div 
                                                            className={cn("h-full rounded-full", risk.color)}
                                                            style={{ width: `${wallet.fraudScore}%` }}
                                                        />
                                                    </div>
                                                    <span className="text-sm font-medium">{wallet.fraudScore}%</span>
                                                </div>
                                            </TableCell>
                                            <TableCell>
                                                <span className={cn(
                                                    "px-2 py-1 rounded-full text-xs font-medium",
                                                    risk.textColor,
                                                    risk.color.replace('bg-', 'bg-opacity-20 bg-')
                                                )}>
                                                    {risk.label}
                                                </span>
                                            </TableCell>
                                            <TableCell>{formatNumber(wallet.txCount)}</TableCell>
                                            <TableCell>${formatNumber(wallet.totalValue)}</TableCell>
                                            <TableCell>
                                                {wallet.isSuspicious ? (
                                                    <span className="px-2 py-1 rounded-full text-xs font-medium bg-rose-100 text-rose-700 dark:bg-rose-900/30 dark:text-rose-400">
                                                        Flagged
                                                    </span>
                                                ) : (
                                                    <span className="px-2 py-1 rounded-full text-xs font-medium bg-zinc-100 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400">
                                                        Normal
                                                    </span>
                                                )}
                                            </TableCell>
                                        </TableRow>
                                    );
                                })}
                            </TableBody>
                        </Table>

                        {/* Pagination */}
                        {totalPages > 1 && (
                            <div className="flex items-center justify-between mt-4 pt-4 border-t">
                                <p className="text-sm text-muted-foreground">
                                    Showing {((pagination.page - 1) * pagination.pageSize) + 1} to {Math.min(pagination.page * pagination.pageSize, pagination.totalCount)} of {pagination.totalCount} wallets
                                </p>
                                <div className="flex gap-2">
                                    <Button 
                                        variant="outline" 
                                        size="sm" 
                                        onClick={() => handlePageChange(pagination.page - 1)}
                                        disabled={pagination.page <= 1}
                                    >
                                        <ChevronLeft className="h-4 w-4" />
                                    </Button>
                                    <span className="flex items-center px-3 text-sm">
                                        Page {pagination.page} of {totalPages}
                                    </span>
                                    <Button 
                                        variant="outline" 
                                        size="sm" 
                                        onClick={() => handlePageChange(pagination.page + 1)}
                                        disabled={pagination.page >= totalPages}
                                    >
                                        <ChevronRight className="h-4 w-4" />
                                    </Button>
                                </div>
                            </div>
                        )}
                    </CardContent>
                </Card>
            )}
        </div>
    );
};

export default Fraud;
