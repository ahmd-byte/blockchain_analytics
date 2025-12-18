import { useState, useEffect } from 'react';
// import apiClient from '../api/apiClient';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import FraudTable from '../components/FraudTable';
import { Skeleton } from '@/components/ui/skeleton';
import { cn } from "@/lib/utils";

const Fraud = () => {
    const [loading, setLoading] = useState(true);
    const [data, setData] = useState([]);

    useEffect(() => {
        const fetchData = async () => {
            try {
                // const response = await apiClient.get('/fraud/wallets');
                // setData(response.data);

                await new Promise(resolve => setTimeout(resolve, 1000));
                setData([
                    { hash: "0x987...xyz", from: "0xbad...actor", to: "0xvic...tim", amount: "150.0", riskScore: 98 },
                    { hash: "0x654...abc", from: "0xhac...ker", to: "0xexc...hange", amount: "500.0", riskScore: 95 },
                    { hash: "0x321...def", from: "0xmal...ware", to: "0xwal...let", amount: "2.5", riskScore: 88 },
                    { hash: "0x111...ghi", from: "0xsus...pect", to: "0xmix...er", amount: "10.0", riskScore: 75 },
                    { hash: "0x222...jkl", from: "0xbot...net", to: "0xdex...swap", amount: "0.5", riskScore: 60 },
                    { hash: "0x333...mno", from: "0xuse...r1", to: "0xuse...r2", amount: "1.0", riskScore: 12 },
                ]);

            } catch (error) {
                console.error("Failed to fetch fraud data", error);
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, []);

    return (
        <div className="space-y-6">
            <div className="flex flex-col gap-2">
                <h2 className="text-3xl font-bold tracking-tight">Fraud Detection</h2>
                <p className="text-muted-foreground">Monitor high-risk transactions and suspicious wallet activities.</p>
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
                        <CardTitle>Live Fraud Feed</CardTitle>
                        <CardDescription>Real-time analysis of blockchain transactions.</CardDescription>
                    </CardHeader>
                    <CardContent>
                        <FraudTable transactions={data} />
                    </CardContent>
                </Card>
            )}
        </div>
    );
};

export default Fraud;
