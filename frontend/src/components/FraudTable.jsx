import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table"
import { BadgeAlert, CheckCircle, AlertTriangle } from "lucide-react"

const FraudTable = ({ transactions }) => {
    return (
        <div className="rounded-md overflow-hidden">
            <Table>
                <TableHeader>
                    <TableRow>
                        <TableHead>Transaction Hash</TableHead>
                        <TableHead>From</TableHead>
                        <TableHead>To</TableHead>
                        <TableHead>Amount</TableHead>
                        <TableHead className="text-right">Risk Score</TableHead>
                        <TableHead className="text-right">Status</TableHead>
                    </TableRow>
                </TableHeader>
                <TableBody>
                    {transactions?.map((tx) => (
                        <TableRow key={tx.hash} className="hover:bg-zinc-50/50 dark:hover:bg-white/5 transition-colors border-b-black/5 dark:border-b-white/5">
                            <TableCell className="font-mono text-xs text-zinc-500">{tx.hash.substring(0, 16)}...</TableCell>
                            <TableCell className="font-mono text-xs text-zinc-500">{tx.from.substring(0, 10)}...</TableCell>
                            <TableCell className="font-mono text-xs text-zinc-500">{tx.to.substring(0, 10)}...</TableCell>
                            <TableCell className="font-medium">{tx.amount} ETH</TableCell>
                            <TableCell className="text-right font-medium">
                                <span className={
                                    tx.riskScore > 80 ? "text-rose-600" :
                                        tx.riskScore > 50 ? "text-amber-600" :
                                            "text-emerald-600"
                                }>
                                    {tx.riskScore}/100
                                </span>
                            </TableCell>
                            <TableCell className="flex justify-end">
                                {tx.riskScore > 80 ? (
                                    <div className="flex items-center text-rose-600 gap-1.5 text-[10px] font-bold uppercase tracking-wider bg-rose-50 border border-rose-100 px-2.5 py-1 rounded-full dark:bg-rose-900/20 dark:border-rose-900/30">
                                        <BadgeAlert className="h-3 w-3" /> Fraud
                                    </div>
                                ) : tx.riskScore > 50 ? (
                                    <div className="flex items-center text-amber-600 gap-1.5 text-[10px] font-bold uppercase tracking-wider bg-amber-50 border border-amber-100 px-2.5 py-1 rounded-full dark:bg-amber-900/20 dark:border-amber-900/30">
                                        <AlertTriangle className="h-3 w-3" /> Suspicious
                                    </div>
                                ) : (
                                    <div className="flex items-center text-emerald-600 gap-1.5 text-[10px] font-bold uppercase tracking-wider bg-emerald-50 border border-emerald-100 px-2.5 py-1 rounded-full dark:bg-emerald-900/20 dark:border-emerald-900/30">
                                        <CheckCircle className="h-3 w-3" /> Safe
                                    </div>
                                )}
                            </TableCell>
                        </TableRow>
                    ))}
                    {!transactions?.length && (
                        <TableRow>
                            <TableCell colSpan={6} className="text-center h-24 text-muted-foreground">
                                No recent fraud alerts found.
                            </TableCell>
                        </TableRow>
                    )}
                </TableBody>
            </Table>
        </div>
    )
}

export default FraudTable
