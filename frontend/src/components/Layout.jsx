import { Link, useLocation } from 'react-router-dom';
import { LayoutDashboard, Wallet, ShieldAlert } from 'lucide-react';
import { cn } from '@/lib/utils';
import { Outlet } from 'react-router-dom';
import { AnimatePresence, motion } from 'framer-motion';


const SidebarItem = ({ to, icon: Icon, label }) => {
    const location = useLocation();
    const isActive = location.pathname === to || (to !== '/' && location.pathname.startsWith(to));

    return (
        <Link
            to={to}
            className={cn(
                "flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-300 text-sm font-medium",
                isActive
                    ? "bg-[#D40000] text-white shadow-lg shadow-[#D40000]/20 translate-x-1"
                    : "text-zinc-500 hover:text-zinc-900 hover:bg-zinc-50 dark:text-zinc-400 dark:hover:text-zinc-100 dark:hover:bg-zinc-800/50"
            )}
        >
            <Icon className="h-5 w-5" />
            {label}
        </Link>
    );
};

const Layout = () => {
    const location = useLocation();

    return (
        <div className="flex min-h-screen bg-[#f8f9fa] text-zinc-900 font-sans selection:bg-[#D40000] selection:text-white dark:bg-[#050505] dark:text-zinc-100">
            {/* Sidebar with Glassmorphism */}
            <aside className="w-64 border-r border-black/5 bg-white/80 backdrop-blur-xl dark:bg-[#111111]/80 dark:border-white/5 hidden md:flex flex-col shadow-xl shadow-black/5 z-20 sticky top-0 h-screen">
                <div className="p-6 flex items-center gap-2 border-b border-black/5 dark:border-white/5">
                    <h1 className="text-xl font-bold tracking-tight uppercase italic bg-gradient-to-r from-zinc-900 to-zinc-600 bg-clip-text text-transparent dark:from-white dark:to-zinc-400">ChainGuard</h1>
                </div>

                <nav className="flex-1 p-4 space-y-2">
                    <SidebarItem to="/" icon={LayoutDashboard} label="Dashboard" />
                    <SidebarItem to="/wallet" icon={Wallet} label="Wallet Analytics" />
                    <SidebarItem to="/fraud" icon={ShieldAlert} label="Fraud Detection" />
                </nav>

                <div className="p-4 border-t border-black/5 dark:border-white/5 bg-black/[0.02]">
                    <p className="text-[10px] font-bold text-[#D40000] uppercase tracking-widest opacity-80">Scuderia Analytics</p>
                    <p className="text-[10px] text-zinc-400 mt-1 font-medium">v1.2.0 • Pro Edition</p>
                </div>
            </aside>

            {/* Main Content */}
            <main className="flex-1 overflow-x-hidden">
                <header className="h-16 border-b border-black/5 dark:border-white/5 bg-white/80 backdrop-blur-xl dark:bg-[#111111]/80 flex items-center px-6 md:hidden sticky top-0 z-10">
                    <span className="font-bold text-[#D40000] italic uppercase">ChainGuard</span>
                </header>

                <AnimatePresence mode="wait">
                    <motion.div
                        key={location.pathname}
                        initial={{ opacity: 0, y: 15, filter: "blur(5px)" }}
                        animate={{ opacity: 1, y: 0, filter: "blur(0px)" }}
                        exit={{ opacity: 0, y: -15, filter: "blur(5px)" }}
                        transition={{ duration: 0.3, ease: "easeOut" }}
                        className="p-6 mx-auto max-w-7xl"
                    >
                        <Outlet />
                    </motion.div>
                </AnimatePresence>
            </main>
        </div>
    );
};

export default Layout;
