import { lazy } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';

// Lazy load components for better performance
const Layout = lazy(() => import('../components/Layout'));
const Dashboard = lazy(() => import('../pages/Dashboard'));
const Wallet = lazy(() => import('../pages/Wallet'));
const Fraud = lazy(() => import('../pages/Fraud'));

const AppRoutes = () => {
    return (
        <BrowserRouter>
            <Routes>
                <Route path="/" element={<Layout />}>
                    <Route index element={<Dashboard />} />
                    <Route path="wallet/:address?" element={<Wallet />} />
                    <Route path="fraud" element={<Fraud />} />
                    <Route path="*" element={<Navigate to="/" replace />} />
                </Route>
            </Routes>
        </BrowserRouter>
    );
};

export default AppRoutes;
