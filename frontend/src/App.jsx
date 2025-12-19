import { Suspense } from 'react';
import { Toaster } from 'sonner';
import AppRoutes from './routes/AppRoutes';
import LoadingSpinner from './components/LoadingSpinner';
import './index.css';

function App() {
  return (
    <Suspense fallback={<LoadingSpinner message="Loading application..." />}>
      <AppRoutes />
      <Toaster position="top-right" richColors />
    </Suspense>
  );
}

export default App;
