import { Toaster } from 'sonner';
import AppRoutes from './routes/AppRoutes';
import './index.css';

function App() {
  return (
    <>
      <AppRoutes />
      <Toaster position="top-right" richColors />
    </>
  );
}

export default App;
