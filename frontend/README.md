# ChainGuard - Blockchain Analytics Platform

A modern, high-performance dashboard for monitoring blockchain transactions, analyzing wallet behaviors, and detecting potential fraud in real-time. Built with **React**, **Vite**, and **Tailwind CSS v4**, featuring a premium **Ferrari-inspired "Scuderia" aesthetics**.

## 🚀 Features

- **📊 Interactive Dashboard**: Real-time overview of transaction volumes, active users, and network health.
- **👛 Wallet Analytics**: Deep dive into specific wallet addresses to view transaction history, risk scores, and balance trends.
- **🛡️ Fraud Detection**: AI-powered fraud feed highlighting suspicious transactions with risk probability scores.
- **🎨 Premium UX/UI**:
  - **"Scuderia" Theme**: Distinctive Rosso Corsa Red, Carbon Black, and White palette.
  - **Glassmorphism**: Modern backdrop blur effects on sidebars and headers.
  - **Smooth Animations**: Powered by **Framer Motion** for polished page transitions and hover effects.
- **📈 Data Visualization**: Interactive charts powered by **Recharts**.
- **📱 Fully Responsive**: Optimized for all devices with a mobile-first approach.

## 🛠️ Tech Stack

- **Framework**: [React 18](https://react.dev/) + [Vite](https://vitejs.dev/)
- **Styling**: [Tailwind CSS v4](https://tailwindcss.com/)
- **UI Components**: [shadcn/ui](https://ui.shadcn.com/)
- **Charts**: [Recharts](https://recharts.org/)
- **Routing**: [React Router](https://reactrouter.com/)
- **HTTP Client**: [Axios](https://axios-http.com/)
- **Icons**: [Lucide React](https://lucide.dev/)
- **Animations**: [Framer Motion](https://www.framer.com/motion/)
- **Notifications**: [Sonner](https://sonner.emilkowal.ski/)

## ⚡ Getting Started

### Prerequisites

- Node.js (v18 or higher)
- npm or yarn

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd blockchain_analytics/frontend
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Start the development server**
   ```bash
   npm run dev
   ```
   The application will be available at `http://localhost:5173`.

## 🔧 Configuration

The application is configured to connect to a backend API.

- **API URL**: Default is `http://localhost:8000`. You can override this by creating a `.env` file:
  ```env
  VITE_API_URL=http://your-api-url.com
  ```

> **Note**: If the backend is unreachable, the application automatically falls back to **Mock Data mode**, allowing you to explore the UI and features without a running server.

## 📂 Project Structure

```
src/
├── api/            # API client configuration and interceptors
├── components/     # Reusable UI components
│   ├── ui/         # Generic shadcn/ui components
│   └── ...         # Domain components (StatCard, TransactionChart, etc.)
├── pages/          # Main application pages (Dashboard, Wallet, Fraud)
├── routes/         # Route definitions
├── lib/            # Utility functions
└── App.jsx         # Root component
```

## 🏗️ Building for Production

To create a production-ready build:

```bash
npm run build
```

The output will be in the `dist/` directory.

## 🤝 Contributing

1. Fork the project
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

© 2024 ChainGuard Analytics.
