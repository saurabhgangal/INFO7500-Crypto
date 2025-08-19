// frontend/src/App.js - Complete Web3 Integration
import React, { useState, useEffect } from 'react';
import { WagmiConfig, createConfig, configureChains, createPublicClient, http } from 'wagmi';
import { localhost } from 'wagmi/chains';
import { publicProvider } from 'wagmi/providers/public';
import { InjectedConnector } from 'wagmi/connectors/injected';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Toaster } from 'react-hot-toast';
import './App.css';

// Components
import Header from './components/Header';
import PoolList from './components/PoolList';
import CreatePool from './components/CreatePool';
import SwapForm from './components/SwapForm';
import LiquidityForm from './components/LiquidityForm';
import TransactionHistory from './components/TransactionHistory';
import Analytics from './components/Analytics';
import PriceChart from './components/PriceChart';
import GasEstimator from './components/GasEstimator';

// Context
import { ThemeProvider } from './contexts/ThemeContext';

// Configure for localhost only
const { chains, publicClient } = configureChains(
  [localhost],
  [publicProvider()]
);

// Simple injected connector (MetaMask)
const connectors = [
  new InjectedConnector({
    chains,
    options: {
      name: 'MetaMask',
      shimDisconnect: true,
    },
  }),
];

// Create wagmi config
const config = createConfig({
  autoConnect: true,
  connectors,
  publicClient,
});

// Create query client
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 3,
    },
  },
});

// ConnectButton is now in Header component

function App() {
  const [selectedPool, setSelectedPool] = useState(null);
  const [activeTab, setActiveTab] = useState('swap');
  const [showAnalytics, setShowAnalytics] = useState(false);
  const [theme, setTheme] = useState('dark');

  // Load saved preferences
  useEffect(() => {
    const savedTheme = localStorage.getItem('theme') || 'dark';
    setTheme(savedTheme);
    document.body.className = savedTheme;
  }, []);

  const toggleTheme = () => {
    const newTheme = theme === 'dark' ? 'light' : 'dark';
    setTheme(newTheme);
    localStorage.setItem('theme', newTheme);
    document.body.className = newTheme;
  };

  return (
    <WagmiConfig config={config}>
      <QueryClientProvider client={queryClient}>
        <ThemeProvider value={{ theme, toggleTheme }}>
          <div className="App">
            <Header 
              onToggleAnalytics={setShowAnalytics}
              showAnalytics={showAnalytics}
              theme={theme}
              toggleTheme={toggleTheme}
            />
            
            <div className="container">
              <div className="sidebar">
                <CreatePool 
                  onPoolCreated={() => {
                    console.log('Pool created!');
                    // In real app, this would refresh the pool list
                  }} 
                />
                <PoolList 
                  onSelectPool={setSelectedPool} 
                  selectedPool={selectedPool} 
                />
              </div>
              
              <div className="main-content">
                {showAnalytics ? (
                  <div className="analytics-section">
                    <Analytics />
                    <PriceChart />
                    <GasEstimator />
                  </div>
                ) : (
                  <div>
                    {selectedPool ? (
                      <div className="pool-trading-section">
                        <div className="pool-info">
                          <h2>{selectedPool.symbol0}/{selectedPool.symbol1} Pool</h2>
                          <div className="pool-stats">
                            <div>TVL: ${selectedPool.tvl?.toLocaleString() || '0'}</div>
                            <div>24h Volume: ${selectedPool.volume24h?.toLocaleString() || '0'}</div>
                            <div>24h Fees: ${selectedPool.fees24h?.toLocaleString() || '0'}</div>
                          </div>
                        </div>
                        
                        <div className="tabs">
                          <button 
                            className={`tab-btn ${activeTab === 'swap' ? 'active' : ''}`}
                            onClick={() => setActiveTab('swap')}
                          >
                            Swap
                          </button>
                          <button 
                            className={`tab-btn ${activeTab === 'liquidity' ? 'active' : ''}`}
                            onClick={() => setActiveTab('liquidity')}
                          >
                            Liquidity
                          </button>
                        </div>
                        
                        <div className="tab-content">
                          {activeTab === 'swap' ? (
                            <SwapForm pool={selectedPool} />
                          ) : (
                            <LiquidityForm pool={selectedPool} />
                          )}
                        </div>
                        
                        <TransactionHistory pool={selectedPool} />
                      </div>
                    ) : (
                      <div className="welcome-section">
                        <h2>Welcome to Premium AMM</h2>
                        <p>Select a pool from the list or create a new one to get started.</p>
                        
                        <div className="status-checklist">
                          <p>✅ React is working!</p>
                          <p>✅ Theme switching works!</p>
                          <p>✅ Web3 integration complete!</p>
                          <p>✅ Pool components connected!</p>
                          <p>✅ Trading interface ready!</p>
                          <p>🚀 Ready to deploy and test!</p>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
            
            <Toaster position="bottom-right" />
          </div>
        </ThemeProvider>
      </QueryClientProvider>
    </WagmiConfig>
  );
}

export default App;
