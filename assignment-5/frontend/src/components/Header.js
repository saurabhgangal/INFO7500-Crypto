// frontend/src/components/Header.js
import React from 'react';
import { useAccount, useBalance } from 'wagmi';

// Simple connect button component
function ConnectButton() {
  const { address, isConnected, connect, disconnect } = useAccount();
  const { data: balance } = useBalance({ address });

  if (isConnected) {
    return (
      <div className="connect-button-wrapper">
        <span className="address">
          {address?.slice(0, 6)}...{address?.slice(-4)}
        </span>
        <button onClick={() => disconnect()} className="disconnect-btn">
          Disconnect
        </button>
      </div>
    );
  }

  return (
    <button 
      onClick={() => connect()} 
      className="connect-btn"
    >
      Connect Wallet
    </button>
  );
}

function Header({ onToggleAnalytics, showAnalytics, theme, toggleTheme }) {
  return (
    <header className="header">
      <div className="header-left">
        <div className="logo">
          <span className="logo-icon">🔄</span>
          <div>
            <h1>Premium AMM</h1>
            <span className="subtitle">Decentralized Exchange</span>
          </div>
        </div>
      </div>

      <nav className="header-nav">
        <button 
          className={`nav-btn ${!showAnalytics ? 'active' : ''}`}
          onClick={() => onToggleAnalytics(false)}
        >
          Trade
        </button>
        <button 
          className={`nav-btn ${showAnalytics ? 'active' : ''}`}
          onClick={() => onToggleAnalytics(true)}
        >
          Analytics
        </button>
      </nav>

      <div className="header-right">
        <button 
          className="theme-toggle"
          onClick={toggleTheme}
        >
          {theme === 'dark' ? '🌞' : '🌙'}
        </button>
        <ConnectButton />
      </div>
    </header>
  );
}

export default Header;