import React, { useState, useEffect } from 'react';
import './App.css';
import { ThemeProvider } from './contexts/ThemeContext';

function App() {
  const [theme, setTheme] = useState('dark');

  const toggleTheme = () => {
    const newTheme = theme === 'dark' ? 'light' : 'dark';
    setTheme(newTheme);
    document.body.className = newTheme;
  };

  return (
    <ThemeProvider value={{ theme, toggleTheme }}>
      <div className="App">
        <header className="header">
          <h1>Premium AMM</h1>
          <button onClick={toggleTheme}>
            {theme === 'dark' ? '🌞' : '🌙'}
          </button>
        </header>
        
        <div className="container">
          <h2>Welcome to Premium AMM</h2>
          <p>Your app is working! Web3 functionality needs to be connected.</p>
          
          <div style={{ marginTop: '2rem' }}>
            <h3>Next Steps:</h3>
            <ol>
              <li>Deploy your smart contracts</li>
              <li>Connect Web3 provider</li>
              <li>Start trading!</li>
            </ol>
          </div>
        </div>
      </div>
    </ThemeProvider>
  );
}

export default App;
