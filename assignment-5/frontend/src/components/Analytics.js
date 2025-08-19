// frontend/src/components/Analytics.js
import React, { useState, useEffect } from 'react';
import { useContractReads } from 'wagmi';
import { motion } from 'framer-motion';
import { Doughnut, Bar } from 'react-chartjs-2';
import { FACTORY_ADDRESS } from '../utils/constants';
import factoryABI from '../abi/AMMFactory.json';
import { formatNumber } from '../utils/format';

function Analytics() {
  const [totalPools, setTotalPools] = useState(0);
  const [totalLiquidity, setTotalLiquidity] = useState(0);
  const [totalVolume, setTotalVolume] = useState(0);
  const [topPools, setTopPools] = useState([]);

  // Fetch analytics data
  useEffect(() => {
    // This would normally fetch from a subgraph or indexer
    // For demo purposes, using mock data
    setTotalPools(12);
    setTotalLiquidity(2456789);
    setTotalVolume(8934567);
    setTopPools([
      { name: 'WETH/USDC', tvl: 1234567, volume: 3456789, apy: 24.5 },
      { name: 'WBTC/WETH', tvl: 987654, volume: 2345678, apy: 18.3 },
      { name: 'DAI/USDC', tvl: 654321, volume: 1234567, apy: 12.7 },
    ]);
  }, []);

  const tvlChartData = {
    labels: topPools.map(p => p.name),
    datasets: [{
      data: topPools.map(p => p.tvl),
      backgroundColor: [
        '#4CAF50',
        '#2196F3',
        '#FF9800',
        '#9C27B0',
        '#F44336',
      ],
      borderWidth: 0,
    }],
  };

  const volumeChartData = {
    labels: topPools.map(p => p.name),
    datasets: [{
      label: '24h Volume',
      data: topPools.map(p => p.volume),
      backgroundColor: '#4CAF50',
      borderRadius: 8,
    }],
  };

  return (
    <motion.div 
      className="analytics-dashboard"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.5 }}
    >
      <div className="analytics-header">
        <h1>Protocol Analytics</h1>
        <p>Real-time insights into the AMM ecosystem</p>
      </div>

      <div className="stats-grid">
        <motion.div 
          className="stat-card"
          whileHover={{ scale: 1.05 }}
          transition={{ type: "spring", stiffness: 300 }}
        >
          <div className="stat-icon">🏊</div>
          <div className="stat-content">
            <h3>Total Pools</h3>
            <div className="stat-value">{totalPools}</div>
            <div className="stat-change positive">+2 this week</div>
          </div>
        </motion.div>

        <motion.div 
          className="stat-card"
          whileHover={{ scale: 1.05 }}
          transition={{ type: "spring", stiffness: 300 }}
        >
          <div className="stat-icon">💰</div>
          <div className="stat-content">
            <h3>Total Liquidity</h3>
            <div className="stat-value">${formatNumber(totalLiquidity)}</div>
            <div className="stat-change positive">+12.5%</div>
          </div>
        </motion.div>

        <motion.div 
          className="stat-card"
          whileHover={{ scale: 1.05 }}
          transition={{ type: "spring", stiffness: 300 }}
        >
          <div className="stat-icon">📊</div>
          <div className="stat-content">
            <h3>24h Volume</h3>
            <div className="stat-value">${formatNumber(totalVolume)}</div>
            <div className="stat-change negative">-3.2%</div>
          </div>
        </motion.div>

        <motion.div 
          className="stat-card"
          whileHover={{ scale: 1.05 }}
          transition={{ type: "spring", stiffness: 300 }}
        >
          <div className="stat-icon">💵</div>
          <div className="stat-content">
            <h3>Total Fees (24h)</h3>
            <div className="stat-value">${formatNumber(totalVolume * 0.003)}</div>
            <div className="stat-change positive">+8.7%</div>
          </div>
        </motion.div>
      </div>

      <div className="charts-grid">
        <div className="chart-card">
          <h3>TVL Distribution</h3>
          <div className="chart-container">
            <Doughnut 
              data={tvlChartData}
              options={{
                plugins: {
                  legend: {
                    position: 'bottom',
                  },
                },
              }}
            />
          </div>
        </div>

        <div className="chart-card">
          <h3>Top Pools by Volume</h3>
          <div className="chart-container">
            <Bar 
              data={volumeChartData}
              options={{
                plugins: {
                  legend: {
                    display: false,
                  },
                },
                scales: {
                  y: {
                    beginAtZero: true,
                  },
                },
              }}
            />
          </div>
        </div>
      </div>

      <div className="top-pools-table">
        <h3>Top Performing Pools</h3>
        <table>
          <thead>
            <tr>
              <th>Pool</th>
              <th>TVL</th>
              <th>24h Volume</th>
              <th>APY</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {topPools.map((pool, index) => (
              <motion.tr
                key={pool.name}
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: index * 0.1 }}
              >
                <td className="pool-name">{pool.name}</td>
                <td>${formatNumber(pool.tvl)}</td>
                <td>${formatNumber(pool.volume)}</td>
                <td className="apy">{pool.apy.toFixed(2)}%</td>
                <td>
                  <button className="view-btn">View Pool</button>
                </td>
              </motion.tr>
            ))}
          </tbody>
        </table>
      </div>
    </motion.div>
  );
}

export default Analytics;