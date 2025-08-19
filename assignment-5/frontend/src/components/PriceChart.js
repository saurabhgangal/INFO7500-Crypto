// frontend/src/components/PriceChart.js
import React, { useState, useEffect } from 'react';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
} from 'chart.js';
import { usePriceHistory } from '../hooks/usePriceHistory';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

function PriceChart({ pool }) {
  const [timeframe, setTimeframe] = useState('24h');
  const { data: priceData, isLoading } = usePriceHistory(pool.address, timeframe);

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
      tooltip: {
        mode: 'index',
        intersect: false,
        backgroundColor: 'rgba(0, 0, 0, 0.8)',
        titleColor: '#fff',
        bodyColor: '#fff',
        borderColor: '#333',
        borderWidth: 1,
      },
    },
    scales: {
      x: {
        grid: {
          display: false,
        },
        ticks: {
          color: '#999',
        },
      },
      y: {
        grid: {
          color: 'rgba(255, 255, 255, 0.1)',
        },
        ticks: {
          color: '#999',
        },
      },
    },
    interaction: {
      mode: 'nearest',
      axis: 'x',
      intersect: false,
    },
  };

  const chartData = {
    labels: priceData?.labels || [],
    datasets: [
      {
        label: `${pool.symbol0}/${pool.symbol1}`,
        data: priceData?.prices || [],
        borderColor: '#4CAF50',
        backgroundColor: 'rgba(76, 175, 80, 0.1)',
        fill: true,
        tension: 0.4,
      },
    ],
  };

  return (
    <div className="price-chart card">
      <div className="chart-header">
        <h3>Price Chart</h3>
        <div className="timeframe-selector">
          {['1h', '24h', '7d', '30d'].map(tf => (
            <button
              key={tf}
              className={timeframe === tf ? 'active' : ''}
              onClick={() => setTimeframe(tf)}
            >
              {tf}
            </button>
          ))}
        </div>
      </div>
      
      <div className="chart-container">
        {isLoading ? (
          <div className="chart-loading">Loading chart data...</div>
        ) : (
          <Line options={chartOptions} data={chartData} />
        )}
      </div>
      
      <div className="chart-stats">
        <div className="stat">
          <span className="label">Current Price</span>
          <span className="value">
            {priceData?.currentPrice || '0.00'} {pool.symbol1}/{pool.symbol0}
          </span>
        </div>
        <div className="stat">
          <span className="label">24h Change</span>
          <span className={`value ${priceData?.change24h >= 0 ? 'positive' : 'negative'}`}>
            {priceData?.change24h >= 0 ? '+' : ''}{priceData?.change24h?.toFixed(2)}%
          </span>
        </div>
        <div className="stat">
          <span className="label">24h High</span>
          <span className="value">{priceData?.high24h || '0.00'}</span>
        </div>
        <div className="stat">
          <span className="label">24h Low</span>
          <span className="value">{priceData?.low24h || '0.00'}</span>
        </div>
      </div>
    </div>
  );
}

export default PriceChart;