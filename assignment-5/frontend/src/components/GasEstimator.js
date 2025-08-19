// frontend/src/components/GasEstimator.js
import React from 'react';

function GasEstimator() {
  return (
    <div className="gas-estimator card">
      <h3>Gas Estimator</h3>
      <div className="gas-info">
        <div className="info-row">
          <span>Estimated Gas:</span>
          <span>150,000 units</span>
        </div>
        <div className="info-row">
          <span>Network:</span>
          <span>Localhost (Hardhat)</span>
        </div>
        <div className="info-row">
          <span>Status:</span>
          <span className="status-connected">Connected</span>
        </div>
        <div className="info-row">
          <span>Gas Price:</span>
          <span>20 Gwei</span>
        </div>
      </div>
    </div>
  );
}

export default GasEstimator;