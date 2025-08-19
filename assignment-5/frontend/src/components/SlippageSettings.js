// frontend/src/components/SlippageSettings.js
import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

function SlippageSettings({ slippage, onSlippageChange, onClose }) {
  const [customSlippage, setCustomSlippage] = useState('');
  const [showCustom, setShowCustom] = useState(false);

  const presetSlippages = [0.1, 0.5, 1.0];

  const handlePresetClick = (value) => {
    onSlippageChange(value);
    setShowCustom(false);
    setCustomSlippage('');
  };

  const handleCustomSubmit = () => {
    const value = parseFloat(customSlippage);
    if (!isNaN(value) && value >= 0 && value <= 50) {
      onSlippageChange(value);
    }
  };

  return (
    <motion.div
      className="slippage-settings"
      initial={{ opacity: 0, height: 0 }}
      animate={{ opacity: 1, height: 'auto' }}
      exit={{ opacity: 0, height: 0 }}
    >
      <div className="settings-header">
        <h4>Transaction Settings</h4>
        <button className="close-btn" onClick={onClose}>×</button>
      </div>

      <div className="slippage-section">
        <label>Slippage Tolerance</label>
        <div className="slippage-options">
          {presetSlippages.map(value => (
            <button
              key={value}
              className={`slippage-btn ${slippage === value && !showCustom ? 'active' : ''}`}
              onClick={() => handlePresetClick(value)}
            >
              {value}%
            </button>
          ))}
          <button
            className={`slippage-btn ${showCustom ? 'active' : ''}`}
            onClick={() => setShowCustom(true)}
          >
            Custom
          </button>
        </div>

        {showCustom && (
          <div className="custom-slippage">
            <input
              type="number"
              placeholder="0.50"
              value={customSlippage}
              onChange={(e) => setCustomSlippage(e.target.value)}
              onBlur={handleCustomSubmit}
              min="0"
              max="50"
              step="0.1"
            />
            <span>%</span>
          </div>
        )}

        {slippage > 5 && (
          <div className="slippage-warning">
            ⚠️ Your transaction may be frontrun with high slippage
          </div>
        )}
      </div>

      <div className="deadline-section">
        <label>Transaction Deadline</label>
        <div className="deadline-input">
          <input type="number" defaultValue="20" min="1" max="60" />
          <span>minutes</span>
        </div>
      </div>
    </motion.div>
  );
}

export default SlippageSettings;