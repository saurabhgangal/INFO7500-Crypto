// frontend/src/contexts/ThemeContext.js
import React, { createContext } from 'react';

export const ThemeContext = createContext({
  theme: 'dark',
  toggleTheme: () => {},
});

export const ThemeProvider = ThemeContext.Provider;