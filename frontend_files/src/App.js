// src/App.js
import React from 'react';
import Arbitrage from './Arbitrage';
import Historical from './Historical';
import Predictions from './Predictions';
import './App.css';



function App() {
  return (
    <div className="App">
            <header className="project-title">
                ARBITRAGE
            </header>
            <div className="dashboard">
                <div className="arbitrage">
                    <Arbitrage />
                </div>
                <div className="historical">
                    <Historical />
                </div>
                <div className="predictions">
                    <Predictions />
                </div>
            </div>
        </div>
  );
}

export default App;
