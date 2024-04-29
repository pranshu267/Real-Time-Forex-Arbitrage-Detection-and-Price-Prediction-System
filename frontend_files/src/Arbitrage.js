// src/Arbitrage.js
import React, { useEffect, useRef } from 'react';
import Chart from 'chart.js/auto';

function Arbitrage() {
    const chartRef = useRef(null);

    useEffect(() => {
        fetch('http://localhost:3001/api/arbitrage')
            .then(response => response.json())
            .then(data => {
                const ctx = chartRef.current.getContext('2d');
                new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: data.map(d => d.time), // Assuming 'time' is your label
                        datasets: [{
                            label: 'Arbitrage Opportunities',
                            data: data.map(d => d.value), // Assuming 'value' is your data point
                            borderColor: 'rgb(75, 192, 192)',
                            tension: 0.1
                        }]
                    },
                    options: {
                        scales: {
                            y: {
                                beginAtZero: true
                            }
                        }
                    }
                });
            })
            .catch(error => console.error('Error fetching arbitrage data:', error));
    }, []);

    return (
        <div>
            <h2>Arbitrage Opportunities</h2>
            <canvas ref={chartRef}></canvas>
        </div>
    );
}

export default Arbitrage;
