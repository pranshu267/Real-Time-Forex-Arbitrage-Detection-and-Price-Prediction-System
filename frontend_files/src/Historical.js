// src/Historical.js
import React, { useEffect, useRef } from 'react';
import Chart from 'chart.js/auto';

function Historical() {
    const chartRef = useRef(null);

    useEffect(() => {
        fetch('http://localhost:3001/api/historical')
            .then(response => response.json())
            .then(data => {
                const ctx = chartRef.current.getContext('2d');
                new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: data.map(d => d.time), // Time or date
                        datasets: [{
                            label: 'Historical Forex Prices',
                            data: data.map(d => d.price), // Price or value
                            borderColor: 'rgb(255, 99, 132)',
                            tension: 0.1
                        }]
                    },
                    options: {
                        scales: {
                            y: {
                                beginAtZero: false
                            }
                        }
                    }
                });
            })
            .catch(error => console.error('Error fetching historical data:', error));
    }, []);

    return (
        <div>
            <h2>Historical Forex Data</h2>
            <canvas ref={chartRef}></canvas>
        </div>
    );
}

export default Historical;
