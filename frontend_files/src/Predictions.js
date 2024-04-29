// src/Predictions.js
import React, { useEffect, useRef } from 'react';
import Chart from 'chart.js/auto';

function Predictions() {
    const chartRef = useRef(null);

    useEffect(() => {
        fetch('http://localhost:3001/api/predictions')
            .then(response => response.json())
            .then(data => {
                const ctx = chartRef.current.getContext('2d');
                new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: data.map(d => d.time), // Assuming 'time' is your label
                        datasets: [{
                            label: 'Forex Price Predictions',
                            data: data.map(d => d.predicted_price), // Predicted price
                            borderColor: 'rgb(54, 162, 235)',
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
            .catch(error => console.error('Error fetching prediction data:', error));
    }, []);

    return (
        <div>
            <h2>Forex Price Predictions</h2>
            <canvas ref={chartRef}></canvas>
        </div>
    );
}

export default Predictions;
