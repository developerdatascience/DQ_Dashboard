function fetchAndRenderChart() {
    fetch("/api/column_check")
        .then((response) => response.json())
        .then((data) => {
            const ctx = document.getElementById("metrics").getContext("2d");
            
            if (window.myChart) {
                window.myChart.destroy(); // Destroy the previous chart before creating a new one
            }
            
            window.myChart = new Chart(ctx, {
                type: "bar",
                data: {
                    labels: data.labels,
                    datasets: data.datasets,
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: {
                            position: "top",
                        },
                    },
                },
            });
        })
        .catch((error) => console.error("Error loading chart data:", error));
}

function fetchAndRenderHistory() {
    fetch("/api/records")
        .then((response) => response.json())
        .then((data) => {
            const ctx = document.getElementById("data_history").getContext("2d");
            
            if (window.myHistoryChart) {
                window.myHistoryChart.destroy(); // Destroy the previous chart before creating a new one
            }
            
            window.myHistoryChart = new Chart(ctx, {
                type: "bar",
                data: {
                    labels: data.labels,
                    datasets: data.datasets,
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: {
                            position: "top",
                        },
                    },
                },
            });
        })
        .catch((error) => console.error("Error loading history chart data:", error));
}

// Initial chart load
fetchAndRenderChart();
fetchAndRenderHistory();

// Refresh charts every 40 seconds
setInterval(fetchAndRenderChart, 40000);
setInterval(fetchAndRenderHistory, 40000);
