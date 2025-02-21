// Fetch data from the FastAPI backend
fetch("/api/column_check")
    .then((response) => response.json())
    .then((data) => {
        const ctx = document.getElementById("metrics").getContext("2d");
        new Chart(ctx, {
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



// Fetch data from the FastAPI backend
fetch("/api/records")
    .then((response) => response.json())
    .then((data) => {
        const ctx = document.getElementById("data_history").getContext("2d");
        new Chart(ctx, {
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
