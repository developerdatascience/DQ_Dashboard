function fetchData() {
    fetch("/api")
        .then(response => response.json())
        .then(data => {
            document.getElementById("total_records").innerText = data.Total_Records;
            document.getElementById("total_columns").innerText = data.Total_Columns;
            document.getElementById("null_records").innerText = data.Null_Records;
            document.getElementById("duplicate_records").innerText = data.Duplicate_Records;
            document.getElementById("pk_duplicates").innerText = data.pk_duplicates;
            document.getElementById("missing_columns").innerText = data.missing_columns;
            document.getElementById("pk_check").innerText = data.pk_check;
        })
        .catch(error => console.error("Error fetching data:", error));
}

// Fetch data when the page loads
document.addEventListener("DOMContentLoaded", function () {
    fetchData();
    // Refresh data every 5 seconds (5000 milliseconds)
    setInterval(fetchData, 5000);
});