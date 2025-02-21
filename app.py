import pandas as pd
from kutils import read_data, transform_data, get_column_null_report, get_past_records
from src.kpmg.utils import create_and_move_files
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates


app = FastAPI()

templates = Jinja2Templates(directory="templates")

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files for frontend
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/api/data-quality", response_class=HTMLResponse, status_code=200)
async def get_data(request: Request):
    df = read_data()
    total_records = df.loc["total_records", "Values"]
    total_columns = df.loc["total_Columns", 'Values']
    null_records = df.loc["null_counts", "Values"]
    pk_duplicates = df.loc["primarykey_duplicates", "Values"]
    duplicate_records = df.loc["duplicate_count", "Values"]
    missing_columns = df.loc["missing_columns", "Values"]
    pk_check = df.loc["primary_key_check", 'Values']

    quality_stats = {
        "Total_Records": total_records,
        "Total_Columns": total_columns,
        "Null_Records": null_records,
        "Duplicate_Records": duplicate_records,
        "pk_duplicates": pk_duplicates,
        "missing_columns": missing_columns,
        "pk_check": pk_check
    }

    return templates.TemplateResponse("index.html", {"request":request, "data": quality_stats})


@app.get("/api/metrics", response_class=JSONResponse)
async def get_barchart_data():
    df = transform_data(df=read_data())
    df = df.reset_index()

    labels = df.Metrics.tolist()
    data = df.Values.tolist()
    return {
        "labels": labels,
        "datasets": [
            {
                "label": "Metrics",
                "data": data,
                "backgroundColor": ["rgba(51, 0, 204, 0.6)"] * len(df.Metrics.tolist()),
                "borderColor": ["rgba(75, 192, 132, 1)"] * len(df.Metrics.tolist()),
                "borderWidth": 1,
            }
        ],
    }

@app.get("/api/records", response_class=JSONResponse)
async def get_data_records():
    get_past_records()
    df = pd.read_csv("artifacts/data_records.csv")
    labels = df.Date.tolist()
    data = df.Value.tolist()
    return {
        "labels": labels,
        "datasets": [
            {
                "label": "Records",
                "data": data,
                "backgroundColor": ["rgba(186, 79, 167, 0.6)"] * len(df.Date.tolist()),
                "borderColor": ["rgba(186, 79, 167, 1)"] * len(df.Value.tolist()),
                "borderWidth": 1,
            }
        ],
    }


@app.get("/api/column_check", response_class=JSONResponse)
async def get_column_report():
    df = get_column_null_report()
    print(df.head())
    labels = df.ColumnName.tolist()
    data = df.NullCount.tolist()
    return {
        "labels": labels,
        "datasets": [
            {
                "label": "Nulls",
                "data": data,
                "backgroundColor": ["rgba(51, 0, 204, 0.6)"] * len(df.ColumnName.tolist()),
                "borderColor": ["rgba(51, 0, 204, 1)"] * len(df.NullCount.tolist()),
                "borderWidth": 1,
            }
        ],
    }

