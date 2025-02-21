from pathlib import Path
import logging
import os

logging.basicConfig(level= logging.INFO, format='[%(asctime)s]: %(message)s')

project_name = Path("kpmg")

list_of_files = [
    '.github/workflows/.gitkeep',
    f"src/{project_name}/__init__.py",
    f"src/{project_name}/utils/__init__.py",
    f"src/{project_name}/utils/common.py",
    f"src/{project_name}/config/__init__.py",
    f"src/{project_name}/config/configuration.py",
    f"src/{project_name}/constants/__init__.py",
    f"src/{project_name}/entity/__init__.py",
    f"src/{project_name}/pipeline/__init__.py",
    "artifacts/reports.csv",
    "config/config.yaml",
    "dvc.yaml",
    "params.yaml",
    "main.py",
    "app.py",
    "Dockerfile",
    "requirements.txt",
    "setup.py",
    "research/trials.ipynb",
    "templates/index.html",
    "static/style.css",
    "static/scripts.js"
]

for filepath in list_of_files:
    filepath = Path(filepath)
    filedir, filename = os.path.split(filepath)


    if filedir != '':
        os.makedirs(filedir, exist_ok=True)
        logging.info(f"Creating directory {filedir} for the file {filename}")
    
    if (not os.path.exists(filepath)) or (os.path.getsize(filepath) == 0):
        with open(filepath, "w"):
            pass
        logging.info(f"Creating an empty file: {filepath}")
    
    else:
        logging.info(f"{filepath} already exists")