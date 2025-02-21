import shutil
from pathlib import Path
from datetime import datetime, timedelta


def create_and_move_file():
    yesterday = datetime.today() - timedelta(days=1)
    yesterday = yesterday.strftime("%Y%m%d")
    archive_path = Path(f"archive/")

    old_file_path = Path(f"inputs/finance/{yesterday}.csv")

    if not archive_path.exists():
        archive_path.mkdir(parents=True, exist_ok=True)
    else:
        print(f"folder {archive_path} exists")

    if old_file_path.exists():
        shutil.move(src=old_file_path, dst=archive_path)
        print("file moved successfully")
    else:
        print("Path does not exists")


