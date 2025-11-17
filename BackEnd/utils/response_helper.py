import os
import shutil
import tempfile
from pathlib import Path
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

def create_file_response(output_path: str):
    if not output_path or not os.path.exists(output_path):
        return None

    if os.path.isfile(output_path):
        temp_dir = tempfile.mkdtemp()
        temp_file_path = Path(temp_dir) / Path(output_path).name
        shutil.move(output_path, temp_file_path)
        
        cleanup = BackgroundTask(shutil.rmtree, temp_dir)
        return FileResponse(temp_file_path, filename=Path(output_path).name, background=cleanup)
        
    elif os.path.isdir(output_path):
        temp_dir = tempfile.mkdtemp()
        archive_base_path = Path(temp_dir) / Path(output_path).name
        shutil.make_archive(str(archive_base_path), 'zip', output_path)
        
        zip_path = str(archive_base_path) + ".zip"
        
        cleanup = BackgroundTask(shutil.rmtree, temp_dir)
        return FileResponse(zip_path, filename=f"{Path(output_path).name}.zip", background=cleanup)
    
    return None
