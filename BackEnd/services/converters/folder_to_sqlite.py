from fastapi import HTTPException
import importlib
import os
from ...services.helper.sanitize import sanitize_table_name

def convert(spark, request):
    if not request.input.path:
        raise HTTPException(400, "input path (folder) is required for this operation")
    if not request.output.path:
        raise HTTPException(400, "output path is required for sqlite output")

    input_folder = request.input.path
    output_db_path = request.output.path
    
    try:
        build_module = importlib.import_module(f".services.build.sqlite", package="BackEnd")
        build_function = getattr(build_module, "write_sqlite")
    except (ImportError, AttributeError):
        raise HTTPException(400, f"Unsupported output type: sqlite")

    for filename in os.listdir(input_folder):
        file_path = os.path.join(input_folder, filename)
        if os.path.isfile(file_path):
            file_extension = os.path.splitext(filename)[1].lstrip('.')
            
            try:
                ingest_module = importlib.import_module(f".services.ingest.{file_extension}", package="BackEnd")
                ingest_function = getattr(ingest_module, f"read_{file_extension}")
            except (ImportError, AttributeError):
                # Skip unsupported file types
                continue

            df = ingest_function(spark, file_path)
            table_name = sanitize_table_name(filename)
            
            build_function(df, output_db_path, table_name)
