from fastapi import HTTPException
import importlib
import os
from ...services.helper.sanitize import sanitize_table_name

def convert(spark, request):
    if not request.input.path:
        raise HTTPException(400, "input path (folder) is required for this operation")
    if not (request.output.url and request.output.path):
        raise HTTPException(400, "output url and database name (in path) are required for mongodb output")

    input_folder = request.input.path
    database_name = request.output.path

    try:
        build_module = importlib.import_module(f".services.build.mongodb", package="BackEnd")
        build_function = getattr(build_module, f"write_mongodb")
    except (ImportError, AttributeError):
        raise HTTPException(400, f"Unsupported output type: mongodb")

    for filename in os.listdir(input_folder):
        file_path = os.path.join(input_folder, filename)
        if os.path.isfile(file_path):
            file_extension = os.path.splitext(filename)[1].lstrip('.')
            
            try:
                ingest_module = importlib.import_module(f".services.ingest.{file_extension}", package="BackEnd")
                ingest_function = getattr(ingest_module, f"read_{file_extension}")
            except (ImportError, AttributeError):
                continue

            df = ingest_function(spark, file_path)
            collection_name = sanitize_table_name(filename)
            
            build_function(df, request.output.url, database_name, collection_name)
