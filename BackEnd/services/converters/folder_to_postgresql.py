from fastapi import HTTPException
import importlib
import os
from ...services.helper.sanitize import sanitize_table_name

def convert(spark, request):
    if not request.input.path:
        raise HTTPException(400, "input path (folder) is required for this operation")
    if not (request.output.url and request.output.properties):
        raise HTTPException(400, "output url and properties are required for postgresql output")

    input_folder = request.input.path
    
    try:
        build_module = importlib.import_module(f".services.build.postgresql", package="BackEnd")
        build_function = getattr(build_module, "write_postgresql")
    except (ImportError, AttributeError):
        raise HTTPException(400, f"Unsupported output type: postgresql")

    output_properties = request.output.properties.copy()
    output_properties["prepareThreshold"] = "0"

    output_url = request.output.url
    if "statementPooling=False" not in output_url:
        if "?" in output_url:
            output_url += "&statementPooling=False"
        else:
            output_url += "?statementPooling=False"

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
            table_name = sanitize_table_name(filename)
            
            build_function(df, output_url, table_name, output_properties)
