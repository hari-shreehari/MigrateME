from fastapi import HTTPException
import importlib
import os
from ...services.helper import transform

def convert(spark, request):
    output_type = request.output.type.lower()
    output_path = request.output.path
    if not output_path:
        raise HTTPException(400, "output path is required")

    try:
        ingest_module = importlib.import_module(f".services.ingest.mongodb", package="BackEnd")
        ingest_function = getattr(ingest_module, "read_mongodb")
    except (ImportError, AttributeError):
        raise HTTPException(400, f"Unsupported input type 'mongodb'")

    try:
        build_module = importlib.import_module(f".services.build.{output_type}", package="BackEnd")
        build_function = getattr(build_module, f"write_{output_type}")
    except (ImportError, AttributeError):
        raise HTTPException(400, f"Unsupported output type: {output_type}")

    if not request.input.url:
        raise HTTPException(400, "url is required for mongodb input")

    get_database_names_func = getattr(ingest_module, "get_database_names")
    database_names = get_database_names_func(request.input.url)

    for database_name in database_names:
        db_output_path = f"{output_path}/{database_name}"
        os.makedirs(db_output_path, exist_ok=True)
        
        get_collection_names_func = getattr(ingest_module, "get_collection_names")
        collection_names = get_collection_names_func(request.input.url, database_name)

        for collection_name in collection_names:
            df = ingest_function(spark, request.input.url, database_name, collection_name)
            
            if request.transforms:
                for transform_name in request.transforms:
                    if transform_name == "drop_nulls":
                        df = transform.drop_nulls(df)
                    else:
                        raise HTTPException(400, f"Unsupported transform: {transform_name}")

            build_function(df, f"{db_output_path}/{collection_name}")
