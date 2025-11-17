from fastapi import HTTPException
import importlib
from ...services.helper import transform

def convert(spark, request):
    if not request.input.url or not request.output.url:
        raise HTTPException(400, "input and output urls are required for mongodb to mongodb conversion")

    ingest_module = importlib.import_module(f".services.ingest.mongodb", package="BackEnd")
    ingest_function = getattr(ingest_module, "read_mongodb")
    get_database_names_func = getattr(ingest_module, "get_database_names")
    get_collection_names_func = getattr(ingest_module, "get_collection_names")

    build_module = importlib.import_module(f".services.build.mongodb", package="BackEnd")
    build_function = getattr(build_module, "write_mongodb")

    database_names = get_database_names_func(request.input.url)

    for database_name in database_names:
        collection_names = get_collection_names_func(request.input.url, database_name)

        for collection_name in collection_names:
            df = ingest_function(spark, request.input.url, database_name, collection_name)
            
            if request.transforms:
                for transform_name in request.transforms:
                    if transform_name == "drop_nulls":
                        df = transform.drop_nulls(df)
                    else:
                        raise HTTPException(400, f"Unsupported transform: {transform_name}")

            build_function(df, request.output.url, database_name, collection_name)
