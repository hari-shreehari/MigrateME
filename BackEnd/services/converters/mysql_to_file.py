from fastapi import HTTPException
import importlib
from ...services.helper import transform

def convert(spark, request):
    output_type = request.output.type.lower()
    output_path = request.output.path
    if not output_path:
        raise HTTPException(400, "output path is required")

    try:
        ingest_module = importlib.import_module(f".services.ingest.mysql", package="BackEnd")
        ingest_function = getattr(ingest_module, "read_mysql")
    except (ImportError, AttributeError):
        raise HTTPException(400, f"Unsupported input type 'mysql'")

    try:
        build_module = importlib.import_module(f".services.build.{output_type}", package="BackEnd")
        build_function = getattr(build_module, f"write_{output_type}")
    except (ImportError, AttributeError):
        raise HTTPException(400, f"Unsupported output type: {output_type}")

    if not request.input.url:
        raise HTTPException(400, "url is required for mysql input")

    get_table_names_func = getattr(ingest_module, "get_table_names")
    table_names = get_table_names_func(spark, request.input.url, request.input.properties)

    for table_name in table_names:
        df = ingest_function(spark, request.input.url, table_name, request.input.properties)
        
        if request.transforms:
            for transform_name in request.transforms:
                if transform_name == "drop_nulls":
                    df = transform.drop_nulls(df)
                else:
                    raise HTTPException(400, f"Unsupported transform: {transform_name}")

        build_function(df, f"{output_path}/{table_name}")
