from fastapi import HTTPException
import importlib
from ...services.helper import transform

def convert(spark, request):
    input_type = request.input.type.lower()
    output_type = request.output.type.lower()
    
    try:
        ingest_module = importlib.import_module(f".services.ingest.{input_type}", package="BackEnd")
        ingest_function = getattr(ingest_module, f"read_{input_type}")
    except (ImportError, AttributeError):
        raise HTTPException(400, f"Unsupported input type '{request.input.type}'")

    output_path = request.output.path
    if not output_path:
        raise HTTPException(400, "output path is required")

    try:
        build_module = importlib.import_module(f".services.build.{output_type}", package="BackEnd")
        build_function = getattr(build_module, f"write_{output_type}")
    except (ImportError, AttributeError):
        raise HTTPException(400, f"Unsupported output type: {output_type}")

    if not request.input.path:
        raise HTTPException(400, f"path is required for {input_type} input")
    df = ingest_function(spark, request.input.path)

    if request.transforms:
        for transform_name in request.transforms:
            if transform_name == "drop_nulls":
                df = transform.drop_nulls(df)
            else:
                raise HTTPException(400, f"Unsupported transform: {transform_name}")
    
    build_function(df, output_path)
