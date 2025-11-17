from fastapi import HTTPException
import importlib
from ...services.helper import transform

def convert(spark, request):
    if not request.input.url or not request.output.url:
        raise HTTPException(400, "input and output urls are required for postgresql to postgresql conversion")

    ingest_module = importlib.import_module(f".services.ingest.postgresql", package="BackEnd")
    ingest_function = getattr(ingest_module, "read_postgresql")
    get_table_names_func = getattr(ingest_module, "get_table_names")

    build_module = importlib.import_module(f".services.build.postgresql", package="BackEnd")
    build_function = getattr(build_module, "write_postgresql")

    table_names = get_table_names_func(spark, request.input.url, request.input.properties)

    output_url = request.output.url
    if "statementPooling=False" not in output_url:
        if "?" in output_url:
            output_url += "&statementPooling=False"
        else:
            output_url += "?statementPooling=False"

    output_properties = request.output.properties.copy()
    output_properties["prepareThreshold"] = "0"

    for table_name in table_names:
        df = ingest_function(spark, request.input.url, table_name, request.input.properties)
        
        if request.transforms:
            for transform_name in request.transforms:
                if transform_name == "drop_nulls":
                    df = transform.drop_nulls(df)
                else:
                    raise HTTPException(400, f"Unsupported transform: {transform_name}")

        build_function(df, output_url, table_name, output_properties)
