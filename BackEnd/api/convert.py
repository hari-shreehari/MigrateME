from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
import importlib

from ..utils.spark import get_spark_session

router = APIRouter()


class InputSource(BaseModel):
    type: str = Field(..., description="Input format, e.g. parquet, postgresql")
    path: Optional[str] = Field(None, description="Path if file-based input")
    url:  Optional[str] = Field(None, description="DB URL if input is a database")
    properties: Optional[dict] = Field(
        default_factory=dict,
        description="Extra DB connection props: user, password, driver…",
    )


class OutputTarget(BaseModel):
    type: str = Field(..., description="Desired output format, e.g. parquet, csv, sqlite")
    path: Optional[str] = Field(None, description="Folder/filename for file output")
    url:  Optional[str] = Field(None, description="DB URL if output is a database")
    properties: Optional[dict] = Field(
        default_factory=dict,
        description="Extra DB connection props: user, password, driver…",
    )


class ConversionRequest(BaseModel):
    input:  InputSource
    output: OutputTarget
    transforms: Optional[list[str]] = Field(None, description="List of transformations to apply")

@router.post("/convert", tags=["Convert"])
async def convert_file(request: ConversionRequest):
    spark = get_spark_session()
    
    try:
        input_type = request.input.type.lower()
        output_type = request.output.type.lower()

        if input_type == "postgresql" and output_type == "postgresql":
            converter_module_name = "postgresql_to_postgresql"
        elif input_type == "mongodb" and output_type == "mongodb":
            converter_module_name = "mongodb_to_mongodb"
        elif input_type == "folder" and output_type == "postgresql":
            converter_module_name = "folder_to_postgresql"
        elif input_type == "folder" and output_type == "mongodb":
            converter_module_name = "folder_to_mongodb"
        elif input_type == "folder" and output_type == "sqlite":
            converter_module_name = "folder_to_sqlite"
        elif input_type == "postgresql":
            converter_module_name = "postgresql_to_file"
        elif input_type == "mongodb":
            converter_module_name = "mongodb_to_file"
        elif input_type == "sqlite":
            converter_module_name = "sqlite_to_file"
        else:
            converter_module_name = "file_to_file"

        try:
            converter_module = importlib.import_module(f".services.converters.{converter_module_name}", package="BackEnd")
            converter_function = getattr(converter_module, "convert")
        except (ImportError, AttributeError):
            raise HTTPException(400, f"Unsupported conversion from '{input_type}' to '{output_type}'")

        converter_function(spark, request)

        return {
            "status": "success",
            "input_type": request.input.type,
            "input_loc": request.input.path or request.input.url,
            "output_type": request.output.type,
            "output_loc": request.output.path or request.output.url,
        }
    except Exception as e:
        raise HTTPException(500, f"An error occurred during data conversion: {e}")
