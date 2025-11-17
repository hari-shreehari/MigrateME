from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pathlib import Path
import shutil
import json
import tempfile
from ..api.convert import convert_file, ConversionRequest, InputSource, OutputTarget
from ..utils.response_helper import create_file_response

router = APIRouter()

@router.post("/upload", tags=["Upload"])
async def upload_and_convert_file(
    request: str = Form(...),
    file: UploadFile = File(...)
): 
    try:
        request_data = json.loads(request)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON request.")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        file_path = temp_path / file.filename
        
        try:
            with file_path.open("wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
        finally:
            file.file.close()
            
        input_type = request_data.get("input", {}).get("type")
        if not input_type:
            input_type = file.filename.split('.')[-1]

        output_data = request_data.get("output", {})
        output_type = output_data.get("type")
        
        output_filename = Path(request_data.get("output", {}).get("path")).name
        output_path = temp_path / output_filename

        if not output_type:
            raise HTTPException(status_code=400, detail="Output type is required.")

        conversion_request = ConversionRequest(
            input=InputSource(
                type=input_type,
                path=str(file_path)
            ),
            output=OutputTarget(
                type=output_type,
                path=str(output_path),
                properties=output_data.get("properties")
            ),
            transforms=request_data.get("transforms")
        )
        
        try:
            result = await convert_file(conversion_request)
            
            output_loc = result.get("output_loc")
            file_response = create_file_response(output_loc)
            
            if file_response:
                return file_response
            else:
                return result

        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred during conversion: {e}")
