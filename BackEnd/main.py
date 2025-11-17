from fastapi import FastAPI
from .api import root, convert, upload

app = FastAPI(title="File Conversion & Visualization API")

app.include_router(root.router)
app.include_router(convert.router)
app.include_router(upload.router)

@app.on_event("startup")
async def startup_event() -> None:
    print("Starting up application...")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    print("Shutting down application...")
