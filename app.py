from fastapi import *
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from routers import websocket_connect
from starlette.middleware.sessions import SessionMiddleware
import os
import asyncio
from routers.websocket_connect import per_sec_consumer_loop, MA_consumer_loop

app = FastAPI(
    docs_url="/stock/v1/docs",
    openapi_url="/stock/v1/openapi.json"
)
app.mount("/stock/v1/static", StaticFiles(directory='static'), name="static")

# !-----------------------------------------
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(per_sec_consumer_loop("kafka_per_sec_data"))
    asyncio.create_task(MA_consumer_loop("kafka_MA_data"))

# !-----------------------------------------
app.include_router(websocket_connect.router, tags=["websocket"], prefix="/stock/v1")

# !-----------------------------------------
html_router = APIRouter(prefix="/stock/v1")

@html_router.get("/", include_in_schema=False)
async def index(request: Request):
	return FileResponse("./static/index.html", media_type="text/html")

app.include_router(html_router)