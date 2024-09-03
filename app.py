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
# app.add_middleware(AuthMiddleware)
# setup_cors(app)

# !-----------------------------------------
@app.on_event("startup")
async def startup_event():
    # loop = asyncio.get_event_loop()
    # loop.create_task(kafka_consumer_loop("kafka_per_sec_data"))
    # !這邊應該要修正，因為好像會不斷重新建立 consumer
    asyncio.create_task(per_sec_consumer_loop("kafka_per_sec_data"))
    asyncio.create_task(MA_consumer_loop("kafka_MA_data"))



# @app.on_event("startup")
# async def startup_event():
#     app.state.async_sql_pool = await build_async_sql_pool()
#     app.state.async_redis_pool = await build_async_redis_pool()
#     app.state.handle_expired_keys_task = asyncio.create_task(handle_expired_keys(app.state.async_redis_pool, app.state.async_sql_pool))  # 開啟ttl 監聽

# @app.on_event("shutdown")  
# async def shutdown_event():
# 	app.state.handle_expired_keys_task.cancel() 
# 	await app.state.handle_expired_keys_task
	
# 	app.state.async_sql_pool.close()
# 	await app.state.async_sql_pool.wait_closed()
# 	await app.state.async_redis_pool.disconnect()

# @app.middleware("http")
# async def all_db_connection(request: Request, call_next):
#     request.state.async_sql_pool = app.state.async_sql_pool
#     request.state.async_redis_pool = app.state.async_redis_pool
#     response = await call_next(request)
#     return response

# GOOGLE_SESSION_SECRET_KEY= os.getenv('GOOGLE_SESSION_SECRET_KEY')
# app.add_middleware(SessionMiddleware, secret_key=GOOGLE_SESSION_SECRET_KEY, same_site="lax", https_only=False)

# !-----------------------------------------
app.include_router(websocket_connect.router, tags=["websocket"], prefix="/stock/v1")
# app.include_router(api_attraction.router, tags=["attraction"], prefix="/tdt/v1")
# app.include_router(api_attractions.router, tags=["attraction"], prefix="/tdt/v1")

# app.include_router(api_user_post.router, tags=["user"], prefix="/tdt/v1")
# app.include_router(api_user_put.router, tags=["user"], prefix="/tdt/v1")
# app.include_router(api_user_get.router, tags=["user"], prefix="/tdt/v1")
# app.include_router(api_user_logout.router, tags=["user"], prefix="/tdt/v1")
# app.include_router(api_user_google.router, tags=["user"], prefix="/tdt/v1")

# app.include_router(api_booking_post.router, tags=["booking"], prefix="/tdt/v1")
# app.include_router(api_booking_get.router, tags=["booking"], prefix="/tdt/v1")
# app.include_router(api_booking_delete.router, tags=["booking"], prefix="/tdt/v1")

# app.include_router(api_orders_post.router, tags=["order"], prefix="/tdt/v1")
# app.include_router(api_order_get.router, tags=["order"], prefix="/tdt/v1")
# app.include_router(api_orders_all.router, tags=["order"], prefix="/tdt/v1")

# app.include_router(api_user_reset_password.router, tags=["reset_password"], prefix="/tdt/v1")
# app.include_router(api_user_reset_send_email.router, tags=["reset_password"], prefix="/tdt/v1")
# app.include_router(api_user_reset_url.router, tags=["reset_password"], prefix="/tdt/v1")


# !-----------------------------------------
html_router = APIRouter(prefix="/stock/v1")

@html_router.get("/", include_in_schema=False)
async def index(request: Request):
	return FileResponse("./static/index.html", media_type="text/html")
# @html_router.get("/attraction/{id}", include_in_schema=False)
# async def api_attraction(request: Request, id: int):
# 	return FileResponse("./static/attraction.html", media_type="text/html")
# @html_router.get("/booking", include_in_schema=False)
# async def booking(request: Request):
# 	return FileResponse("./static/booking.html", media_type="text/html")
# @html_router.get("/thankyou", include_in_schema=False)
# async def thankyou(request: Request):
# 	return FileResponse("./static/thankyou.html", media_type="text/html")
# @html_router.get("/history_orders", include_in_schema=False)
# async def history_orders(request: Request):
# 	return FileResponse("./static/history_orders.html", media_type="text/html")

app.include_router(html_router)