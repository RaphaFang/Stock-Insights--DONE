from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path.startswith("/tdt/v1/static/") or request.url.path.startswith("/tdt/v1/api") :
            return await call_next(request)
        if request.url.path.startswith("/tdt/v1/attraction/") or request.url.path.startswith("/tdt/v1/auth/") : 
            return await call_next(request)
        if request.url.path not in ["/tdt/v1/", "/tdt/v1/booking", "/tdt/v1/thankyou", '/tdt/v1/history_orders', '/tdt/v1/docs', '/tdt/v1/openapi.json']:
            return RedirectResponse(url='/tdt/v1/')
        return await call_next(request)