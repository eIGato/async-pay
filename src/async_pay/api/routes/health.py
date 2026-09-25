import logging

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from async_pay.api.deps import get_session

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(session: AsyncSession = Depends(get_session)) -> JSONResponse:
    """Liveness/readiness probe. Deliberately unauthenticated so orchestrators
    can call it without holding the API key."""
    try:
        await session.execute(text("SELECT 1"))
    except Exception:
        logger.exception("health check failed: database unreachable")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unhealthy", "database": "unreachable"},
        )
    return JSONResponse(content={"status": "ok", "database": "ok"})
