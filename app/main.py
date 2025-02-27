import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from app.services.process_messages import process_request
from app.services import dispatchers
from app.core import utils, gundi

# For running behind a proxy, we'll want to configure the root path for OpenAPI browser.
root_path = os.environ.get("ROOT_PATH", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup Hook
    yield
    # Shotdown Hook
    await gundi.portal_client.close()
    await utils.redis_client.close()
    await dispatchers.gcp_storage.close()


app = FastAPI(
    title="Gundi TrapTagger Dispatcher",
    description="Service that sends data to TrapTagger",
    version="1",
    lifespan=lifespan,
)


origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = logging.getLogger(__name__)


@app.get(
    "/",
    tags=["health-check"],
    summary="Check that the service is healthy",
)
def health_check(
    request: Request,
):
    return {"status": "healthy"}


@app.post(
    "/",
    summary="Process a message from Pub/Sub",
)
async def process_cloud_event(
    request: Request,
):
    body = await request.body()
    headers = request.headers
    print(f"Message Received.\n RAW body: {body}\n headers: {headers}")
    return await process_request(request=request)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):

    logger.debug(
        "Failed handling body: %s",
        jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
    )

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
    )
