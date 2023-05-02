from fastapi import APIRouter

from dbs_assignment.endpoints import connection

router = APIRouter()
router.include_router(connection.router, tags=["Flight_controller"])
