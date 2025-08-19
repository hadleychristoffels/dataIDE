from fastapi import FastAPI

from dataide.routes import router


def create_app() -> FastAPI:
    app = FastAPI(title="dataIDE")
    app.include_router(router)
    return app


app = create_app()


