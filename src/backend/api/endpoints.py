from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
from datetime import datetime
from backend import generate_users

app = FastAPI(
    title="Fake Data API",
    description="API para gerar dados fictícios para pipelines de dados",
    version="1.0.0"
)

# Configuração do CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health", tags=["Health"])
async def health_check():
    """Endpoint para verificar a saúde da API."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

@app.get(
    "/users/",
    response_model=List[Dict],
    summary="Gerar usuários fictícios",
    description="Endpoint para gerar uma lista de usuários fictícios com dados aleatórios",
    responses={
        200: {
            "description": "Lista de usuários gerada com sucesso",
            "content": {
                "application/json": {
                    "example": [{"id": 1, "name": "John Doe", "email": "john@example.com"}]
                }
            }
        },
        400: {
            "description": "Parâmetro inválido",
            "content": {
                "application/json": {
                    "example": {"detail": "O número máximo de registros é 1000"}
                }
            }
        }
    }
)
async def get_users(n: int = 10):
    """Endpoint para gerar usuários fictícios."""
    if n < 1:
        raise HTTPException(status_code=400, detail="O número de registros deve ser maior que 0")
    if n > 1000:
        raise HTTPException(status_code=400, detail="O número máximo de registros é 1000")
    return generate_users(n)

@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    """Endpoint personalizado para a documentação da API."""
    from fastapi.openapi.docs import get_swagger_ui_html
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="Fake Data API - Documentação",
        swagger_js_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js",
        swagger_css_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css",
    )