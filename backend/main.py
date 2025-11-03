# from fastapi import FastAPI
# from pydantic import BaseModel
# from fastapi.middleware.cors import CORSMiddleware

# from backend.crud import get_top_students, get_student_by_name
# from backend.ai_agent import ask_question  # <-- updated import

# # Initialize FastAPI
# app = FastAPI(title="Training Institute Chatbot API")

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  # Allow all origins
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # ✅ Request model
# class QuestionRequest(BaseModel):
#     question: str

# @app.get("/")
# def root():
#     return {"message": "Welcome to Training Institute Chatbot API"}

# @app.get("/top-students/")
# def top_students(limit: int = 5):
#     return get_top_students(limit)

# @app.get("/student/{name}")
# def student_info(name: str):
#     student = get_student_by_name(name)
#     return student or {"error": "Student not found"}

# @app.post("/ask/")
# async def ask_route(payload: QuestionRequest):
#     query = payload.question
#     return ask_question(query)




from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional
import logging

# Import your modules
from ai_agent import ask_question, health_check

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(
    title="Jetking Training Institute Chatbot API",
    description="AI-powered chatbot for querying training institute data",
    version="2.0.0"
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================================
# Request/Response Models
# =====================================

class QuestionRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=500, description="Natural language question")
    context: Optional[str] = Field(None, max_length=1000, description="Additional context for the question")

    class Config:
        json_schema_extra = {
            "example": {
                "question": "How many branches are there in Mumbai?",
                "context": "Focus on active branches only"
            }
        }

class QuestionResponse(BaseModel):
    success: bool
    question: str
    answer: Optional[str] = None
    error: Optional[str] = None
    source: str
    sql_used: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    database: bool
    openai: bool
    agent: bool
    message: str

# =====================================
# API Endpoints
# =====================================

@app.get("/", tags=["Root"])
def root():
    """Root endpoint - API information"""
    return {
        "message": "Welcome to Jetking Training Institute Chatbot API",
        "version": "2.0.0",
        "endpoints": {
            "health": "/health",
            "ask_question": "/ask/ (POST)",
            "quick_query": "/quick/ (GET)",
            "docs": "/docs"
        }
    }

@app.get("/health", response_model=HealthResponse, tags=["Health"])
def health():
    """
    Check API and service health
    """
    try:
        health_status = health_check()
        all_healthy = all(health_status.values())
        
        return {
            "status": "healthy" if all_healthy else "degraded",
            "database": health_status["database"],
            "openai": health_status["openai"],
            "agent": health_status["agent"],
            "message": "All systems operational" if all_healthy else "Some services are down"
        }
    except Exception as e:
        logger.error(f"Health check error: {e}")
        raise HTTPException(status_code=500, detail="Health check failed")

@app.post("/ask/", response_model=QuestionResponse, tags=["Query"])
async def ask_route(payload: QuestionRequest):
    """
    Ask a question using natural language
    
    - **question**: Your natural language question
    - **context**: Optional additional context to guide the answer
    """
    try:
        logger.info(f"Received question: {payload.question}")
        
        # Process the question
        result = ask_question(payload.question, payload.context)
        
        if not result:
            raise HTTPException(status_code=500, detail="Failed to process question")
        
        return QuestionResponse(
            success=result.get("success", False),
            question=result.get("question", payload.question),
            answer=result.get("answer"),
            error=result.get("error"),
            source=result.get("source", "unknown"),
            sql_used=result.get("sql_used")
        )
        
    except Exception as e:
        logger.error(f"Error in ask_route: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/quick/", tags=["Query"])
async def quick_query(
    q: str = Query(..., min_length=3, max_length=500, description="Your question")
):
    """
    Quick query endpoint using GET method
    
    Example: /quick/?q=How many branches are there?
    """
    try:
        logger.info(f"Quick query: {q}")
        result = ask_question(q)
        
        if not result:
            raise HTTPException(status_code=500, detail="Failed to process question")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in quick_query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =====================================
# Optional: Statistics Endpoints
# =====================================

@app.get("/stats/summary", tags=["Statistics"])
async def get_summary_stats():
    """
    Get summary statistics about the database
    """
    try:
        questions = [
            "How many branches are there?",
            "How many active campaigns?",
            "Total number of calls?",
            "Total cities in database?"
        ]
        
        results = {}
        for q in questions:
            result = ask_question(q)
            if result.get("success"):
                results[q] = result.get("answer")
        
        return {
            "success": True,
            "statistics": results
        }
    except Exception as e:
        logger.error(f"Error in get_summary_stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =====================================
# Error Handlers
# =====================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Custom HTTP exception handler"""
    return {
        "success": False,
        "error": exc.detail,
        "status_code": exc.status_code
    }

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """General exception handler"""
    logger.error(f"Unhandled exception: {exc}")
    return {
        "success": False,
        "error": "An unexpected error occurred",
        "status_code": 500
    }

# =====================================
# Startup/Shutdown Events
# =====================================

@app.on_event("startup")
async def startup_event():
    """Actions to perform on startup"""
    logger.info("🚀 Starting Jetking Chatbot API...")
    logger.info("✅ API is ready to receive requests")

@app.on_event("shutdown")
async def shutdown_event():
    """Actions to perform on shutdown"""
    logger.info("🛑 Shutting down Jetking Chatbot API...")

# =====================================
# Run the application
# =====================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Disable in production
        log_level="info"
    )