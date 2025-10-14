from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

from backend.crud import get_top_students, get_student_by_name
from backend.ai_query import ask_question

# Initialize FastAPI
app = FastAPI(title="Training Institute Chatbot API")

# ✅ CORS Configuration (must be placed BEFORE any routes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://jetkinggptt.netlify.app",  # your frontend domain
        "http://localhost:5173",            # local dev frontend
        "http://localhost:5174",
        "*"                                 # fallback (optional — for testing only)
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Request model
class QuestionRequest(BaseModel):
    question: str

# ✅ Routes
@app.get("/")
def root():
    return {"message": "Welcome to Training Institute Chatbot API"}

@app.get("/top-students/")
def top_students(limit: int = 5):
    return get_top_students(limit)

@app.get("/student/{name}")
def student_info(name: str):
    student = get_student_by_name(name)
    return student or {"error": "Student not found"}

@app.post("/ask/")
async def ask_route(req: QuestionRequest):
    """Ask chatbot a natural language question"""
    return ask_question(req.question)
