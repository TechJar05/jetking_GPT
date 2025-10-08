from fastapi import FastAPI
from backend.crud import get_top_students, get_student_by_name

app = FastAPI(title="Training Institute Chatbot API")

@app.get("/")
def root():
    return {"message": "Welcome to Training Institute Chatbot API"}

@app.get("/top-students/")
def top_students(limit: int = 5):
    return get_top_students(limit)

@app.get("/student/{name}")
def student_info(name: str):
    student = get_student_by_name(name)
    if student:
        return student
    return {"error": "Student not found"}
