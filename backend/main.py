# # from fastapi import FastAPI
# # from backend.crud import get_top_students, get_student_by_name

# # app = FastAPI(title="Training Institute Chatbot API")

# # @app.get("/")
# # def root():
# #     return {"message": "Welcome to Training Institute Chatbot API"}

# # @app.get("/top-students/")
# # def top_students(limit: int = 5):
# #     return get_top_students(limit)

# # @app.get("/student/{name}")
# # def student_info(name: str):
# #     student = get_student_by_name(name)
# #     if student:
# #         return student
# #     return {"error": "Student not found"}




# from fastapi import FastAPI
# from pydantic import BaseModel
# from backend.crud import get_top_students, get_student_by_name
# from backend.ai_query import ask_question

# app = FastAPI(title="Training Institute Chatbot API")

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
# def ask_ai(req: QuestionRequest):
#     """Ask chatbot a natural language question"""
#     return ask_question(req.question)



from fastapi import FastAPI
from pydantic import BaseModel
from backend.crud import get_top_students, get_student_by_name
from backend.ai_query import ask_question
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Training Institute Chatbot API")


# ✅ Allowed origins
allow_origins=["https://jetkinggptt.netlify.app/"]
# ✅ Allow all origins (not recommended for production)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all domains
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],  # Allow all headers
)

class QuestionRequest(BaseModel):
    question: str
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
def ask_ai(req: QuestionRequest):
    """Ask chatbot a natural language question"""
    return ask_question(req.question)
