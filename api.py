from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from connection import send_to_event_hub
from data import generate_banking_transaction

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/")
def banking_home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


@app.get("/transaction")
def make_transaction(request: Request):  
    transaction = generate_banking_transaction()
    result = send_to_event_hub(transaction)
    return templates.TemplateResponse("confirmation.html", {"request": request})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)