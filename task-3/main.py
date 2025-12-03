from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse
import math

app = FastAPI()


def lcm(a: int, b: int) -> int:
    if a == 0 or b == 0:
        return 0
    return abs(a * b) // math.gcd(a, b)


@app.get("/kalandarovj70_gmail_com", response_class=PlainTextResponse)
def get_lcm(x: str = Query(...), y: str = Query(...)):
    # Validate: must be non-negative integers
    if not (x.isdigit() and y.isdigit()):
        return "NaN"
    x_int = int(x)
    y_int = int(y)
    return str(lcm(x_int, y_int))
