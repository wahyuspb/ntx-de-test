from fastapi import FastAPI
import hashlib

app = FastAPI()


@app.post("/predict")
async def predict(text: str) -> int:
    hashed = int(hashlib.md5(text.encode()).hexdigest(), 16)
    result = hashed % 4

    return result


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=6000)
