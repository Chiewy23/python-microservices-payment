import requests, time
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.background import BackgroundTasks
from redis_om import get_redis_connection, HashModel
from starlette.requests import Request

# RUN: uvicorn main:app --reload --port=8001

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# In reality this should be a different database,
# since each microservice should have its own database.
# Read these from config file eventually.
redis = get_redis_connection(
    host="redis-14148.c300.eu-central-1-1.ec2.cloud.redislabs.com",
    port="14148",
    password="6sIkabUNkuRt9lwqzCLX0Uidfx7vlW4R",
    decode_responses=True
)


class Order(HashModel):
    product_id: str
    price: float
    fee: float
    total: float
    quantity: int
    status: str  # pending, completed, refunded

    class Meta:
        database = redis


@app.get('/orders/{pk}')
def get(pk: str):
    return Order.get(pk)


@app.post('/orders')
async def create(request: Request, background_tasks: BackgroundTasks):  # id, quantity
    body = await request.json()

    req = requests.get('http://localhost:8000/products/%s' % body['id'])
    product = req.json()

    order = Order(
        product_id=body['id'],
        price=product['price'],
        fee=0.2 * product['price'],
        total=1.2 * product['price'],
        quantity=body['quantity'],
        status='pending'
    )

    order.save()
    background_tasks.add_task(order_completed, order)

    return order


def order_completed(order: Order):
    order.status = 'completed'
    order.save()
    redis.xadd('order_completed', order.dict(), '*')
