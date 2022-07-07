import logging, os, json
from hashlib import sha256

from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from aio_producer import AIOProducer
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette import status
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from confluent_kafka.serialization import StringSerializer

from schema import DataLayer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

load_dotenv(verbose=True)

middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
]

# Use token based authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def auth_request(token: str = Depends(oauth2_scheme)) -> bool:

    authenticated = (
        token
        == sha256(
            f'{os.environ["API_KEY"]}{os.environ["API_SECRET"]}'.encode("utf-8")
        ).hexdigest()
    )

    return authenticated


def verify_host(request: Request) -> bool:

    allowed_hosts = json.loads(os.environ["ALLOWED_HOSTS"])
    authorized = (
        "origin" in request.headers.keys() and request.headers["origin"] in allowed_hosts
    )

    return authorized


app = FastAPI(middleware=middleware)


@app.on_event("startup")
async def startup_event():
    global producer

    producer = AIOProducer(
        {
            "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
            "linger.ms": int(os.environ["TOPIC_GOOGLE_ANALYTICS_LINGER_MS"]),
            "enable.idempotence": os.environ[
                "TOPIC_GOOGLE_ANALYTICS_ENABLE_IDEMPOTENCE"
            ],
            "max.in.flight.requests.per.connection": int(
                os.environ["TOPIC_GOOGLE_ANALYTICS_INFLIGHT_REQS"]
            ),
            "acks": os.environ["TOPIC_GOOGLE_ANALYTICS_ACKS"],
            "key.serializer": StringSerializer("utf_8"),
            "partitioner": os.environ["TOPIC_GOOGLE_ANALYTICS_PARTIOTIONER"],
        }
    )

    client = AdminClient({"bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"]})
    topic = NewTopic(
        os.environ["TOPIC_GOOGLE_ANALYTICS_NAME"],
        num_partitions=int(os.environ["TOPIC_GOOGLE_ANALYTICS_PARTITIONS"]),
        replication_factor=int(os.environ["TOPIC_GOOGLE_ANALYTICS_REPLICAS"]),
    )
    try:
        futures = client.create_topics([topic])
        for topic_name, future in futures.items():
            future.result()
            logger.info(f"Created topics {topic_name}")
    except Exception as e:
        logger.warning(e)


class ProducerCallback:
    def __init__(self, dataLayer):
        self.dataLayer = dataLayer

    def __call__(self, err, msg):
        if err:
            logger.error(f"Failed to produce {self.dataLayer}", exc_info=err)
        else:
            logger.info(
                f"""
        Successfully produced {self.dataLayer}
        to partition {msg.partition()}
        at offset {msg.offset()}
      """
            )


@app.post(
    "/api/producer/ga-datalayer",
    status_code=201
    # , response_model=DataLayer
)
async def send_analytics_dataLayer(
    dataLayer: DataLayer,
    authorized: bool = Depends(verify_host),
    authenticated: bool = Depends(auth_request),
):

    if not authorized and not authenticated:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
        )
    try:
        result = await producer.produce(
            topic=os.environ["TOPIC_GOOGLE_ANALYTICS_NAME"],
            # key=dataLayer.hitType.lower().replace(r"s+", "-").encode("utf-8"), # uncomment to send a key value
            value=dataLayer.json(),
        )
        return {"timestamp": result.timestamp()}
    except KafkaException as ex:
        raise HTTPException(status_code=500, detail=ex.args[0].str())
