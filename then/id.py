import datetime
import uuid
import base64

def spec_uuid():
    now = datetime.datetime.now()

    now.year
    now.month

def base64_uuid():
    x = uuid.uuid4()
    return x.hex, base64.b64encode (x.bytes)

print(base64_uuid())