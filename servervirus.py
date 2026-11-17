import os
from cloudlink import cloudlink

cl = cloudlink()
server = cl.server(logs=True)

puerto = int(os.environ.get("PORT", 8080))
server.run(ip="0.0.0.0", port=puerto)
