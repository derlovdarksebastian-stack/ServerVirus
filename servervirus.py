import os
from cloudlink import server
from cloudlink.server.protocols import clpv4

srv = server()
srv.logging.basicConfig(level=srv.logging.DEBUG)
protocolo = clpv4(srv)

puerto = int(os.environ.get("PORT", 8080))
srv.run(ip="0.0.0.0", port=puerto)
