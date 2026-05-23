from nautilus_trader.model.identifiers import ClientId, Venue

THETADATA = "THETADATA"
THETADATA_VENUE = Venue(THETADATA)
THETADATA_CLIENT_ID = ClientId(THETADATA)

# ThetaTerminal v3 default ports — see docs/architecture/notes/thetadata-wire-format.md.
# v3 dropped /v2/* entirely; HTTP port also moved (v2 = 25510, v3 = 25503).
THETADATA_DEFAULT_HTTP_URL = "http://127.0.0.1:25503"
THETADATA_DEFAULT_WS_URL = "ws://127.0.0.1:25520/v1/events"
