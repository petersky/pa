"""HTTPS delivery pinned to a public resolved address; no redirects or proxies."""
import http.client
import ipaddress
import json
import socket
import ssl
from urllib.parse import urlsplit
from pa.scheduling.errors import DeliveryError


def send_webhook(target, envelope, occurrence_id):
    url = urlsplit(target)
    if url.scheme != 'https' or not url.hostname or url.username or url.password or url.fragment:
        raise DeliveryError('webhook_requires_https_without_embedded_credentials')
    port = url.port or 443
    addresses = socket.getaddrinfo(url.hostname, port, type=socket.SOCK_STREAM)
    if not addresses or any(not ipaddress.ip_address(row[4][0]).is_global for row in addresses):
        raise DeliveryError('webhook_requires_public_destination')
    address = addresses[0][4][0]

    class PinnedHTTPS(http.client.HTTPSConnection):
        def connect(self):
            self.sock = socket.create_connection((address, port), timeout=15)
            self.sock = ssl.create_default_context().wrap_socket(self.sock, server_hostname=url.hostname)

    connection = PinnedHTTPS(url.hostname, port, timeout=15)
    try:
        path = url.path or '/'
        if url.query:
            path += '?' + url.query
        connection.request('POST', path, json.dumps(envelope, sort_keys=True).encode(),
                           {'Content-Type':'application/json', 'Idempotency-Key':occurrence_id})
        response = connection.getresponse()
        # Success is destination HTTP acceptance. No response-body interpretation.
        if not 200 <= response.status < 300:
            raise DeliveryError(f'webhook_http_{response.status}')
        return {'accepted':True}
    finally:
        connection.close()
