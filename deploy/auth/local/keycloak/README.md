# Local Keycloak for IAM

This setup is for local development only. It imports a realm named `iam-dev`,
a password-grant test client, and a test user. The client adds `iam-api` to the
access token audience.

Start Keycloak from the repository root:

```sh
docker compose -f deploy/auth/local/keycloak/docker-compose.yaml up
```

Keycloak listens on `http://localhost:8080`. Its development admin account is
`admin` / `admin`. The imported test user is `iam-test` /
`iam-test-password`.

The IAM verifier settings are:

```sh
export HTTP_SERVER_OIDC_ISSUER=http://localhost:8080/realms/iam-dev
export HTTP_SERVER_OIDC_JWKS_URL=http://localhost:8080/realms/iam-dev/protocol/openid-connect/certs
export HTTP_SERVER_OIDC_AUDIENCE=iam-api
```

Get an access token (requires `jq`):

```sh
ACCESS_TOKEN="$(curl -fsS -X POST \
  http://localhost:8080/realms/iam-dev/protocol/openid-connect/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'grant_type=password' \
  --data-urlencode 'client_id=iam-test' \
  --data-urlencode 'username=iam-test' \
  --data-urlencode 'password=iam-test-password' | jq -r .access_token)"
```

The token should have `iss` equal to the configured issuer and `aud` containing
`iam-api`. Keycloak publishes the matching signing key at the configured JWKS
URL.

To start IAM locally, configure a reachable MySQL database and TLS certificate
and key first. For a temporary self-signed certificate, for example:

```sh
mkdir -p /tmp/iam-local
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout /tmp/iam-local/iam.key \
  -out /tmp/iam-local/iam.crt \
  -days 2 -subj '/CN=localhost'
```

Then start IAM with `IAM_CONFIG_FILE=cmd/iam/.local/.config.env`, the three OIDC
variables above, and `HTTP_SERVER_CERT_FILE=/tmp/iam-local/iam.crt` plus
`HTTP_SERVER_KEY_FILE=/tmp/iam-local/iam.key`. The current config file leaves
`SQL_DATABASE_NAME` empty, so set it to an existing local database before
starting IAM.

To verify JWT authentication without querying a device, send a valid token and
an empty request body. IAM should return `400 invalid client request`; a
missing or invalid token should return `401`:

```sh
curl -k -i -X POST https://localhost:2008/iam/v2/connection-ticket \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{}'
```

For a device owned by the token subject, request a signed connection ticket:

```sh
curl -k -X POST https://localhost:2008/iam/v2/connection-ticket \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"device_id":"42"}'
```

Provision the Hub keyring separately and set `HUB_NOISE_DESCRIPTOR_FILE` to its
public descriptor. No client X25519 key is sent to IAM. The response contains
`ticket` and `hub`; use the trusted Hub public key for Noise NK over TCP.
