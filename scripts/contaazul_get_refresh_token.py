"""Helper interativo pra obter o refresh_token inicial do Conta Azul (OAuth2).

Roda 1 vez por tenant. O refresh_token sai aqui e vai pra Connection do
Airflow (`<tenant_slug>__conta_azul`). Depois o connector cuida da
rotação automática a cada renovação de access_token.

Uso:
    python scripts/contaazul_get_refresh_token.py \\
        --tenant-slug luminea \\
        --client-id <client_id> \\
        --client-secret <client_secret> \\
        --redirect-uri https://contaazul.com

Modos de captura do code:
- localhost: se redirect_uri = http://localhost:<port>/<path>, sobe HTTP
  server local e captura automaticamente.
- externa: redirect_uri aponta pra URL externa (ex: https://contaazul.com,
  https://app.datajoin.io/...). User cola a URL completa depois de
  autorizar; script extrai o code.

Sem deps externas — usa só stdlib (urllib, http.server).
"""
from __future__ import annotations

import argparse
import base64
import json
import secrets
import sys
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

# Endpoints oficiais Conta Azul (OAuth2 v3).
# Doc: https://developers.contaazul.com/auth
AUTHORIZE_URL = "https://auth.contaazul.com/oauth2/authorize"
TOKEN_URL = "https://auth.contaazul.com/oauth2/token"


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--tenant-slug", required=True, help="Slug do tenant (ex: luminea). Usado pra montar a sugestao de Conn Id.")
    p.add_argument("--client-id", required=True)
    p.add_argument("--client-secret", required=True)
    p.add_argument("--redirect-uri", required=True, help="Tem que estar registrada no portal de devs Conta Azul, exatamente igual.")
    p.add_argument("--scope", default="", help="Espaco-separado. Vazio = sem param scope na URL (app usa o default cadastrado).")
    p.add_argument(
        "--authorize-url",
        default=AUTHORIZE_URL,
        help=f"Override do endpoint de authorize. Default: {AUTHORIZE_URL}",
    )
    p.add_argument(
        "--token-url",
        default=TOKEN_URL,
        help=f"Override do endpoint de token. Default: {TOKEN_URL}",
    )
    args = p.parse_args()

    # State CSRF — qualquer string aleatoria; conferimos no callback.
    state = secrets.token_urlsafe(16)

    # 1. Monta URL de authorize.
    authorize_params = {
        "response_type": "code",
        "client_id": args.client_id,
        "redirect_uri": args.redirect_uri,
        "state": state,
    }
    if args.scope:
        authorize_params["scope"] = args.scope
    authorize_url = (
        f"{args.authorize_url}?{urllib.parse.urlencode(authorize_params)}"
    )

    parsed = urlparse(args.redirect_uri)
    is_localhost = parsed.hostname in ("localhost", "127.0.0.1")

    print()
    print("=" * 72)
    print(f"Conta Azul — Authorization Code Flow ({args.tenant_slug})")
    print("=" * 72)
    print()
    print("1) Abra a URL abaixo no browser e autorize com a conta do CLIENTE")
    print(f"   final ({args.tenant_slug.upper()}, neste caso):")
    print()
    print(f"   {authorize_url}")
    print()

    # Tenta abrir automaticamente — best effort.
    try:
        webbrowser.open(authorize_url)
        print("   (browser aberto automaticamente)")
        print()
    except Exception:
        pass

    # 2. Captura o code (auto se localhost, manual senao).
    if is_localhost:
        port = parsed.port or 80
        path = parsed.path or "/"
        print(f"2) Aguardando redirect em {args.redirect_uri} ...")
        print()
        code = _capture_code_local(port, path, expected_state=state)
    else:
        code = _capture_code_manual(args.redirect_uri, expected_state=state)

    # 3. Troca code por tokens.
    print()
    print("3) Trocando code por tokens...")
    tokens = _exchange_code(
        args.token_url,
        args.client_id,
        args.client_secret,
        args.redirect_uri,
        code,
    )

    # 4. Apresenta resultado pronto pra colar na Connection do Airflow.
    refresh = tokens.get("refresh_token")
    access = tokens.get("access_token", "")
    expires = tokens.get("expires_in")

    print()
    print("=" * 72)
    print("✅ Tokens obtidos!")
    print("=" * 72)
    print()
    print(f"  access_token:  {access[:30]}{'...' if len(access) > 30 else ''}")
    print(f"  refresh_token: {refresh}")
    print(f"  expires_in:    {expires} segundos (~{int(expires)//60} min)")
    print()
    print("─" * 72)
    print("CONNECTION DO AIRFLOW")
    print("─" * 72)
    print()
    print(f"  Vá em: http://localhost:8080 → Admin → Connections → +")
    print()
    print(f"  Connection Id   : {args.tenant_slug}__conta_azul")
    print(f"  Connection Type : HTTP")
    print(f"  Login           : {args.client_id}")
    print(f"  Password        : <client_secret>")
    print(f"  Extra (cole exato):")
    print()
    extra = {"refresh_token": refresh}
    print(f"      {json.dumps(extra)}")
    print()
    print("ℹ️  refresh_token é uso ÚNICO — Conta Azul gera um novo a cada")
    print("    renovação. O connector persiste o novo automaticamente na")
    print("    Connection. Você só preenche o inicial.")
    print()
    return 0


def _capture_code_local(
    port: int, path: str, *, expected_state: str
) -> str:
    """Sobe HTTP server local pra capturar o redirect com ?code=... ."""
    captured: dict = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path != path:
                self.send_response(404)
                self.end_headers()
                return
            qs = parse_qs(parsed.query)
            captured["code"] = (qs.get("code") or [None])[0]
            captured["state"] = (qs.get("state") or [None])[0]
            captured["error"] = (qs.get("error") or [None])[0]
            captured["error_description"] = (
                qs.get("error_description") or [None]
            )[0]

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            if captured["code"]:
                self.wfile.write(
                    b"<h1>Autorizacao OK</h1>"
                    b"<p>Pode fechar esta aba e voltar ao terminal.</p>"
                )
            else:
                err = (captured.get("error") or "unknown") + ": " + (
                    captured.get("error_description") or ""
                )
                self.wfile.write(
                    f"<h1>Erro na autorizacao</h1><pre>{err}</pre>".encode()
                )

        def log_message(self, fmt, *a):  # silencia logs default
            return

    server = HTTPServer(("localhost", port), Handler)
    server.handle_request()  # bloqueia ate 1 request

    if captured.get("error"):
        print(
            f"❌ Conta Azul retornou erro: {captured['error']} — "
            f"{captured.get('error_description')}",
            file=sys.stderr,
        )
        sys.exit(1)
    if captured.get("state") != expected_state:
        print(
            "❌ State CSRF não bate — possível ataque ou redirecionamento errado.",
            file=sys.stderr,
        )
        sys.exit(1)
    if not captured.get("code"):
        print("❌ Sem `code` no callback.", file=sys.stderr)
        sys.exit(1)
    return captured["code"]


def _capture_code_manual(redirect_uri: str, *, expected_state: str) -> str:
    """Pede pro user colar a URL completa após autorizar."""
    print(
        f"2) Após autorizar, o navegador será redirecionado para algo como:"
    )
    print(f"     {redirect_uri}/?code=XXXX&state=YYYY")
    print()
    print(
        "   COLE A URL COMPLETA da barra de endereço aqui (ou só o `code`):"
    )
    print()
    raw = input("   > ").strip()
    if not raw:
        print("❌ Nada colado. Abortando.", file=sys.stderr)
        sys.exit(1)

    # URL completa: extrai code + valida state.
    if raw.startswith("http"):
        qs = parse_qs(urlparse(raw).query)
        code = (qs.get("code") or [None])[0]
        state = (qs.get("state") or [None])[0]
        error = (qs.get("error") or [None])[0]
        if error:
            err_desc = (qs.get("error_description") or [""])[0]
            print(f"❌ Conta Azul retornou erro: {error} — {err_desc}", file=sys.stderr)
            sys.exit(1)
        if not code:
            print(f"❌ Sem `code` na URL colada.", file=sys.stderr)
            sys.exit(1)
        if state != expected_state:
            print(
                "⚠️  State CSRF não bate — confira que a URL veio do redirect "
                "que VOCÊ iniciou. Continuando mesmo assim…",
                file=sys.stderr,
            )
        return code

    # Só o code (presumido).
    return raw


def _exchange_code(
    token_url: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    code: str,
) -> dict:
    """POST token_url com grant_type=authorization_code + Basic auth."""
    basic = base64.b64encode(
        f"{client_id}:{client_secret}".encode()
    ).decode("ascii")
    body = urllib.parse.urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }
    ).encode()
    req = urllib.request.Request(
        token_url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.request.HTTPError as e:
        body_str = e.read().decode(errors="replace")
        print(
            f"❌ Erro {e.code} ao trocar code por tokens:\n{body_str}",
            file=sys.stderr,
        )
        sys.exit(1)
    except urllib.request.URLError as e:
        print(f"❌ Erro de rede: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
