# -*- coding: utf-8 -*-
"""Manda um PDF de etiquetas direto para a Central Boni (sem e-mail, sem painel).
Uso: python3 enviar_central_pdf.py etiquetas.pdf [central_token.txt]
O arquivo central_token.txt tem so o valor da variavel API_TOKEN do Railway."""
import base64, json, os, sys, urllib.request

URL = os.environ.get("CENTRAL_URL", "https://operacaodoboni.up.railway.app")


def main():
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(2)
    pdf = sys.argv[1]
    arq_tok = sys.argv[2] if len(sys.argv) > 2 else "central_token.txt"
    token = os.environ.get("CENTRAL_TOKEN") or open(arq_tok, encoding="utf-8-sig").read().strip()
    corpo = json.dumps({"nome": os.path.basename(pdf), "dados": base64.b64encode(open(pdf, "rb").read()).decode()}).encode()
    req = urllib.request.Request(URL.rstrip("/") + "/api/importar-pdf", data=corpo, method="POST",
                                 headers={"Content-Type": "application/json", "X-Token": token})
    with urllib.request.urlopen(req, timeout=180) as r:
        res = json.loads(r.read())
    if not res.get("ok"):
        print("ERRO Central:", res.get("erro")); sys.exit(1)
    print(f"Central Boni: {res['etiquetas']} etiqueta(s) lida(s) - {res['novos']} nova(s), {res['atualizados']} ja existiam.")


if __name__ == "__main__":
    main()
