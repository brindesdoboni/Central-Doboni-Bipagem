# -*- coding: utf-8 -*-
"""Envia o lote do dia (spec.py da folha de gravacao) para a Central Boni.

Uso:  python3 enviar_lote.py spec.py [saida.pdf] --url https://SEU-APP.up.railway.app --token SEU_TOKEN
      (ou defina CENTRAL_URL e CENTRAL_TOKEN no ambiente)

spec.py: P=[(pedido, canal ER/TT/SP, tipo G/A/C/E/N, [(sku,cor,nome)], fonte, obs)]
saida.pdf (opcional): as etiquetas geradas; dele saem o numero da ETIQUETA e o rastreio
de cada pedido, para o bipe funcionar tambem pelo codigo de barras do rastreio.
"""
import json, os, re, runpy, sys, urllib.request

CANAIS = {"ER": "ENTREGA RAPIDA", "TT": "TIKTOK", "SP": "SHOPEE"}
RX_RASTREIO = [r"\b(BR\d{12,14}[A-Z]?)\b", r"\b([A-Z]{2}\d{9}BR)\b", r"\b(88\d{10,14})\b"]


def textos_pdf(caminho):
    try:
        import pypdfium2 as pdfium
        pdf = pdfium.PdfDocument(caminho)
        out = []
        for i in range(len(pdf)):
            tp = pdf[i].get_textpage()
            out.append(tp.get_text_range() or "")
            tp.close()
        return out
    except ImportError:
        from pypdf import PdfReader
        return [p.extract_text() or "" for p in PdfReader(caminho).pages]


def montar(spec_path, pdf_path=None):
    P = runpy.run_path(spec_path)["P"]
    info = {}
    if pdf_path:
        compacto = lambda s: re.sub(r"[^A-Z0-9]", "", s.upper())
        paginas = [(t, compacto(t)) for t in textos_pdf(pdf_path)]
        for reg in P:
            ped = str(reg[0]); pc = compacto(ped)
            for t, tc in paginas:
                if pc and pc in tc:
                    d = info.setdefault(ped, {"etiquetas": [], "rastreio": ""})
                    m = re.search(r"ETIQUETA\s*N?[ºo°.]?\s*(\d+)", t, re.I)
                    if m and int(m.group(1)) not in d["etiquetas"]:
                        d["etiquetas"].append(int(m.group(1)))
                    for rx in RX_RASTREIO:
                        r = re.search(rx, t)
                        if r and not d["rastreio"]:
                            d["rastreio"] = r.group(1)
    itens = []
    vistos = {}
    for reg in P:
        pedido, canal, tipo, pecas, fonte, obs = (list(reg) + [None] * 6)[:6]
        pedido = str(pedido)
        n = vistos[pedido] = vistos.get(pedido, 0) + 1
        pecas = pecas or []
        d = info.get(pedido, {})
        etqs = d.get("etiquetas") or []
        itens.append({
            "pedido": pedido, "seq": n, "canal": CANAIS.get(canal, canal),
            "tipo": tipo, "personalizado": tipo != "N",
            "sku": " + ".join(dict.fromkeys(str(p[0]) for p in pecas if p)),
            "cor": " + ".join(dict.fromkeys(str(p[1]) for p in pecas if len(p) > 1 and p[1])),
            "nomes": [str(p[2]) for p in pecas if len(p) > 2 and p[2]],
            "fonte": fonte or "", "obs": obs or "",
            "etiqueta": etqs[n - 1] if len(etqs) >= n else (etqs[0] if etqs else None),
            "rastreio": d.get("rastreio", ""),
        })
    return itens


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    opt = dict(zip(sys.argv[1::1], sys.argv[2::1]))
    url = opt.get("--url") or os.environ.get("CENTRAL_URL")
    token = opt.get("--token") or os.environ.get("CENTRAL_TOKEN")
    args = [a for a in args if a not in (url, token)]
    if not args or not url or not token:
        print(__doc__); sys.exit(2)
    itens = montar(args[0], args[1] if len(args) > 1 else None)
    corpo = json.dumps({"itens": itens}).encode()
    req = urllib.request.Request(url.rstrip("/") + "/api/lotes", data=corpo, method="POST",
                                 headers={"Content-Type": "application/json", "X-Token": token})
    with urllib.request.urlopen(req, timeout=60) as r:
        res = json.loads(r.read())
    sem_etq = sum(1 for i in itens if not i["etiqueta"])
    print(f"Central Boni: {res['novos']} novo(s), {res['atualizados']} atualizado(s) no lote {res['lote']}."
          + (f" {sem_etq} sem numero de etiqueta (bipe pelo pedido/rastreio)." if sem_etq else ""))


if __name__ == "__main__":
    main()
