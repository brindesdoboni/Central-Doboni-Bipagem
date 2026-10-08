"""Monta o PDF de impressão do lote e o CSV do LightBurn.

Ordem: ENTREGA RÁPIDA -> TIKTOK -> SHOPEE.
Em cada canal: personalizados (paisagem, numerados, por SKU) depois SEM PERSONALIZAR (retrato original, por SKU).
Cada seção começa com uma folha separadora (retrato, texto grande).
"""
import io, re, sys, importlib.util
import pikepdf
try:
    import pdfplumber
except Exception:
    pdfplumber = None
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
import os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from etiquetas import W_L, H_L
from painel2 import painel, lote_bonito

CANAIS = [("ER", "ENTREGA RÁPIDA"), ("TT", "TIKTOK"), ("SP", "SHOPEE")]
TIPO_RANK = {"G": 0, "A": 1, "C": 2, "E": 3}


PART = {"de","da","do","das","dos","e","di","du"}
def cap_nome(n, obs=""):
    if re.search(r"mai[uú]scul|caixa alta", obs or "", re.I):
        return n
    out = []
    for i, w in enumerate(n.split(" ")):
        if not w or w.startswith("@"):
            out.append(w); continue
        lw = w.lower()
        if i > 0 and lw in PART:
            out.append(lw); continue
        out.append(lw[:1].upper() + lw[1:])
    return " ".join(out)


def load_prateleiras():
    """prateleiras.csv (ao lado do script): uma linha por SKU -> 'sku;prateleira' na ORDEM das prateleiras."""
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prateleiras.csv")
    ordem, prat = {}, {}
    if os.path.exists(p):
        for i, ln in enumerate(open(p, encoding="utf-8-sig")):
            parts = [x.strip() for x in re.split(r"[;,\t]", ln.strip()) if x.strip()]
            if len(parts) < 2 or parts[0].lower() == "sku": continue
            k = parts[0].lstrip("0").upper()
            ordem.setdefault(k, i); prat.setdefault(k, parts[1])
    return ordem, prat


PRAT_ORDEM, PRAT_NOME = load_prateleiras()
def sku_key(sku):
    return str(sku).split("-")[0].strip().lstrip("0").upper()
def prat_de(skus):
    ps = [PRAT_NOME.get(sku_key(s)) for s in sorted(skus, key=lambda s: PRAT_ORDEM.get(sku_key(s), 10**6))]
    ps = [p for p in dict.fromkeys(ps) if p]
    return " / ".join(ps)
def prat_rank(p):
    rs = [PRAT_ORDEM.get(sku_key(t[0]), 10**6) for t in p[3]]
    return min(rs) if rs else 10**6


def load_spec(path):
    s = importlib.util.spec_from_file_location("spec", path)
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m)
    global SPEC_IDS
    SPEC_IDS = getattr(m, "IDS", {}) or {}
    return m.P


def canvas_pdf(draw_fns, size):
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=size)
    for fn in draw_fns:
        fn(c); c.showPage()
    c.save(); buf.seek(0)
    return pikepdf.open(buf)


def separator(title, sub, n, lote="", falta=False):
    def draw(c):
        W, H = W_L, H_L
        c.setLineWidth(3); c.rect(5 * mm, 5 * mm, W - 10 * mm, H - 10 * mm)
        if falta:  # 2ª via: vai em cima das etiquetas cujo material não tem em estoque
            c.setFillGray(0); c.rect(5 * mm, H - 38 * mm, W - 10 * mm, 33 * mm, fill=1, stroke=0)
            c.setFillGray(1)
            c.setFont("Helvetica-Bold", 38); c.drawCentredString(W / 2, H - 24 * mm, "NÃO TEM")
            c.setFont("Helvetica-Bold", 12); c.drawCentredString(W / 2, H - 33 * mm, "aguardando material / estoque")
            c.setFillGray(0)
        lines = title.split("\n")
        y = H / 2 + 20 * len(lines)
        for ln in lines:
            s = 34
            while stringWidth(ln, "Helvetica-Bold", s) > W - 16 * mm: s -= 1
            c.setFont("Helvetica-Bold", s); c.drawCentredString(W / 2, y, ln); y -= s + 8
        c.setFont("Helvetica", 14); c.drawCentredString(W / 2, y - 10, sub)
        c.setFont("Helvetica-Bold", 20); c.drawCentredString(W / 2, y - 40, f"{n} etiqueta{'s' if n != 1 else ''}")
        if lote:
            for txt, fn, mx, yy in (("LOTE " + lote_bonito(lote), "Helvetica-Bold", 15, 24 * mm),
                                    ("arquivo LightBurn:", "Helvetica", 8, 18 * mm),
                                    ("lightburn_nomes_" + lote + ".csv", "Helvetica", 8, 14.5 * mm)):
                sz = mx
                while sz > 5 and stringWidth(txt, fn, sz) > W - 14 * mm: sz -= 0.5
                c.setFont(fn, sz); c.drawCentredString(W / 2, yy, txt)
    return draw


SPEC_IDS = {}
def numerar(pdf_path, n_sep_pages=None, kinds_peds=None):
    """Numera TODAS as etiquetas (menos capas) no canto superior direito: Nº sequencial / total (pedido Lucas 30/09)."""
    pdf = pikepdf.open(pdf_path, allow_overwriting_input=True)
    kinds, peds = [], []
    if kinds_peds:
        kinds, peds = [k for k, _ in kinds_peds], [p for _, p in kinds_peds]
    else:
      with pdfplumber.open(pdf_path) as pl:
        for pg in pl.pages:
            t = pg.extract_text() or ""
            kinds.append("sep" if ("aguardando material" in t or (re.search(r"\d+ etiquetas?\b", t) and "UPPUS" not in t and "Pedido" not in t)) else "lbl")
            up = (re.findall(r"UPPUS(\d+)", t) or [""])[0]
            sid = SPEC_IDS.get(up) or (re.findall(r"Pedido:?\s*(\d{6}[0-9A-Z]{8})\b", t) or [""])[0]
            peds.append(sid or up)  # ID do pedido da plataforma (ex.: 261001ED8X7PNA) - pedido do Lucas 30/09
    tot = kinds.count("lbl"); n = 0
    for pg, k, ped in zip(pdf.pages, kinds, peds):
        if k != "lbl": continue
        n += 1
        box = pg.get("/CropBox") or pg.mediabox
        x0, y0, x1, y1 = [float(v) for v in box]
        buf = io.BytesIO(); c = canvas.Canvas(buf, pagesize=(x1, y1))
        txt = f"{n}/{tot}" + (f"   {ped}" if ped else ""); fs = 10  # nº da etiqueta + nº do pedido (Lucas 30/09)
        w = stringWidth(txt, "Helvetica-Bold", fs) + 8
        c.setFillGray(0); c.rect(x1 - w - 2, y1 - fs - 8, w, fs + 6, fill=1, stroke=0)
        c.setFillGray(1); c.setFont("Helvetica-Bold", fs); c.drawRightString(x1 - 6, y1 - fs - 4, txt)
        c.save(); buf.seek(0)
        ov = pikepdf.open(buf)
        pg.add_overlay(ov.pages[0], pikepdf.Rectangle(0, 0, x1, y1))
    pdf.save(pdf_path)


def main(labels_pdf, spec_path, out_pdf, out_csv, lote=None, page_of=None, ids=None, prateleiras=None):
    global SPEC_IDS, PRAT_ORDEM, PRAT_NOME
    if ids is not None: SPEC_IDS = dict(ids)
    if prateleiras:
        PRAT_ORDEM = {sku_key(k): i for i, k in enumerate(prateleiras)}; PRAT_NOME = {sku_key(k): v for k, v in prateleiras.items()}
    P = load_spec(spec_path) if isinstance(spec_path, str) else list(spec_path)
    if lote is None:
        import os as _os
        lote = _os.path.splitext(_os.path.basename(out_csv))[0].replace("lightburn_nomes_", "")
    # página de cada pedido
    if page_of is None:
      page_of = {}
      with pdfplumber.open(labels_pdf) as pdf:
        for i, pg in enumerate(pdf.pages):
            for m in set(re.findall(r"UPPUS(\d+)", (pg.extract_text() or ""))):
                page_of.setdefault(m, []).append(i)
    faltando = [p[0] for p in P if p[0] not in page_of]
    extras = sorted(set(page_of) - {p[0] for p in P})

    def okey(p):
        it = sorted(p[3], key=lambda t: (t[0], t[1].lower()))
        # prateleira -> SKU -> COR -> tipo -> fonte -> pedido  (pedido do Lucas 30/09: separar por prateleira, SKU e cor)
        return (prat_rank(p), sku_key(it[0][0]), it[0][1].lower(), TIPO_RANK.get(p[2], 9), p[4] or "~", p[0])

    plan = []   # (kind, payload)
    nomes, seq, linha = [], 0, 0
    for canal, titulo in CANAIS:
        pers = [p for p in P if p[1] == canal and p[2] != "N" and p[0] in page_of]
        pers.sort(key=okey)
        sem = sorted([p for p in P if p[1] == canal and p[2] == "N" and p[0] in page_of], key=okey)
        if pers:
            plan.append(("sep", (titulo, "PERSONALIZADOS", len(pers))))
            for p in pers:
                ped, _, tipo, itens, fonte, obs = p
                row = {"CANAL": canal, "LOTE": lote, "PEDIDO": ped, "FONTE": fonte if tipo == "G" else "", "OBS": obs,
                       "SKU": " + ".join(sorted({t[0] for t in itens})), "PRAT": prat_de([t[0] for t in itens]),
                       "COR": " + ".join(sorted({t[1] for t in itens})) + (f" ({len(itens)} un)" if len(itens) > 1 else "")}
                if tipo == "G":
                    seq += 1; row["SEQ"] = str(seq); row["STATUS"] = "OK"
                    its = []
                    fl = [f.strip() for f in fonte.split("/")] if "/" in (fonte or "") else None
                    if fl and len(fl) == len(itens):   # uma fonte por nome, na ordem do spec
                        itens = [tuple(t[:3]) + (f,) for t, f in zip(itens, fl)]
                    for t in sorted(itens, key=lambda t: t[1].lower()):
                        nm = cap_nome(t[2], obs); linha += 1; nomes.append(nm)
                        its.append((str(linha), t[1], nm, t[3] if len(t) > 3 else fonte))
                    row["ITENS"] = its
                else:
                    row["SEQ"] = ""
                    row["ITENS"] = [("", t[1], cap_nome(t[2], obs) if (t[2] and tipo == "A") else None, (t[3] if len(t) > 3 else fonte) if tipo == "A" else "") for t in itens]
                    row["STATUS"] = {"C": "PENDENTE CHAT", "E": "ESPECIAL", "A": "ATENCAO"}[tipo]
                    if tipo == "C" and not obs: row["OBS"] = "sem nome no pedido nem no chat"
                plan.append(("land", (page_of[ped], row)))
        if sem:
            plan.append(("sep", (titulo + "\nSEM PERSONALIZAR", "enviar sem gravação", len(sem))))
            for p in sem:
                for pg_i in page_of[p[0]]:
                    plan.append(("port", pg_i))
    for ped in extras:  # páginas do PDF que não estavam no plano
        for pg_i in page_of[ped]:
            plan.append(("port", pg_i))

    src = pikepdf.open(labels_pdf)
    out = pikepdf.new()
    PW, PH = 150 * mm, 100 * mm
    STRIP = 95.0   # altura (pt) útil do topo das páginas de continuação (lista de itens)
    def geo(spg):
        box = spg.get("/CropBox") or spg.mediabox
        x0, y0 = float(box[0]), float(box[1]); return x0, y0, float(box[2]) - x0, float(box[3]) - y0
    # decide layout de cada etiqueta paisagem: escala e se as continuações cabem na mesma folha
    lays = []
    for kind, pay in plan:
        if kind != "land": continue
        pgs = pay[0]; x0, y0, bw, bh = geo(src.pages[pgs[0]])
        ext = pgs[1:]
        tot_h = bh + STRIP * len(ext)
        s = min((PH * W_L / H_L) / bw, PH / tot_h)
        fits = not ext or s >= 0.44
        if not fits: s = min((PH * W_L / H_L) / bw, PH / bh)
        lays.append((s, fits, bw))
    land_rows = [x[1][1] for x in plan if x[0] == "land"]
    ov = canvas_pdf([(lambda c, r=r, L=L: painel(c, L[2] * L[0] + 1 * mm, 0, PW - L[2] * L[0] - 1 * mm, PH, r))
                     for r, L in zip(land_rows, lays)], (PW, PH))
    seps = [x[1] for x in plan if x[0] == "sep"]
    sp = canvas_pdf([separator(*s, lote=lote, falta=f) for s in seps for f in (False, True)], (W_L, H_L))
    li = si = 0
    inv = {i: p for p, l in page_of.items() for i in l}
    KP = []
    def _pid(i):
        p = inv.get(i, ""); return SPEC_IDS.get(p, p)
    for kind, pay in plan:
        if kind == "sep":
            KP += [("sep", ""), ("sep", "")]
            out.pages.append(sp.pages[2 * si]); out.pages.append(sp.pages[2 * si + 1]); si += 1  # capa normal + capa "NÃO TEM"
        elif kind == "port":
            KP.append(("lbl", _pid(pay)))
            out.pages.append(src.pages[pay])
        else:
            pgs, row = pay
            KP.append(("lbl", _pid(pgs[0])))
            s, fits, _ = lays[li]
            spg = src.pages[pgs[0]]
            x0, y0, bw, bh = geo(spg)
            lw = bw * s
            ext = pgs[1:] if fits else []
            used_h = (bh + STRIP * len(ext)) * s
            top_off = (PH - used_h) / 2
            pg = out.add_blank_page(page_size=(PW, PH))
            res = pikepdf.Dictionary()
            cmds = []
            # etiqueta principal
            ty = PH - top_off - bh * s
            res["/L0"] = out.copy_foreign(spg.as_form_xobject())
            cmds.append(f"q 0 {ty:.2f} {lw:.2f} {bh*s:.2f} re W n q {s:.5f} 0 0 {s:.5f} {-x0*s:.3f} {ty - y0*s:.3f} cm /L0 Do Q Q")
            # continuações (só a faixa de cima) empilhadas embaixo
            cy = ty
            for k, e in enumerate(ext, 1):
                epg = src.pages[e]; ex0, ey0, ebw, ebh = geo(epg)
                cy -= STRIP * s
                res[f"/L{k}"] = out.copy_foreign(epg.as_form_xobject())
                # faixa de cima da página e: y de (ey0+ebh-STRIP) até (ey0+ebh)
                cmds.append(f"q 0 {cy:.2f} {lw:.2f} {STRIP*s:.2f} re W n q {s:.5f} 0 0 {s:.5f} {-ex0*s:.3f} {cy - (ey0+ebh-STRIP)*s:.3f} cm /L{k} Do Q Q")
                cmds.append(f"q 0.6 w 0 {cy+STRIP*s:.2f} m {lw:.2f} {cy+STRIP*s:.2f} l S Q")
            res["/Inf"] = out.copy_foreign(ov.pages[li].as_form_xobject()); li += 1
            cmds.append("q /Inf Do Q")
            pg.Resources = pikepdf.Dictionary(XObject=res)
            pg.Contents = out.make_stream(" ".join(cmds).encode())
            if not fits:
                for e in pgs[1:]:
                    KP.append(("lbl", _pid(e))); out.pages.append(src.pages[e])
    out.save(out_pdf)
    numerar(out_pdf, n_sep_pages=None, kinds_peds=KP)
    # 06/10/2026: todas as paginas do mesmo tamanho/orientacao (150x100 deitado) - a Zebra pulava as diferentes
    try:
        from normalizar import normalizar
        normalizar(out_pdf)
    except Exception as e:
        print("AVISO: nao normalizou paginas:", e)
    try:
        import pikepdf as _pk
        open(out_pdf + ".paginas", "w").write(str(len(_pk.open(out_pdf).pages)))
    except Exception:
        pass

    # LightBurn: 1ª linha em branco, depois "Nome," por linha (inclusive a última)
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        f.write("\r\n")
        for n in nomes:
            f.write(n.replace(",", " ") + ",\r\n")
    print(f"paginas={len(out.pages)} etiquetas_numeradas={seq} nomes={len(nomes)} faltando={faltando} extras={extras}")
    return {"paginas": len(out.pages), "numeradas": seq, "nomes": nomes, "faltando": faltando}


if __name__ == "__main__":
    main(*sys.argv[1:6])
