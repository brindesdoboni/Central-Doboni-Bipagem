"""Painel v2: número grande = linha(s) do LightBurn; cada nome com a cor e F<n>."""
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from etiquetas import fit, wrap_lines

FNUM = {"alice": 1, "glacial indifference": 2, "chewy": 3, "great vibes": 4, "gilker": 5,
        "waltograph": 6, "avengeance": 7, "julli": 8}

def fcode(f):
    if not f: return ""
    n = FNUM.get(f.strip().lower())
    return f"F{n}" if n else f

import re as _re


def lote_bonito(cod):
    """2026-09-29_11h_entrega_rapida -> 29/09 · 11h · ENTREGA RÁPIDA"""
    m = _re.match(r"(\d{4})-(\d{2})-(\d{2})_?(.*)", cod or "")
    if not m:
        return (cod or "").replace("_", " ").upper()
    partes = [f"{m.group(3)}/{m.group(2)}"]
    resto = m.group(4)
    h = _re.match(r"(\d{1,2}h\d{0,2})_?(.*)", resto)
    if h:
        partes.append(h.group(1)); resto = h.group(2)
    nomes = {"entrega_rapida": "ENTREGA RÁPIDA", "sabado": "SÁBADO", "tiktok": "TIKTOK", "shopee": "SHOPEE"}
    for k, v in nomes.items():
        resto = resto.replace(k, v)
    resto = resto.replace("_", " ").strip().upper()
    if resto:
        partes.append(resto)
    return " · ".join(partes)


def _fit_block(txts, font, width, height, smax, smin=5, gap=2):
    """Maior tamanho em que todas as linhas (com quebra) cabem em width x height."""
    s = smax
    while s > smin:
        L = [wrap_lines(t, font, s, width) for t in txts]
        tot = sum(len(l) * (s + gap) for l in L)
        if tot <= height and all(stringWidth(x, font, s) <= width for l in L for x in l):
            return s, L
        s -= 0.5
    return smin, [wrap_lines(t, font, smin, width) for t in txts]


def painel(c, x, y, w, h, r):
    pad = 3 * mm
    c.setLineWidth(0.8); c.roundRect(x + 1.5 * mm, y + 1.5 * mm, w - 3 * mm, h - 3 * mm, 2 * mm)
    cx, top, right = x + pad + 1 * mm, y + h - pad - 1 * mm, x + w - pad - 1 * mm
    iw = right - cx
    st = r.get("STATUS", ""); seq = r.get("SEQ") or ""
    itens = r.get("ITENS", [])
    qs = [int(q) for q, *_ in itens if q]
    # cabeçalho: linha(s) do LightBurn em destaque
    if qs:
        big = str(qs[0]) if len(qs) == 1 else f"{min(qs)}-{max(qs)}"
        c.setFont("Helvetica", 7); c.drawString(cx, top - 6, "LIGHTBURN LINHA" + ("S" if len(qs) > 1 else ""))
        bs = fit(c, big, "Helvetica-Bold", iw * 0.55, 34, 16)
        c.setFont("Helvetica-Bold", bs); c.drawString(cx, top - 6 - bs * 0.9, big)
    else:
        c.setFont("Helvetica-Bold", 12); c.drawString(cx, top - 18, "FORA DA SEQUÊNCIA")
    rw = iw * 0.42  # coluna da direita do cabeçalho
    c.setFont("Helvetica", 7)
    sku = r.get("SKU", "")
    ss = fit(c, sku, "Helvetica-Bold", rw, 9, 5)
    c.setFont("Helvetica-Bold", ss); c.drawRightString(right, top - 17, sku)
    prat = r.get("PRAT", "")
    if prat:
        t = "PRAT. " + prat
        ps = fit(c, t, "Helvetica", rw, 7.5, 4.5)
        c.setFont("Helvetica", ps); c.drawRightString(right, top - 25.5, t)
    n_un = len(itens)
    if n_un > 1:
        c.setFont("Helvetica", 7); c.drawRightString(right, top - 33, f"{n_un} peças")
    hy = top - 40
    c.setLineWidth(0.5); c.line(cx, hy, right, hy)
    hy = faixa_destaque(c, cx, hy, iw, r)

    nomes = [t for t in itens if t[2]]
    sem_nome = st == "PENDENTE CHAT" or not nomes
    # rodapé: faixa do lote + pedido + obs (obs sempre inteira, fonte diminui)
    obs = r.get("OBS", "")
    fy = y + pad + 1
    lote = r.get("LOTE", "")
    canal = r.get("CANAL", "")
    if canal in ("ER", "TT"):
        # 06/10/2026 (Lucas): Entrega Rapida e TikTok bem identificados na etiqueta
        bh = 30
        bx, by, bw = x + 1.5 * mm, y + 1.5 * mm, w - 3 * mm
        nome_c = "ENTREGA RÁPIDA" if canal == "ER" else "TIKTOK"
        if canal == "ER":   # faixa preta cheia, texto branco
            c.setFillGray(0); c.rect(bx, by, bw, bh, fill=1, stroke=0); c.setFillGray(1)
        else:               # TikTok: faixa branca com borda grossa e listras, texto preto
            c.setFillGray(1); c.rect(bx, by, bw, bh, fill=1, stroke=0)
            c.setFillGray(0); c.setLineWidth(3); c.rect(bx + 1.5, by + 1.5, bw - 3, bh - 3, fill=0, stroke=1)
        lt = lote_bonito(lote) if lote else ""
        c.setFont("Helvetica", 6.5); lw_ = c.stringWidth(lt, "Helvetica", 6.5)
        fs_ = fit(c, nome_c, "Helvetica-Bold", bw - lw_ - 16, 19, 9)
        c.setFont("Helvetica-Bold", fs_); c.drawString(cx, by + (bh - fs_ * 0.72) / 2, nome_c)
        c.setFont("Helvetica", 6.5); c.drawRightString(x + w - 1.5 * mm - 5, by + 5, lt)
        c.setFillGray(0)
        fy += bh
        if canal == "ER":   # moldura grossa em volta do painel
            c.setLineWidth(3); c.roundRect(x + 1.5 * mm, y + 1.5 * mm, w - 3 * mm, h - 3 * mm, 2 * mm); c.setLineWidth(0.8)
    elif lote:
        c.setFillGray(0); c.rect(x + 1.5 * mm, y + 1.5 * mm, w - 3 * mm, 15, fill=1, stroke=0)
        c.setFillGray(1)
        txt = "LOTE " + lote_bonito(lote)
        c.setFont("Helvetica-Bold", fit(c, txt, "Helvetica-Bold", w - 10 * mm, 10, 6))
        c.drawString(cx, y + 1.5 * mm + 4.5, txt)
        c.setFillGray(0)
        fy += 15
    c.setFont("Helvetica", 7); c.drawString(cx, fy, f"Pedido {r.get('PEDIDO','')}" + (f"   ·   etiqueta {seq}" if seq else ""))
    fy += 10
    corpo_min = 40 if not sem_nome else 30
    obs_txt = (("ATENÇÃO: " if st == "ATENCAO" else "") + obs).strip()
    fn = "Helvetica-Bold" if st in ("ATENCAO", "ESPECIAL") or sem_nome else "Helvetica"
    if obs_txt:
        if sem_nome and st != "PENDENTE CHAT":
            obs_h = max(20, (hy - 22) - fy)        # especial: o texto ocupa o corpo todo
        else:
            obs_h = max(18, min(62, (hy - fy) - corpo_min))
        os_, OL = _fit_block([obs_txt], fn, iw, obs_h, 15 if sem_nome else 7.5, 4.5, 1.5)
        lines = OL[0]
        c.setFont(fn, os_)
        yy = fy + 2 + (len(lines) - 1) * (os_ + 1.5)
        for ln in lines:
            yy_draw = yy
            c.drawString(cx, yy_draw, ln); yy -= os_ + 1.5
        fy += 4 + len(lines) * (os_ + 1.5)
    bottom = fy + 4

    if sem_nome:
        titulo = "BUSCAR NOME NO CHAT" if st == "PENDENTE CHAT" else ("ESPECIAL" if st == "ESPECIAL" else "")
        cores = ", ".join(sorted({t[1] for t in itens}))
        if titulo:
            c.setFont("Helvetica-Bold", fit(c, titulo, "Helvetica-Bold", iw, 14, 8)); c.drawString(cx, hy - 16, titulo)
        c.setFont("Helvetica", fit(c, cores, "Helvetica", iw, 9, 5)); c.drawString(cx, hy - 28, cores)
        return
    avail = hy - bottom
    if len(nomes) == 1:
        q, cor, nome, fnt = nomes[0]
        fc = fcode(fnt)
        cs = fit(c, cor.upper(), "Helvetica-Bold", iw, 11, 6)
        c.setFont("Helvetica-Bold", cs); c.drawString(cx, hy - 13, cor.upper())
        fonte_h = 22 if fc else 0
        s, L = _fit_block([nome], "Helvetica-Bold", iw, avail - 17 - fonte_h, 28, 6)
        yy = hy - 17
        for ln in L[0]:
            yy -= s + 2; c.setFont("Helvetica-Bold", s); c.drawString(cx, yy, ln)
        if fc:
            fsz = 18 if fc.startswith("F") else fit(c, fc, "Helvetica-Bold", iw, 14, 6)
            yy -= fsz + 4; c.setFont("Helvetica-Bold", fsz); c.drawString(cx, yy, fc)
            if fc.startswith("F"):
                off = stringWidth(fc, "Helvetica-Bold", fsz) + 5; c.setFont("Helvetica", fit(c, fnt, "Helvetica", iw - off, 10, 5)); c.drawString(cx + off, yy, fnt)
        return
    # vários nomes
    cores = {t[1].strip().lower() for t in nomes}
    mesma = len(cores) == 1
    if mesma:
        tt = ("TODAS " + nomes[0][1].upper())
        c.setFont("Helvetica-Bold", fit(c, tt, "Helvetica-Bold", iw, 11, 6)); c.drawString(cx, hy - 13, tt)
        hy -= 17
    avail = hy - bottom
    numw = 7 * mm
    tw = iw - numw
    def txt(t):
        fc = fcode(t[3])
        return ("" if mesma else t[1].upper() + ": ") + t[2] + (f" ({fc})" if fc else "")
    gap = 3
    sz = 24.0
    while True:
        L = [wrap_lines(txt(t), "Helvetica-Bold", sz, tw) for t in nomes]
        tot = sum(len(l) * (sz + 2) for l in L) + gap * (len(L) - 1) + 2
        if tot <= avail or sz <= 4:
            break
        sz -= 0.5
    yy = hy - 2
    for t, lines in zip(nomes, L):
        yy -= sz
        if t[0]:
            c.setFont("Helvetica", max(4, min(11, sz * 0.55))); c.drawString(cx, yy + 1, str(t[0]))
        for k, ln in enumerate(lines):
            if k: yy -= sz + 2
            c.setFont("Helvetica-Bold", sz); c.drawString(cx + numw, yy, ln)
        yy -= 2 + gap
        c.setLineWidth(0.2); c.line(cx, yy + gap / 2 + 1, right, yy + gap / 2 + 1)


def faixa_destaque(c, cx, hy, iw, r):
    """Faixa preta (Lucas 05/10): SKU, COR e QUANTIDADE em letra grande, branco em negrito. Devolve o novo topo do corpo."""
    itens = r.get("ITENS", [])
    sku = (r.get("SKU", "") or "").upper()
    cores = list(dict.fromkeys((t[1] or "").strip().upper() for t in itens if (t[1] or "").strip()))
    cor = " + ".join(cores)
    qtd = max(len(itens), 1)
    mk = _re.search(r"(\d+)\s*(UN|UND|UNIDADES)\b", cor)
    qtxt = f"QTD: {mk.group(1)} un" if (mk and qtd == 1) else f"QTD: {qtd}"
    destaque_q = qtd >= 2
    f = "Helvetica-Bold"
    pad = 3
    qs = 15
    qw = stringWidth(qtxt, f, qs) + 8
    l1 = sku + ("  ·  " + cor if cor else "")
    s1 = 15
    while s1 > 7 and stringWidth(l1, f, s1) > iw - qw - 10: s1 -= 0.5
    if s1 >= 11:                                  # tudo numa linha
        linhas = [(l1, s1)]
    else:                                         # SKU numa linha, cor na outra
        s_a = 15
        while s_a > 7 and stringWidth(sku, f, s_a) > iw - qw - 10: s_a -= 0.5
        s_b = 14
        while s_b > 6 and stringWidth(cor, f, s_b) > iw - 6: s_b -= 0.5
        linhas = [(sku, s_a), (cor, s_b)]
    hgt = sum(s for _, s in linhas) + 4 * (len(linhas) - 1) + 2 * pad + 2
    top = hy - 2
    c.setFillGray(0); c.rect(cx - 2, top - hgt, iw + 4, hgt, fill=1, stroke=0)
    c.setFillGray(1)
    yy = top - pad
    for i, (t, s) in enumerate(linhas):
        yy -= s * 0.82
        c.setFont(f, s); c.drawString(cx + 2, yy, t)
        yy -= 4 + s * 0.18
    # quantidade na direita da 1a linha; 2 ou mais = caixa invertida (branca com letra preta)
    s0 = linhas[0][1]
    by = top - pad - s0 * 0.82 - 3
    if destaque_q:
        c.setFillGray(1); c.rect(cx + iw - qw + 2, by - 1, qw, qs + 4, fill=1, stroke=0)
        c.setFillGray(0)
    c.setFont(f, qs); c.drawRightString(cx + iw - 2, by + 2, qtxt)
    c.setFillGray(0)
    return top - hgt - 2
