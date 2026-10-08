"""Base comum das etiquetas Brindes do Boni (recriado 29/09/2026).
W_L, H_L = etiqueta térmica retrato 100x150 mm (Zebra ZD220)."""
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth

W_L, H_L = 100 * mm, 150 * mm


def fit(c, text, font, maxw, maxs, mins):
    """Maior tamanho de fonte (entre mins e maxs) em que o texto cabe em maxw."""
    s = maxs
    while s > mins and stringWidth(text or "", font, s) > maxw:
        s -= 0.5
    return s


def wrap_lines(text, font, size, width):
    """Quebra o texto em linhas que caibam em width (quebra palavra longa se precisar)."""
    out, cur = [], ""
    for w in (text or "").split():
        t = (cur + " " + w).strip()
        if stringWidth(t, font, size) <= width:
            cur = t; continue
        if cur: out.append(cur)
        while stringWidth(w, font, size) > width and len(w) > 1:
            k = len(w)
            while k > 1 and stringWidth(w[:k], font, size) > width: k -= 1
            out.append(w[:k]); w = w[k:]
        cur = w
    if cur: out.append(cur)
    return out or [""]
