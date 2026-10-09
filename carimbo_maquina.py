# -*- coding: utf-8 -*-
"""Carimba "MAQUINA N" nas etiquetas COM PAINEL (as de gravacao) de um PDF de lote.
Lugar (pedido do Lucas 09/10): tarja preta no canto de cima, a esquerda, em cima da data/hora que o UpSeller
escreve depois do codigo UPPUS (o codigo UPPUS e o "n/total" continuam aparecendo). A tarja desce so ate onde
o espaco esta vazio (mede pagina por pagina): nunca cobre destinatario, logo, QR ou codigo de barras.
Paginas sem painel (capa, separadoras, etiqueta original sem gravacao) nao sao tocadas.
Uso: python carimbo_maquina.py entrada.pdf saida.pdf 1      (1, 2 ou 3)"""
import io, re, sys
import pdfplumber
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.lib.colors import black, white

TOPO, LINHA, FUNDO_MAX, LARG_MIN = 0.6, 9.8, 18.5, 62   # pontos medidos de cima
FONTE = "Helvetica-Bold"
RESERVA = ((318, 252, 318.1, 252.1), (318, 252, 420, 261.5))   # painel, linha de baixo (x0, top0, x1, top1)


def achar_lugar(pg):
    """Devolve (faixa_data, tarja) em coordenadas (x0, top0, x1, top1) ou None se nao achar a linha do UpSeller."""
    ws = pg.extract_words()
    lin = sorted([w for w in ws if w["top"] < LINHA - 3 and w["x1"] < pg.width * 0.45], key=lambda w: w["x0"])
    if not lin or not lin[0]["text"].upper().startswith("UPPUS"):
        return None
    meio = [w for w in lin[1:] if not re.fullmatch(r"\d+/\d+", w["text"]) or "/20" in w["text"] or len(w["text"]) > 7]
    fim = [w for w in lin[1:] if re.fullmatch(r"\d{1,4}/\d{1,4}", w["text"])]
    if not meio:
        return None
    d0 = lin[0]["x1"] + 1.2
    d1 = max(w["x1"] for w in meio) + 1.0
    borda = [r for r in pg.rects if r["width"] > 120 and r["top"] < 14 and r["x0"] < 40]
    teto = (min(r["x1"] for r in borda) - 2) if borda else pg.width * 0.41   # nunca passa da etiqueta de envio
    lim = min((fim[-1]["x0"] - 3) if fim else teto, teto)
    obst = [w for w in ws if w["top"] >= LINHA - 3 and w["top"] < 40 and w["x0"] < pg.width * 0.45]
    obst += [i for i in pg.images if i["top"] < 40]
    obst += [r for r in pg.rects if r["top"] < 40 and r["height"] > 3 and r["width"] < 120]
    xs0 = sorted({d0} | {o["x1"] + 1.5 for o in obst if d0 < o["x1"] + 1.5 < lim})
    xs1 = sorted({lim} | {o["x0"] - 1.5 for o in obst if d0 < o["x0"] - 1.5 < lim})
    melhor = None
    for a in xs0:
        for b in xs1:
            if b - a < LARG_MIN:
                continue
            fundo = min([FUNDO_MAX] + [o["top"] - 0.8 for o in obst if o["x0"] < b and o["x1"] > a and o["top"] > LINHA - 3])
            nota = (round(fundo, 1), b - a)
            if melhor is None or nota > melhor[0]:
                melhor = (nota, (a, TOPO, b, fundo))
    baixo_faixa = min([LINHA] + [o["top"] - 0.6 for o in obst if o["x0"] < d1 and o["x1"] > d0])
    cobre = max(w["bottom"] for w in meio) + 0.4      # a data/hora sempre fica 100% coberta
    faixa = (d0, TOPO, max(d1, d0 + 1), max(baixo_faixa, cobre))
    if melhor is None:
        return faixa, (d0, TOPO, max(d1, lim), faixa[3])
    return faixa, melhor[1]


def selo(w, h, texto, faixa, tarja):
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(w, h))
    c.setFillColor(black)
    for x0, t0, x1, t1 in (faixa, tarja):
        c.rect(x0, h - t1, x1 - x0, t1 - t0, stroke=0, fill=1)
    x0, t0, x1, t1 = tarja
    alt = t1 - t0
    tam = min(12.5, alt * 0.72)
    while tam > 6 and stringWidth(texto, FONTE, tam) > (x1 - x0) - 4:
        tam -= 0.5
    c.setFillColor(white)
    c.setFont(FONTE, tam)
    c.drawCentredString((x0 + x1) / 2, h - t1 + (alt - tam * 0.72) / 2, texto)
    c.save()
    buf.seek(0)
    return PdfReader(buf).pages[0]


def main(ent, sai, n):
    texto = f"MÁQUINA {int(n)}"
    r, w = PdfReader(ent), PdfWriter()
    k = sem_lugar = 0
    with pdfplumber.open(ent) as pl:
        for pg, pp in zip(r.pages, pl.pages):
            t = pp.extract_text() or ""
            if re.search(r"Pedido\s+\d{5,7}\b", t) and abs(float(pp.width) - 425.2) < 3:
                lugar = achar_lugar(pp)
                if lugar:
                    pg.merge_page(selo(float(pp.width), float(pp.height), texto, *lugar))
                    k += 1
                else:   # etiqueta sem a linha do UpSeller em cima: usa o lugar reserva (linha "Pedido ... etiqueta" do painel)
                    res = RESERVA
                    for wd in pp.extract_words():
                        if wd["text"] == "Pedido" and wd["x0"] > pp.width * 0.45:
                            t0, t1 = wd["top"] - 2.2, wd["bottom"] + 2.2
                            res = ((318, t0, 318.1, t0 + 0.1), (318, t0, 420, t1))
                    pg.merge_page(selo(float(pp.width), float(pp.height), texto, *res))
                    sem_lugar += 1
            w.add_page(pg)
    with open(sai, "wb") as f:
        w.write(f)
    print(f"{k} de {len(r.pages)} paginas carimbadas com {texto} -> {sai}" + (f" | {sem_lugar} sem a linha UPPUS em cima: carimbo no painel, ao lado de Pedido" if sem_lugar else ""))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
