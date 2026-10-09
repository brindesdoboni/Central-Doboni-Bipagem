# -*- coding: utf-8 -*-
"""Carimba "MAQUINA N" nas etiquetas COM PAINEL (as de gravacao) de um PDF de lote.
Lugar: linha de baixo do painel da direita, ao lado de "Pedido ... etiqueta N" (espaco que fica sempre em branco).
Paginas sem painel (capa, separadoras, etiqueta original sem gravacao) nao sao tocadas.
Uso: python carimbo_maquina.py entrada.pdf saida.pdf 1      (1, 2 ou 3)"""
import io, re, sys
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.colors import black, white

X0, X1, Y0, Y1 = 318, 420, 22, 31.5   # pontos, origem embaixo/esquerda (etiqueta 150x100 = 425x283 pt) - mesma altura da linha "Pedido ..."


def selo(w, h, texto):
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(w, h))
    c.setFillColor(black)
    c.roundRect(X0, Y0, X1 - X0, Y1 - Y0, 2.5, stroke=0, fill=1)
    c.setFillColor(white)
    c.setFont("Helvetica-Bold", 9)
    c.drawCentredString((X0 + X1) / 2, Y0 + 2.6, texto)
    c.save()
    buf.seek(0)
    return PdfReader(buf).pages[0]


def tem_painel(pg):
    t = pg.extract_text() or ""
    return bool(re.search(r"Pedido\s+\d{5,7}\b", t)) and abs(float(pg.mediabox.width) - 425.2) < 3


def main(ent, sai, n):
    texto = f"MÁQUINA {int(n)}"
    r, w = PdfReader(ent), PdfWriter()
    k = 0
    for pg in r.pages:
        if tem_painel(pg):
            pg.merge_page(selo(float(pg.mediabox.width), float(pg.mediabox.height), texto))
            k += 1
        w.add_page(pg)
    with open(sai, "wb") as f:
        w.write(f)
    print(f"{k} de {len(r.pages)} paginas carimbadas com {texto} -> {sai}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
