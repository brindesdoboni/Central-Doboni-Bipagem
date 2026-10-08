"""Deixa TODAS as paginas do PDF do mesmo tamanho e na mesma posicao: 150x100 mm deitado,
origem em (0,0). Paginas em pe (capas e etiquetas originais sem personalizar) sao giradas
90 graus e encaixadas. Motivo (06/10/2026): a Zebra pulava as paginas de tamanho/orientacao
diferente dentro do mesmo PDF."""
import sys, pikepdf
W, H = 150 / 25.4 * 72, 100 / 25.4 * 72   # 425.2 x 283.5 pt

def normalizar(src, dst=None):
    pdf = pikepdf.open(src, allow_overwriting_input=True)
    novas = []
    for pg in pdf.pages:
        x0, y0, x1, y1 = [float(v) for v in (pg.get('/CropBox') or pg.mediabox)]
        w, h = x1 - x0, y1 - y0
        if abs(w - W) < 2 and abs(h - H) < 2 and abs(x0) < .5 and abs(y0) < .5 and not int(pg.get('/Rotate', 0)):
            novas.append(None); continue
        rot = int(pg.get('/Rotate', 0)) % 360
        fx = pg.as_form_xobject()
        retrato = (h > w) ^ (rot in (90, 270))
        if retrato:   # gira 90 graus
            s = min(W / h, H / w); ox, oy = (W - s * h) / 2, (H - s * w) / 2
            tx, ty = ox + s * h, oy
            cm = [0, s, -s, 0, s * y0 + tx, -s * x0 + ty]
        else:
            s = min(W / w, H / h); ox, oy = (W - s * w) / 2, (H - s * h) / 2
            cm = [s, 0, 0, s, ox - s * x0, oy - s * y0]
        cont = ('q %s cm /Fx0 Do Q' % ' '.join('%.4f' % v for v in cm)).encode()
        novo = pikepdf.Dictionary(Type=pikepdf.Name.Page, MediaBox=[0, 0, W, H],
                                  Resources=pikepdf.Dictionary(XObject=pikepdf.Dictionary(Fx0=fx)),
                                  Contents=pdf.make_stream(cont))
        novas.append(novo)
    for i, nv in enumerate(novas):
        if nv is not None:
            pdf.pages[i] = pikepdf.Page(nv)
    pdf.save(dst or src)
    return len(pdf.pages), sum(1 for n in novas if n is not None)

if __name__ == '__main__':
    for f in sys.argv[1:]:
        print(f, normalizar(f))
