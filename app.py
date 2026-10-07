# -*- coding: utf-8 -*-
"""Central Boni - Bipagem da producao. Python puro (sem dependencias). SQLite em volume."""
import csv, hashlib, hmac, io, json, os, re, secrets, sqlite3, threading, time
from collections import deque
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

DB = os.environ.get("DB_PATH", "/data/central.db" if os.path.isdir("/data") else "central.db")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "trocar")
API_TOKEN = os.environ.get("API_TOKEN", "token-local")
STATION_KEY = os.environ.get("STATION_KEY", "posto-local")
SECRET = os.environ.get("SECRET_KEY", "segredo-local").encode()
BR = timezone(timedelta(hours=-3))
ALERTA_GRAVACAO_MIN = int(os.environ.get("ALERTA_GRAVACAO_MIN", "30"))
AQUI = os.path.dirname(os.path.abspath(__file__))
_lock = threading.Lock()

ETAPAS = ["AGUARDANDO", "SEPARADO", "EM_GRAVACAO", "GRAVADO", "EXPEDIDO", "DEVOLVIDO"]
GRAV_MIN_SEG = int(os.environ.get("GRAV_MIN_SEG", "10"))   # 2o bipe antes disso = engano (nao conta como terminado)
ORDEM = {e: i for i, e in enumerate(ETAPAS)}


def agora():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def conn():
    c = sqlite3.connect(DB, timeout=15)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON")
    return c


def iniciar_db():
    try:
        _iniciar_db()
    finally:
        _carregar_cor_alias()


def _iniciar_db():
    with conn() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript("""
        CREATE TABLE IF NOT EXISTS colaboradores(id INTEGER PRIMARY KEY, nome TEXT NOT NULL,
            funcao TEXT, codigo TEXT UNIQUE NOT NULL, ativo INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS itens(id INTEGER PRIMARY KEY, chave TEXT UNIQUE, lote TEXT,
            etiqueta INTEGER, pedido TEXT, rastreio TEXT, canal TEXT, loja TEXT, sku TEXT, cor TEXT,
            nomes TEXT, fonte TEXT, tipo TEXT, obs TEXT, personalizado INTEGER DEFAULT 1,
            status TEXT DEFAULT 'AGUARDANDO', falta_material INTEGER DEFAULT 0,
            criado_em TEXT, atualizado_em TEXT);
        CREATE TABLE IF NOT EXISTS codigos(codigo TEXT, item_id INTEGER REFERENCES itens(id) ON DELETE CASCADE,
            PRIMARY KEY(codigo, item_id));
        CREATE TABLE IF NOT EXISTS eventos(id INTEGER PRIMARY KEY, item_id INTEGER REFERENCES itens(id) ON DELETE CASCADE,
            etapa TEXT, colaborador_id INTEGER, posto TEXT, em TEXT, alerta TEXT, desfeito INTEGER DEFAULT 0);
        CREATE INDEX IF NOT EXISTS ix_ev_em ON eventos(em);
        CREATE INDEX IF NOT EXISTS ix_it_criado ON itens(criado_em);
        CREATE TABLE IF NOT EXISTS pausas(id INTEGER PRIMARY KEY, nome TEXT, hora TEXT, pessoas TEXT);
        CREATE TABLE IF NOT EXISTS custos(sku TEXT PRIMARY KEY, descricao TEXT, custo REAL, atualizado_em TEXT);
        CREATE TABLE IF NOT EXISTS emails(id INTEGER PRIMARY KEY, em TEXT, remetente TEXT, assunto TEXT,
            arquivos TEXT, etiquetas INTEGER, novos INTEGER, atualizados INTEGER, erro TEXT, uid TEXT);
        """)
        c.execute("CREATE TABLE IF NOT EXISTS meta(chave TEXT PRIMARY KEY, valor TEXT)")
        if "excluido" not in [r[1] for r in c.execute("PRAGMA table_info(colaboradores)")]:
            c.execute("ALTER TABLE colaboradores ADD COLUMN excluido INTEGER DEFAULT 0")
        if "voz" not in [r[1] for r in c.execute("PRAGMA table_info(colaboradores)")]:
            c.execute("ALTER TABLE colaboradores ADD COLUMN voz INTEGER")   # o "som" de cada um (0 a 15)
        if "envio" not in [r[1] for r in c.execute("PRAGMA table_info(itens)")]:
            c.execute("ALTER TABLE itens ADD COLUMN envio TEXT DEFAULT ''")
        cols_it = [r[1] for r in c.execute("PRAGMA table_info(itens)")]
        if "qtd" not in cols_it:
            c.execute("ALTER TABLE itens ADD COLUMN qtd INTEGER DEFAULT 1")
        if "impresso" not in cols_it:
            c.execute("ALTER TABLE itens ADD COLUMN impresso TEXT DEFAULT ''")
        if "pecas" not in cols_it:
            c.execute("ALTER TABLE itens ADD COLUMN pecas TEXT DEFAULT ''")
        if "oculto" not in cols_it:   # 1 = duplicada (juntada em outra), 2 = encerrada (saiu/cancelada/antiga sem movimento)
            c.execute("ALTER TABLE itens ADD COLUMN oculto INTEGER DEFAULT 0")
            c.execute("ALTER TABLE itens ADD COLUMN oculto_motivo TEXT DEFAULT ''")
        if "despachado_em" not in cols_it:   # saiu daqui com a transportadora (bipe no DESPACHO ou Shopee "enviado")
            c.execute("ALTER TABLE itens ADD COLUMN despachado_em TEXT")
            c.execute("ALTER TABLE itens ADD COLUMN despachado_por TEXT DEFAULT ''")
        c.execute("CREATE INDEX IF NOT EXISTS ix_itens_desp ON itens(despachado_em)")
        # velocidade do bipe e da TV (sem isso cada consulta varre a tabela de eventos inteira)
        c.execute("CREATE INDEX IF NOT EXISTS ix_ev_item ON eventos(item_id, etapa)")
        c.execute("CREATE INDEX IF NOT EXISTS ix_ev_etapa_em ON eventos(etapa, em)")
        c.execute("CREATE INDEX IF NOT EXISTS ix_it_status ON itens(status)")
        c.execute("CREATE INDEX IF NOT EXISTS ix_cod_item ON codigos(item_id)")
        c.execute("DELETE FROM custos WHERE length(COALESCE(atualizado_em,''))=10")  # custos vindos de nota (valor nao real)
        c.execute("""CREATE TABLE IF NOT EXISTS estoque_mov(id INTEGER PRIMARY KEY, em TEXT, sku TEXT, cor TEXT,
            qtd REAL, tipo TEXT, ref TEXT UNIQUE, obs TEXT)""")
        c.execute("CREATE INDEX IF NOT EXISTS ix_mov_sku ON estoque_mov(sku, cor)")
        cols_mov = {r[1] for r in c.execute("PRAGMA table_info(estoque_mov)")}
        if "xbz_pedido" not in cols_mov:
            c.execute("ALTER TABLE estoque_mov ADD COLUMN xbz_pedido TEXT")
        if "importado_em" not in cols_mov:
            c.execute("ALTER TABLE estoque_mov ADD COLUMN importado_em TEXT")
        # compras XBZ recebidas (API PedidosListar): 1 linha por CNPJ + pedido + SKU XBZ + codigo composto = nunca entra 2 vezes
        c.execute("""CREATE TABLE IF NOT EXISTS xbz_pedidos_importados(id INTEGER PRIMARY KEY, cnpj TEXT NOT NULL,
            pedido_numero TEXT NOT NULL, produto_codigo_xbz TEXT NOT NULL, produto_codigo_composto TEXT NOT NULL DEFAULT '',
            quantidade INTEGER NOT NULL, status_logistico TEXT, sku TEXT, cor TEXT, resultado TEXT, mov_id INTEGER,
            importado_em TEXT, UNIQUE(cnpj, pedido_numero, produto_codigo_xbz, produto_codigo_composto))""")
        c.execute("""CREATE TABLE IF NOT EXISTS xbz_pendencias(id INTEGER PRIMARY KEY, cnpj TEXT NOT NULL, pedido_numero TEXT NOT NULL,
            produto_codigo_xbz TEXT NOT NULL, produto_codigo_composto TEXT NOT NULL DEFAULT '', codigo_amigavel TEXT, produto_nome TEXT,
            quantidade INTEGER, status_logistico TEXT, data_ref TEXT, tipo TEXT, motivo TEXT, dados TEXT, criado_em TEXT,
            atualizado_em TEXT, resolvido_em TEXT, resolucao TEXT,
            UNIQUE(cnpj, pedido_numero, produto_codigo_xbz, produto_codigo_composto))""")
        c.execute("CREATE TABLE IF NOT EXISTS xbz_mapa(chave TEXT PRIMARY KEY, sku TEXT NOT NULL, cor TEXT, em TEXT)")
        c.execute("""CREATE TABLE IF NOT EXISTS compra_aprendizado(sku TEXT PRIMARY KEY, fator REAL, pedidos INTEGER,
            atualizado TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS pedidos_xbz(id INTEGER PRIMARY KEY, em TEXT, itens TEXT, total REAL)""")
        c.execute("CREATE TABLE IF NOT EXISTS prateleiras(sku TEXT PRIMARY KEY, prateleira TEXT)")
        c.execute("""CREATE TABLE IF NOT EXISTS shopee_lojas(shop_id INTEGER PRIMARY KEY, nome TEXT, access_token TEXT,
                     refresh_token TEXT, expira INTEGER, autorizada_em TEXT, atualizado_em TEXT, status_loja TEXT DEFAULT '',
                     expira_autorizacao INTEGER DEFAULT 0, erro TEXT DEFAULT '')""")
        c.execute("""CREATE TABLE IF NOT EXISTS vendas_hist(order_sn TEXT PRIMARY KEY, shop_id INTEGER, loja TEXT,
            criado INTEGER, status TEXT, itens TEXT)""")
        c.execute("CREATE INDEX IF NOT EXISTS ix_vh_criado ON vendas_hist(criado)")
        c.execute("""CREATE TABLE IF NOT EXISTS bipes_log(id INTEGER PRIMARY KEY AUTOINCREMENT, em TEXT, op TEXT, dados TEXT)""")
        c.execute("CREATE INDEX IF NOT EXISTS ix_bl_op ON bipes_log(op, id)")
        c.execute("CREATE TABLE IF NOT EXISTS upseller_pedidos(codigo TEXT PRIMARY KEY, pedido TEXT, estado TEXT, loja TEXT, em TEXT)")
        if "itens" not in {r[1] for r in c.execute("PRAGMA table_info(upseller_pedidos)")}:
            c.execute("ALTER TABLE upseller_pedidos ADD COLUMN itens TEXT")
        c.execute("CREATE TABLE IF NOT EXISTS sku_status(sku TEXT, cor TEXT, status TEXT, em TEXT, PRIMARY KEY(sku, cor))")
        # cores: "e a mesma que" (vira uma so) e "sao cores diferentes" (nao pergunta de novo)
        c.execute("CREATE TABLE IF NOT EXISTS cor_alias(sku TEXT, de TEXT, para TEXT, em TEXT, PRIMARY KEY(sku, de))")
        c.execute("CREATE TABLE IF NOT EXISTS cor_ok(sku TEXT, cor TEXT, outra TEXT, em TEXT, PRIMARY KEY(sku, cor, outra))")
        c.execute("""CREATE TABLE IF NOT EXISTS devolucoes(id INTEGER PRIMARY KEY, item_id INTEGER UNIQUE, pedido TEXT, canal TEXT,
            loja TEXT, sku TEXT, cor TEXT, personalizado INTEGER, gravado INTEGER, sugestao TEXT, situacao TEXT, motivo TEXT,
            obs TEXT, custo REAL, colaborador_id INTEGER, em TEXT, decidido_em TEXT, return_sn TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS shopee_devolucoes(return_sn TEXT PRIMARY KEY, shop_id INTEGER, loja TEXT,
            order_sn TEXT, status TEXT, motivo TEXT, texto TEXT, prazo INTEGER, rastreio TEXT, valor REAL, itens TEXT,
            criado INTEGER, atualizado INTEGER)""")
        cols_dv = [r[1] for r in c.execute("PRAGMA table_info(devolucoes)")]
        for col in ("midias", "enviado_em", "envio_resp"):
            if col not in cols_dv:
                c.execute(f"ALTER TABLE devolucoes ADD COLUMN {col} TEXT")
        cols_sd = [r[1] for r in c.execute("PRAGMA table_info(shopee_devolucoes)")]
        for col, tipo in (("detalhe", "TEXT"), ("resultado", "TEXT"), ("contestou", "INTEGER DEFAULT 0")):
            if col not in cols_sd:
                c.execute(f"ALTER TABLE shopee_devolucoes ADD COLUMN {col} {tipo}")
        c.execute("""CREATE TABLE IF NOT EXISTS shopee_pedidos(order_sn TEXT PRIMARY KEY, shop_id INTEGER, loja TEXT,
            status TEXT, criado INTEGER, atualizado INTEGER, prazo INTEGER, envio TEXT, msg TEXT, itens TEXT,
            motivo TEXT, visto_em TEXT)""")
        c.execute("CREATE INDEX IF NOT EXISTS ix_sp_status ON shopee_pedidos(status)")
        if "despachado" not in [r[1] for r in c.execute("PRAGMA table_info(shopee_pedidos)")]:
            c.execute("ALTER TABLE shopee_pedidos ADD COLUMN despachado INTEGER")  # quando a transportadora levou
        c.execute("""CREATE TABLE IF NOT EXISTS xbz_alertas(id INTEGER PRIMARY KEY, em TEXT, sku TEXT, cor TEXT,
            tipo TEXT, detalhe TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS xbz(codigo_xbz TEXT PRIMARY KEY, codigo TEXT, composto TEXT, nome TEXT,
            cor TEXT, preco REAL, estoque INTEGER, status TEXT, reposicao TEXT, atualizado TEXT)""")
        c.execute("CREATE INDEX IF NOT EXISTS ix_xbz_cod ON xbz(codigo)")
        c.execute("""CREATE TABLE IF NOT EXISTS compras(id INTEGER PRIMARY KEY, chave TEXT UNIQUE, nf TEXT, data TEXT,
            conta TEXT, cprod TEXT, descricao TEXT, qtd REAL, unit REAL, total REAL, importado_em TEXT)""")
        if "uid" not in [r[1] for r in c.execute("PRAGMA table_info(emails)")]:
            c.execute("ALTER TABLE emails ADD COLUMN uid TEXT")
        # e-mails recusados antes podem ser lidos de novo se o remetente for liberado
        c.execute("""UPDATE emails SET uid='neg:'||uid WHERE erro LIKE 'remetente nao autorizado%'
                     AND uid IS NOT NULL AND uid NOT LIKE 'neg:%' AND uid NOT LIKE 'xbz:%'""")
        if "duracao" not in [r[1] for r in c.execute("PRAGMA table_info(pausas)")]:
            c.execute("ALTER TABLE pausas ADD COLUMN duracao INTEGER DEFAULT 15")
        c.execute("UPDATE colaboradores SET funcao='Devolução' WHERE funcao='Etiquetas'")
        # so 3 modos de envio: etiqueta manual com "Pedido 9998..." e TikTok; "OUTROS" vira o canal pelo numero
        c.execute("""UPDATE itens SET canal='TIKTOK', envio='' WHERE lote='MANUAL' AND canal<>'TIKTOK'
                     AND length(pedido)=15 AND pedido GLOB '9998[0-9]*'""")
        for r in c.execute("SELECT id, pedido, rastreio FROM itens WHERE canal='OUTROS' OR canal=''").fetchall():
            cn, en = canal_por_codigo(r[1], r[2])
            if cn:
                c.execute("UPDATE itens SET canal=?, envio=? WHERE id=?", (cn, en, r[0]))
        padrao = [("Cafe da manha", "09:15", "Rafael, Guilherme"), ("Cafe da manha", "09:30", "Yuri, Juninho"),
                  ("Cafe da tarde", "15:30", "Yuri, Juninho"), ("Cafe da tarde", "15:45", "Rafael, Guilherme")]
        if not c.execute("SELECT 1 FROM meta WHERE chave='pausas_v2'").fetchone():
            for n, h, pes in padrao:  # so adiciona o que falta; depois disso quem manda e o painel
                if not c.execute("SELECT 1 FROM pausas WHERE hora=? AND pessoas=?", (h, pes)).fetchone():
                    c.execute("INSERT INTO pausas(nome,hora,pessoas) VALUES(?,?,?)", (n, h, pes))
            c.execute("INSERT INTO meta VALUES('pausas_v2','1')")


def norm(cod):
    return re.sub(r"[^A-Z0-9]", "", (cod or "").upper())


# ------------------------------------------------------------------ lotes (API)
def _envio_obs(obs):
    """A ferramenta da Zebra manda a forma de envio no fim da obs: '... [SHOPEE XPRESS]'."""
    m = re.findall(r"\[([^\]]+)\]", obs or "")
    return m[-1].strip() if m else ""


GRUPOS = ["SHOPEE ENTREGA RÁPIDA", "TIKTOK", "SHOPEE EXPRESS"]  # so existem estes 3 modos de envio


def canal_por_codigo(*cods):
    """Descobre o canal pelo numero: TikTok = 18 digitos comecando com 5 ou 'Pedido: 9998...' (15 digitos);
    Shopee = pedido 26xxxx + letras ou rastreio BR... (Express, a menos que a etiqueta diga Entrega Rapida)."""
    for cod in cods:
        c = norm(cod or "")
        if re.fullmatch(r"5\d{17}", c) or re.fullmatch(r"9998\d{11}", c):
            return "TIKTOK", ""
    for cod in cods:
        c = norm(cod or "")
        if re.fullmatch(r"2\d{5}[0-9A-Z]{8}", c) or c.startswith("BR"):
            return "SHOPEE", "SHOPEE XPRESS"
    return "", ""


def grupo_envio(i):
    """Plataforma/forma de envio para a contagem do dia (lojas juntas). Nunca 'OUTROS'."""
    t = " ".join([i.get("canal") or "", i.get("envio") or "", _envio_obs(i.get("obs"))]).upper()
    if "DIRETA" in t or "RAPIDA" in t or "RÁPIDA" in t:
        return "SHOPEE ENTREGA RÁPIDA"
    if "TIKTOK" in t or "TIK TOK" in t:
        return "TIKTOK"
    if "SHOPEE" in t or "SPX" in t or "XPRESS" in t:
        return "SHOPEE EXPRESS"  # Shopee que nao e entrega direta = Express
    c, _ = canal_por_codigo(i.get("pedido"), i.get("rastreio"))
    return "TIKTOK" if c == "TIKTOK" else "SHOPEE EXPRESS"


def por_grupo_status(itens, etapas):
    """Tabela 'Canal' agrupada como os quadros + coluna FALTA (bipado em 'nao tem' e ainda nao saiu)."""
    res = {}
    for i in itens:
        if i["status"] not in etapas:
            continue
        d = res.setdefault(grupo_envio(i), {**{e: 0 for e in etapas}, "FALTA": 0})
        d[i["status"]] += 1
        if i["falta_material"] and i["status"] not in ("EXPEDIDO", "DEVOLVIDO"):
            d["FALTA"] += 1
    return {g: res[g] for g in GRUPOS if g in res}


def por_plataforma(itens):
    """Pedidos por plataforma: quantos subiram, quantos ja sairam (expedidos) e quantos faltam."""
    pedidos = {}
    for i in itens:
        if i["status"] == "DEVOLVIDO" or i.get("lote") == "DEVOLUCAO":
            continue
        k = norm(i["pedido"]) or f"id{i['id']}"
        g = grupo_envio(i)
        p = pedidos.setdefault(k, {"grupo": g, "pendente": False, "pers": False})
        p["grupo"] = g
        if i["status"] != "EXPEDIDO":
            p["pendente"] = True
        if i.get("personalizado"):
            p["pers"] = True
    res = {g: {"total": 0, "enviados": 0, "faltam": 0, "personalizados": 0, "sem_personalizar": 0} for g in GRUPOS}
    for p in pedidos.values():
        r = res[p["grupo"]]
        r["total"] += 1
        r["faltam" if p["pendente"] else "enviados"] += 1
        if p["pendente"]:
            r["personalizados" if p["pers"] else "sem_personalizar"] += 1
    return [dict(grupo=g, **res[g]) for g in GRUPOS if True]


def _qtd_pecas(it):
    """Quantidade de unidades e lista de produtos [{sku,cor,qtd}] da etiqueta (para o controle de materiais)."""
    pec = it.get("pecas") or []
    if isinstance(pec, str):
        try:
            pec = json.loads(pec)
        except Exception:
            pec = []
    pec = [p for p in pec if isinstance(p, dict) and p.get("sku")]
    if not pec:
        return {}
    return {"pecas": json.dumps(pec, ensure_ascii=False), "qtd": sum(int(p.get("qtd") or 1) for p in pec)}


def importar_lote(dados):
    lote = dados.get("lote") or datetime.now(BR).strftime("%Y%m%d-%H%M")
    n_novo = n_atual = 0
    ocorr = {}
    tocados = []
    with _lock, conn() as c:
        for i, it in enumerate(dados.get("itens", [])):
            pedido = str(it.get("pedido") or "").strip()
            if not pedido:
                continue
            nomes = it.get("nomes") or ""
            if isinstance(nomes, list):
                nomes = " | ".join(nomes)
            tipo = (it.get("tipo") or "").upper()
            pers = 0 if tipo == "N" or it.get("personalizado") is False else 1
            chave = "|".join([pedido, str(it.get("sku") or ""), nomes, str(it.get("seq") or "")])
            campos = dict(lote=lote, etiqueta=it.get("etiqueta"), pedido=pedido,
                          rastreio=it.get("rastreio") or "", canal=it.get("canal") or "",
                          envio=(it.get("envio") or _envio_obs(it.get("obs"))).upper(),
                          loja=it.get("loja") or "", sku=it.get("sku") or "", cor=it.get("cor") or "",
                          nomes=nomes, fonte=it.get("fonte") or "", tipo=tipo, obs=it.get("obs") or "",
                          personalizado=pers, atualizado_em=agora(), **_qtd_pecas(it),
                          **({"impresso": it["impresso"]} if it.get("impresso") else {}))
            ant = c.execute("SELECT id FROM itens WHERE chave=?", (chave,)).fetchone()
            if not ant:
                # mesma etiqueta vinda de outra fonte (Zebra, automacao, PDF, bipe): junta em vez de duplicar
                n = ocorr[norm(pedido)] = ocorr.get(norm(pedido), 0) + 1
                chaves_cod = list({norm(x) for x in [pedido, campos["rastreio"], *(it.get("codigos") or [])] if norm(x)})
                cand = [r[0] for r in c.execute(f"""SELECT DISTINCT item_id FROM codigos WHERE codigo IN
                        ({",".join("?" * len(chaves_cod))}) ORDER BY item_id""", chaves_cod)]
                if len(cand) >= n:
                    iid = cand[n - 1]
                    novos = {k: v for k, v in campos.items() if v not in ("", None) and k not in ("lote", "personalizado")
                             and not (k == "canal" and v == "OUTROS")}
                    if tipo or "personalizado" in it:
                        novos["personalizado"] = pers
                    if nomes and not c.execute("SELECT personalizado FROM itens WHERE id=?", (iid,)).fetchone()[0] \
                            and "personalizado" not in it and not tipo:
                        novos["personalizado"] = 1
                    c.execute(f"UPDATE itens SET {', '.join(k + '=?' for k in novos)} WHERE id=?", list(novos.values()) + [iid])
                    n_atual += 1
                    for cod in {pedido, campos["rastreio"], *(it.get("codigos") or [])}:
                        if norm(cod):
                            c.execute("INSERT OR IGNORE INTO codigos VALUES(?,?)", (norm(cod), iid))
                    tocados.append(iid)
                    continue
            if ant:
                iid = ant["id"]
                manter = [k for k in campos if k not in ("rastreio", "envio") or campos[k]]
                sets = ", ".join(f"{k}=?" for k in manter)
                vals = [campos[k] for k in manter]
                c.execute(f"UPDATE itens SET {sets} WHERE id=?", vals + [iid])
                n_atual += 1
            else:
                cols = ", ".join(campos) + ", chave, criado_em"
                q = ", ".join("?" * (len(campos) + 2))
                iid = c.execute(f"INSERT INTO itens({cols}) VALUES({q})",
                                list(campos.values()) + [chave, agora()]).lastrowid
                n_novo += 1
            for cod in {pedido, campos["rastreio"], *(it.get("codigos") or [])}:
                if norm(cod):
                    c.execute("INSERT OR IGNORE INTO codigos VALUES(?,?)", (norm(cod), iid))
            tocados.append(iid)
        corr = _corrigir_personalizados(c, list(dict.fromkeys(tocados)))
        sem = sum(checar_falta(c, iid) for iid in dict.fromkeys(tocados))
    threading.Thread(target=lambda: _tenta_limpar(), daemon=True).start()   # junta duplicadas que este lote criou
    return {"ok": True, "lote": lote, "novos": n_novo, "atualizados": n_atual, "sem_estoque": sem,
            "virou_personalizado": len(corr)}


# ---- "sem personalizacao" por engano: etiqueta de pedido personalizado sem o nome (ex.: "BUSCAR NOME NO CHAT")
# vinha marcada como nao personalizada e pulava a gravacao. Aqui so se CORRIGE para personalizado quando ha prova.
SKUS_SEMPRE_PERS = {"7447"}
_RX_SEM_PERS = re.compile(r"SEM\s+PERSONALIZ|N[AÃ]O\s+PERSONALIZ|SEM\s+GRAVA", re.I)
_RX_PEDE_NOME = re.compile(r"BUSCAR\s+NOME|NOME\s+NO\s+CHAT|SEM\s+NOME\s+NA\s+NOTA|CHAT\b.{0,30}N[AÃ]O\s+ABRIU|FALTA\s+(O\s+)?NOME", re.I)


def _texto_personalizado(t):
    t = t or ""
    return bool(re.search(r"PERSONALIZ", t, re.I)) and not _RX_SEM_PERS.search(t)


def _prova_personalizado(c, it):
    """Motivo (texto) se ha prova de que a etiqueta e de produto personalizado; '' se nao ha."""
    if _RX_PEDE_NOME.search(it["obs"] or ""):
        return "etiqueta pede o nome (" + _RX_PEDE_NOME.search(it["obs"]).group(0).lower() + ")"
    if (it["nomes"] or "").strip():
        return "tem nome para gravar"
    skus = re.findall(r"[0-9A-Z]+", (it["sku"] or "").upper())
    if any(s in SKUS_SEMPRE_PERS for s in skus):
        return "SKU sempre personalizado"
    sp = c.execute("SELECT itens FROM shopee_pedidos WHERE order_sn=?", (norm(it["pedido"]),)).fetchone()
    if sp:
        for x in json.loads(sp[0] or "[]"):
            t = f"{x.get('sku', '')} {x.get('var', '')}"   # so SKU/variacao (o titulo do anuncio diz "personalizada" ate no liso)
            if _texto_personalizado(t):
                return f"Shopee: variacao '{t.strip()[:60]}'"
    return ""


def _corrigir_personalizados(c, ids=None, dias=30):
    """Marca como personalizado o que tem prova; guarda a lista do que foi corrigido (para conferir)."""
    desde = (datetime.now(timezone.utc) - timedelta(days=dias)).isoformat()
    q = "SELECT * FROM itens WHERE personalizado=0 AND COALESCE(criado_em,'')>=?"
    args = [desde]
    if ids is not None:
        if not ids:
            return []
        q += f" AND id IN ({','.join('?' * len(ids))})"
        args += list(ids)
    feitos = []
    for it in c.execute(q, args).fetchall():
        motivo = _prova_personalizado(c, it)
        if not motivo:
            continue
        c.execute("UPDATE itens SET personalizado=1, atualizado_em=? WHERE id=?", (agora(), it["id"]))
        feitos.append({"id": it["id"], "pedido": it["pedido"], "loja": it["loja"] or it["canal"], "sku": it["sku"],
                       "cor": it["cor"], "etiqueta": it["etiqueta"], "status": it["status"], "motivo": motivo, "em": agora()})
    if feitos:
        r = c.execute("SELECT valor FROM meta WHERE chave='pers_corrigidos'").fetchone()
        ids_novos = {f["id"] for f in feitos}
        lst = [x for x in (json.loads(r[0]) if r and r[0] else []) if x.get("id") not in ids_novos] + feitos
        c.execute("INSERT OR REPLACE INTO meta(chave, valor) VALUES('pers_corrigidos', ?)", (json.dumps(lst[-500:], ensure_ascii=False),))
        print(f"Personalizado corrigido em {len(feitos)} etiqueta(s): " + ", ".join(f["pedido"] for f in feitos[:20]), flush=True)
    return feitos


def corrigir_personalizados_todos(dias=30):
    """Procura SEM travar os bipes (so leitura); trava so por um instante para corrigir as poucas que tiverem prova."""
    desde = (datetime.now(timezone.utc) - timedelta(days=dias)).isoformat()
    with conn() as c:
        ids = [it["id"] for it in c.execute("SELECT * FROM itens WHERE personalizado=0 AND COALESCE(criado_em,'')>=?",
                                             (desde,)).fetchall() if _prova_personalizado(c, it)]
    if not ids:
        return []
    with _lock, conn() as c:
        return _corrigir_personalizados(c, ids, dias)


def personalizados_corrigidos():
    with conn() as c:
        r = c.execute("SELECT valor FROM meta WHERE chave='pers_corrigidos'").fetchone()
    lst = json.loads(r[0]) if r and r[0] else []
    return {"ok": True, "total": len(lst), "itens": lst[::-1]}


# ------------------------------------------------------------------ bipe
# leitor em modo continuo le a mesma etiqueta varias vezes seguidas: so a 1a leitura vale.
# Mesma etiqueta, mesmo posto, mesmo leitor/cracha: ignora enquanto continuar chegando em menos de REPETIDO_SEG
# (cada leitura repetida renova o prazo; tirou a etiqueta da frente do leitor por 3 s, pode bipar de novo).
REPETIDO_SEG = float(os.environ.get("REPETIDO_SEG", "3"))
_repetidos = {}
_repetidos_lock = threading.Lock()


def _leitura_repetida(posto, codigo, operador, leitor=""):
    import time
    agora_m = time.monotonic()
    k = ((posto or "").upper(), norm(codigo))   # a mesma etiqueta no mesmo setor, venha de qualquer leitor
    with _repetidos_lock:
        ult = _repetidos.get(k)
        _repetidos[k] = agora_m
        if len(_repetidos) > 5000:
            for kk in [x for x, t in _repetidos.items() if agora_m - t > 60]:
                _repetidos.pop(kk, None)
    return ult is not None and agora_m - ult < REPETIDO_SEG


_ctx = threading.local()


def _codigo_de_etiqueta(cod):
    """So inclui sozinho o que tem cara de etiqueta de envio (nao codigo de barras de produto, cracha etc.)."""
    if len(cod) < 10 or cod.startswith("CMD") or re.fullmatch(r"OP[0-9A-F]{6}", cod):
        return False
    if cod.isdigit() and len(cod) in (12, 13) or cod.isdigit() and len(cod) == 14 and not cod.startswith("9"):
        return False   # codigo de barras de produto (EAN/GTIN)
    if cod.isdigit() and len(cod) == 44:
        return False   # chave da nota fiscal (DANFE), nao e a etiqueta
    return True


def _auto_incluir(c, codigo, posto):
    """Etiqueta que nao estava na Central: entra sozinha no 1o bipe (separacao, gravacao ou expedicao).
    Se for pedido da Shopee ja lido pela API, vem com SKU, cor, quantidade e loja."""
    cod = norm(codigo)
    if not _codigo_de_etiqueta(cod):
        return None
    canal, envio = canal_por_codigo(cod)
    if re.fullmatch(r"9\d{13,15}", cod) and canal != "TIKTOK":
        canal, envio = "SHOPEE", "ENTREGA DIRETA"
    sku = cor = loja = ""
    pecas, pers, sem = [], None, []
    sp = c.execute("SELECT * FROM shopee_pedidos WHERE order_sn=?", (cod,)).fetchone()
    if sp:
        loja = sp["loja"] or ""
        canal = canal or "SHOPEE"
        for i in json.loads(sp["itens"] or "[]"):
            cr = (i.get("var") or "").split(",")[0].strip()
            pecas.append({"sku": estoque_chave(i.get("sku") or "")[0] or (i.get("sku") or ""), "cor": cr, "qtd": int(i.get("qtd") or 1)})
            t = f"{i.get('sku') or ''} {i.get('var') or ''}".upper()
            sem.append(bool(re.search(r"SEM\s+PERSONALIZ", t)))
            if _texto_personalizado(t):
                pers = 1
    else:   # pedido que veio no espelho do UpSeller (tem SKU, cor e se e personalizado)
        up = c.execute("SELECT * FROM upseller_pedidos WHERE codigo=?", (cod,)).fetchone()
        if up and up["itens"]:
            loja = up["loja"] or ""
            for x in json.loads(up["itens"] or "[]"):
                s_, cr, p_, q, _t = _ups_linha(x)
                pecas.append({"sku": s_, "cor": cr, "qtd": q})
                sem.append(p_ is False)
                if p_:
                    pers = 1
    sku = " + ".join(dict.fromkeys(p["sku"] for p in pecas if p["sku"]))
    cor = " + ".join(dict.fromkeys(p["cor"] for p in pecas if p["cor"]))
    if pers is None:   # regra: na duvida passa pela GRAVACAO; so pula se TODOS os itens dizem "sem personalizacao"
        pers = 0 if sem and all(sem) else 1
    if posto == "GRAVACAO":
        pers = 1
    iid = c.execute("""INSERT INTO itens(chave,lote,pedido,rastreio,canal,envio,loja,sku,cor,personalizado,status,obs,pecas,qtd,criado_em,atualizado_em)
                       VALUES(?,?,?,?,?,?,?,?,?,?,'AGUARDANDO',?,?,?,?,?)""",
                    (f"AUTO|{cod}", "AUTO", codigo.strip(), cod if cod.startswith("BR") else "", canal, envio, loja, sku, cor,
                     pers, "incluida no 1o bipe" + ("" if sku else " - completar SKU no painel") + ("" if not pers or any(sem) else " - conferir se e personalizada"),
                     json.dumps(pecas, ensure_ascii=False) if pecas else "", sum(p["qtd"] for p in pecas) or 1, agora(), agora())).lastrowid
    c.execute("INSERT OR IGNORE INTO codigos VALUES(?,?)", (cod, iid))
    if sp and sp["order_sn"] != cod:
        c.execute("INSERT OR IGNORE INTO codigos VALUES(?,?)", (sp["order_sn"], iid))
    return iid


def _prova_sem_pers(c, it):
    """Ha prova de que a etiqueta e SEM personalizacao? (etiqueta 'N', ou a variacao do pedido diz 'sem personalizacao')."""
    if (it.get("tipo") or "").upper() == "N":
        return True
    t = f"{it.get('sku') or ''} {it.get('cor') or ''} {it.get('obs') or ''}"
    sp = c.execute("SELECT itens FROM shopee_pedidos WHERE order_sn=?", (norm(it.get("pedido")),)).fetchone()
    if sp:
        t += " " + " ".join(f"{x.get('sku', '')} {x.get('var', '')}" for x in json.loads(sp[0] or "[]"))
    up = c.execute("SELECT itens FROM upseller_pedidos WHERE codigo=?", (norm(it.get("pedido")),)).fetchone()
    if up and up[0]:
        t += " " + " ".join(f"{x[1] if len(x) > 1 else ''} {x[2] if len(x) > 2 else ''}" for x in json.loads(up[0] or "[]"))
    return bool(re.search(r"SEM\s+PERSONALIZ", t, re.I))


def corrigir_auto_sem_prova():
    """Etiquetas incluidas no 1o bipe que ficaram como 'sem personalizacao' sem nenhuma prova (pulavam a gravacao):
    voltam a ser personalizadas se ainda nao foram expedidas."""
    with _lock, conn() as c:
        ids = []
        for it in c.execute("""SELECT * FROM itens WHERE lote='AUTO' AND personalizado=0 AND status NOT IN ('EXPEDIDO','DEVOLVIDO')""").fetchall():
            if not _prova_sem_pers(c, dict(it)):
                ids.append(it["id"])
        for iid in ids:
            c.execute("UPDATE itens SET personalizado=1, obs=COALESCE(obs,'')||' - conferir se e personalizada', atualizado_em=? WHERE id=?",
                      (agora(), iid))
    if ids:
        _op_cache["v"] = None
        print(f"{len(ids)} etiqueta(s) incluida(s) no bipe voltaram a passar pela gravacao", flush=True)
    return len(ids)


VOZES = 16


def _voz_de(codigo):
    """Cada colaborador tem o seu som (verde, amarelo e vermelho proprios). Numero fixo, escolhido na 1a vez:
    o menor que ninguem da equipe esteja usando."""
    cod = norm(codigo)
    if not cod:
        return None
    with conn() as c:
        r = c.execute("SELECT id, voz FROM colaboradores WHERE codigo=?", (cod,)).fetchone()
    if not r:
        return None
    if r["voz"] is not None:
        return r["voz"]
    with _lock, conn() as c:
        usados = {x[0] for x in c.execute("SELECT voz FROM colaboradores WHERE voz IS NOT NULL AND COALESCE(excluido,0)=0")}
        livre = next((v for v in range(VOZES) if v not in usados), r["id"] % VOZES)
        c.execute("UPDATE colaboradores SET voz=? WHERE id=? AND voz IS NULL", (livre, r["id"]))
        return c.execute("SELECT voz FROM colaboradores WHERE id=?", (r["id"],)).fetchone()[0]


# ---- "meus bipes": o computador da mesa de cada operador mostra so o que ELE bipou (o leitor sem fio manda para o PC
# central). Fica so na memoria: os ultimos bipes, com a mesma resposta que o PC central recebeu.
def _feed_add(posto, codigo, operador, leitor, r, quem=None, origem=""):
    # bipe sem cracha entra com op "" (so o computador central, em TODOS, ve)
    if quem is None:
        quem = norm(codigo) if r.get("tipo") == "operador" else norm(operador)
    it = r.get("item") or {}
    dados = {"em": datetime.now(BR).strftime("%H:%M:%S"), "op": quem, "posto": (posto or "").upper(), "leitor": leitor or "",
             "codigo": norm(codigo), "tipo": r.get("tipo"), "msg": r.get("msg"), "fazer": r.get("fazer"), "evento": r.get("evento"),
             "voz": r.get("voz"), "colaborador": r.get("colaborador"), "origem": str(origem or "")[:20],
             "item": {k: it.get(k) for k in ("sku", "cor", "nomes", "fonte", "etiqueta", "pedido", "canal")} if it else None}
    with _lock, conn() as c:   # rapido (1 linha); com a trava para nunca perder um bipe
        iid = c.execute("INSERT INTO bipes_log(em, op, dados) VALUES(?,?,?)",
                        (agora(), quem, json.dumps(dados, ensure_ascii=False, default=str))).lastrowid
        if iid % 500 == 0:   # guarda so os ultimos 2 dias
            c.execute("DELETE FROM bipes_log WHERE em<?", ((datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),))


_todos_visto = {"t": 0.0}   # ultima vez que o computador central (Bipe Boni em TODOS) pediu os bipes


def meus_bipes(op, desde=None, marcar=True):
    """Bipes do operador depois do id 'desde'. Sem 'desde': so devolve onde a fila esta (nao mostra bipe antigo)."""
    op = norm(op)
    todos = op == "TODOS"   # computador central: mostra os bipes de todo mundo (inclusive os feitos nos PCs dos colaboradores)
    if not op:
        return {"ok": False, "nome": "", "ultimo": 0, "bipes": []}
    if todos and marcar:   # a TV tambem le TODOS, mas quem "ouve" de verdade e o Bipe Boni da central
        _todos_visto["t"] = time.time()
    with conn() as c:
        col = ("Todos",) if todos else c.execute("SELECT nome FROM colaboradores WHERE codigo=? AND ativo=1", (op,)).fetchone()
        ultimo = c.execute("SELECT COALESCE(MAX(id),0) FROM bipes_log").fetchone()[0]
        L = []
        if desde is not None and desde <= ultimo:
            sql, args = (("SELECT id, dados FROM bipes_log WHERE id>? ORDER BY id DESC LIMIT 60", (desde,)) if todos else
                         ("SELECT id, dados FROM bipes_log WHERE op=? AND id>? ORDER BY id DESC LIMIT 30", (op, desde)))
            for r in c.execute(sql, args):
                x = json.loads(r[1])
                x["id"] = r[0]
                L.append(x)
    return {"ok": bool(col), "nome": col[0] if col else "", "ultimo": ultimo, "bipes": L[::-1]}


def bipar(posto, codigo, operador, modo, leitor="", origem=""):
    r = _bipar_resp(posto, codigo, operador, modo, leitor)
    if r.get("tipo") != "ignorado":
        try:
            _feed_add(posto, codigo, operador, leitor, r, origem=origem)
        except Exception as e:
            print("feed:", e, flush=True)
    return r


def _bipar_resp(posto, codigo, operador, modo, leitor=""):
    """Devolve tambem 'colaborador' (nome de quem bipou) e 'evento' (para cada coisa tocar um som diferente)."""
    if norm(codigo) and _leitura_repetida(posto, codigo, operador, leitor):
        return {"tipo": "ignorado", "evento": "ignorado", "msg": "Leitura repetida ignorada", "ignorado": True}
    _ctx.auto = False
    r = _bipar(posto, codigo, operador, modo)
    if getattr(_ctx, "auto", False):
        r["msg"] = "NOVA ETIQUETA incluída  ·  " + (r.get("msg") or "")
        r["novo"] = True
    if not r.get("evento"):
        r["evento"] = "erro" if r.get("tipo") == "erro" else ("repetido" if r.get("tipo") == "aviso" else "ok")
    if not r.get("colaborador"):
        with conn() as c:
            quem = c.execute("SELECT nome FROM colaboradores WHERE codigo=? AND ativo=1",
                             (norm(codigo) if r.get("tipo") == "operador" else norm(operador),)).fetchone()
        r["colaborador"] = quem[0] if quem else ""
    try:
        r["voz"] = _voz_de(codigo if r.get("tipo") == "operador" else operador)
    except Exception:
        r["voz"] = None
    return r


def _bipar(posto, codigo, operador, modo):
    posto = (posto or "").upper()
    cod = norm(codigo)
    with _lock, conn() as c:
        col = c.execute("SELECT * FROM colaboradores WHERE codigo=? AND ativo=1", (cod,)).fetchone()
        if col:
            return {"tipo": "operador", "operador": {"codigo": col["codigo"], "nome": col["nome"]},
                    "msg": f"Ola, {col['nome']}!", "evento": "operador", "colaborador": col["nome"]}
        op = c.execute("SELECT * FROM colaboradores WHERE codigo=? AND ativo=1", (norm(operador),)).fetchone()
        if not op:
            return {"tipo": "erro", "msg": "SEM OPERADOR: bipe o seu CRACHÁ primeiro",
                    "fazer": "Bipe o seu CRACHÁ neste leitor e depois bipe a etiqueta de novo."}
        if cod == "CMDDESFAZER":
            ev = c.execute("""SELECT e.*, i.pedido FROM eventos e JOIN itens i ON i.id=e.item_id
                   WHERE colaborador_id=? AND posto=? AND desfeito=0 ORDER BY e.id DESC LIMIT 1""",
                           (op["id"], posto)).fetchone()
            if not ev:
                return {"tipo": "aviso", "msg": "Nada para desfazer."}
            c.execute("UPDATE eventos SET desfeito=1 WHERE id=?", (ev["id"],))
            if ev["etapa"] == "DESPACHADO":
                c.execute("UPDATE itens SET despachado_em=NULL, despachado_por='' WHERE id=?", (ev["item_id"],))
            recalcular(c, ev["item_id"])
            if ev["etapa"] == "DEVOLVIDO":  # desfez a devolucao: some a ficha e o que tinha voltado ao estoque
                dv = c.execute("SELECT id FROM devolucoes WHERE item_id=?", (ev["item_id"],)).fetchone()
                if dv:
                    _dev_estornar(c, dv[0])
                    c.execute("DELETE FROM devolucoes WHERE id=?", (dv[0],))
            if c.execute("SELECT status FROM itens WHERE id=?", (ev["item_id"],)).fetchone()[0] == "AGUARDANDO":
                c.execute("DELETE FROM estoque_mov WHERE ref LIKE ?", (f"ETQ|{ev['item_id']}|%",))  # volta para o estoque
            return {"tipo": "ok", "msg": f"Desfeito: {ev['etapa']} do pedido {ev['pedido']}", "evento": "desfazer"}
        itens = c.execute("SELECT i.* FROM itens i JOIN codigos k ON k.item_id=i.id WHERE k.codigo=? "
                          "ORDER BY i.etiqueta, i.id", (cod,)).fetchall()
        if any(i["oculto"] == 2 for i in itens):   # foi encerrada sozinha, mas esta aqui de verdade: volta
            c.execute(f"UPDATE itens SET oculto=0, oculto_motivo='' WHERE oculto=2 AND id IN ({','.join('?' * len(itens))})",
                      [i["id"] for i in itens])
            itens = c.execute("SELECT i.* FROM itens i JOIN codigos k ON k.item_id=i.id WHERE k.codigo=? "
                              "ORDER BY i.etiqueta, i.id", (cod,)).fetchall()
        if not itens and posto == "DEVOLUCAO":
            itens = _itens_devolucao(c, cod)
        if not itens and posto == "DEVOLUCAO":
            iid = c.execute("""INSERT INTO itens(chave,lote,pedido,sku,personalizado,status,criado_em,atualizado_em)
                               VALUES(?,?,?,?,0,'AGUARDANDO',?,?)""",
                            (f"DEV|{cod}|{agora()}", "DEVOLUCAO", codigo.strip(), "", agora(), agora())).lastrowid
            c.execute("INSERT OR IGNORE INTO codigos VALUES(?,?)", (cod, iid))
            itens = c.execute("SELECT * FROM itens WHERE id=?", (iid,)).fetchall()
        auto_sep = False
        if not itens and posto in ("SEPARACAO", "GRAVACAO", "EXPEDICAO"):
            iid = _auto_incluir(c, codigo, posto)
            if iid:
                _ctx.auto = True
                auto_sep = posto in ("GRAVACAO", "EXPEDICAO")   # chegou na gravacao/expedicao: ja foi separada
                itens = c.execute("SELECT * FROM itens WHERE id=?", (iid,)).fetchall()
        if not itens and cod.isdigit() and len(cod) == 44:
            return {"tipo": "erro", "msg": "ESSE É O CÓDIGO DA NOTA FISCAL",
                    "fazer": "Bipe o código de barras da ETIQUETA de envio (nº do pedido ou rastreio), não o da nota fiscal."}
        if not itens:
            return {"tipo": "erro", "msg": f"ETIQUETA NÃO ENCONTRADA ({codigo})",
                    "fazer": "Separe esta etiqueta e leve para o Lucas incluir no painel (+ Incluir etiquetas). Depois bipe de novo."}

        if posto in ("SEPARACAO", "GRAVACAO", "EXPEDICAO") and modo != "FALTA":
            canc = _shopee_cancelado(c, [i["id"] for i in itens])
            if canc:
                if canc["status"] == "IN_CANCEL":
                    return {"tipo": "erro", "msg": "PEDIDO EM CANCELAMENTO NA SHOPEE — NÃO ENVIAR",
                            "fazer": "O cliente pediu para cancelar. Não separe, não grave e não despache. "
                                     "Separe esta etiqueta e entregue ao Lucas.", "item": dict(itens[0])}
                return {"tipo": "erro", "msg": "PEDIDO CANCELADO NA SHOPEE — NÃO ENVIAR",
                        "fazer": "Não separe, não grave e não despache. Separe esta etiqueta e entregue ao Lucas. "
                                 "O produto volta para a prateleira.", "item": dict(itens[0])}

        def ev(it, etapa, alerta=""):
            c.execute("INSERT INTO eventos(item_id,etapa,colaborador_id,posto,em,alerta) VALUES(?,?,?,?,?,?)",
                      (it["id"], etapa, op["id"], posto, agora(), alerta))
            recalcular(c, it["id"])
            if etapa in ("SEPARADO", "GRAVACAO_INICIO", "EXPEDIDO"):
                baixar_estoque(c, it["id"])  # baixa no estoque quando o material sai para a separacao
            return dict(c.execute("SELECT * FROM itens WHERE id=?", (it["id"],)).fetchone())

        if auto_sep:
            ev(itens[0], "SEPARADO", "incluida automaticamente")
            itens = c.execute("SELECT * FROM itens WHERE id=?", (itens[0]["id"],)).fetchall()

        if modo == "FALTA":
            it = itens[0]
            if it["falta_material"]:   # ja estava NAO TEM (as vezes marcado sozinho): a pessoa confirmou que nao tem -> zera
                z = _zerar_por_falta(c, dict(it), agora(), op["nome"])
                return {"tipo": "aviso", "msg": f"Ja estava marcado como NAO TEM - {it['sku']}" + (f"  ·  estoque de {z} zerado" if z else ""),
                        "item": dict(it)}
            r_ = ev(it, "FALTA_MATERIAL", "falta de material")
            z = _zerar_por_falta(c, dict(it), agora(), op["nome"])   # bipou NAO TEM: a prateleira esta vazia -> estoque zera
            return {"tipo": "aviso", "msg": f"FALTA DE MATERIAL registrada - {it['sku']}" + (f"  ·  estoque de {z} zerado" if z else ""),
                    "evento": "falta", "item": r_}

        if posto == "SEPARACAO":
            alvo = next((i for i in itens if ORDEM[i["status"]] < ORDEM["SEPARADO"]), None)
            if not alvo:
                return {"tipo": "aviso", "msg": "Ja separado.", "item": dict(itens[0])}
            r = ev(alvo, "SEPARADO")
            if alvo["personalizado"]:
                return {"tipo": "ok", "msg": "SEPARADO  →  vai para a GRAVAÇÃO", "evento": "separado", "item": r}
            return {"tipo": "ok", "msg": "SEPARADO  →  SEM PERSONALIZAR: direto para a EXPEDIÇÃO", "evento": "separado", "item": r}

        if posto == "GRAVACAO":
            # trouxeram para gravar uma etiqueta marcada "sem personalizacao" sem prova nenhuma: vale como personalizada
            for i in itens:
                if not i["personalizado"] and not _prova_sem_pers(c, dict(i)):
                    c.execute("UPDATE itens SET personalizado=1, atualizado_em=? WHERE id=?", (agora(), i["id"]))
            itens = c.execute(f"SELECT * FROM itens WHERE id IN ({','.join('?' * len(itens))})", [i["id"] for i in itens]).fetchall()
            pers = [i for i in itens if i["personalizado"]]
            if not pers:
                return {"tipo": "erro", "msg": "NÃO GRAVAR: produto SEM PERSONALIZAR",
                        "fazer": "Não grave. Leve direto para a EXPEDIÇÃO e bipe lá.", "item": dict(itens[0])}
            # ordem obrigatoria: separacao -> gravacao -> expedicao
            nsep = [i for i in pers if i["status"] == "AGUARDANDO"]
            if nsep:
                return {"tipo": "erro", "msg": "NÃO GRAVAR: ainda NÃO FOI SEPARADO",
                        "fazer": "Leve para a SEPARAÇÃO e bipe lá primeiro. Depois volte e bipe aqui na GRAVAÇÃO.",
                        "item": dict(nsep[0])}
            # 1o bipe = comecou a gravar (GRAVANDO); 2o bipe da mesma etiqueta = terminou (GRAVADO)
            gravando = [i for i in pers if i["status"] == "EM_GRAVACAO"]
            if gravando:
                g = gravando[0]
                ini = c.execute("SELECT em FROM eventos WHERE item_id=? AND etapa='GRAVACAO_INICIO' AND desfeito=0 "
                                "ORDER BY id DESC LIMIT 1", (g["id"],)).fetchone()
                seg = int((datetime.now(timezone.utc) - datetime.fromisoformat(ini[0])).total_seconds()) if ini else 9999
                if seg < GRAV_MIN_SEG:
                    return {"tipo": "aviso", "msg": f"Já está GRAVANDO (começou há {seg} s). Bipe de novo só quando TERMINAR.",
                            "item": dict(g)}
                r = ev(g, "GRAVACAO_FIM")
                return {"tipo": "ok", "evento": "ok", "item": r,
                        "msg": f"✅ GRAVADO em {seg // 60} min {seg % 60:02d} s  →  deixe para a EXPEDIÇÃO"}
            alvo = next((i for i in pers if ORDEM[i["status"]] < ORDEM["EM_GRAVACAO"]), None)
            if alvo:
                r = ev(alvo, "GRAVACAO_INICIO")
                return {"tipo": "ok", "evento": "gravacao", "item": r,
                        "msg": "🔥 GRAVANDO  →  quando TERMINAR, bipe esta etiqueta de novo"}
            return {"tipo": "aviso", "msg": "Já foi GRAVADO.", "item": dict(pers[0])}

        if posto == "EXPEDICAO":
            nsep = [i for i in itens if i["status"] == "AGUARDANDO"]
            if nsep:
                pers_ns = any(i["personalizado"] for i in nsep)
                return {"tipo": "erro", "msg": "NÃO DESPACHAR: ainda NÃO FOI SEPARADO",
                        "fazer": ("Leve para a SEPARAÇÃO e bipe lá. Depois GRAVAÇÃO. Só depois volte para a EXPEDIÇÃO." if pers_ns else
                                  "Leve para a SEPARAÇÃO e bipe lá. Depois volte e bipe aqui na EXPEDIÇÃO."),
                        "item": dict(nsep[0])}
            falta = [i for i in itens if i["personalizado"] and ORDEM[i["status"]] < ORDEM["EM_GRAVACAO"]]
            if falta:
                return {"tipo": "erro", "msg": "NÃO DESPACHAR: ainda NÃO FOI GRAVADO",
                        "fazer": "Leve para a GRAVAÇÃO e bipe lá. Depois volte e bipe aqui na EXPEDIÇÃO.",
                        "item": dict(falta[0])}
            pend = [i for i in itens if i["status"] != "EXPEDIDO"]
            if not pend:
                return {"tipo": "aviso", "msg": "Ja expedido.", "item": dict(itens[0])}
            r = None
            for i in pend:
                if i["status"] == "EM_GRAVACAO":   # gravador esqueceu o 2o bipe: fecha sem contar tempo
                    c.execute("INSERT INTO eventos(item_id,etapa,colaborador_id,posto,em,alerta) VALUES(?,?,?,?,?,?)",
                              (i["id"], "GRAVACAO_FIM", ultimo_op(c, i["id"]), "GRAVACAO", agora(), "fim nao bipado"))
                r = ev(i, "EXPEDIDO")
            return {"tipo": "ok", "msg": f"EXPEDIDO ({len(pend)} item(ns))", "evento": "expedido", "item": r}
        if posto == "DEVOLUCAO":
            pend = [i for i in itens if i["status"] != "DEVOLVIDO"]
            if not pend:
                return {"tipo": "aviso", "msg": "Devolucao ja registrada.", "item": dict(itens[0])}
            r, total = None, 0.0
            perda = False
            for i in pend:
                r = ev(i, "DEVOLVIDO")
                total += custo_de(c, i["sku"]) or 0
                _dev_criar(c, r, op["id"])
                perda = perda or c.execute("SELECT sugestao FROM devolucoes WHERE item_id=?", (i["id"],)).fetchone()[0] == "PERDA"
            sem_sku = any(not i["sku"] for i in pend)
            msg = f"DEVOLUÇÃO registrada ({len(pend)} item(ns))  →  " + ("GRAVADO: separe como PERDA" if perda else "confira e guarde na prateleira")
            msg += " - SKU desconhecido: completar no painel" if sem_sku else (f" - custo R$ {total:.2f}".replace(".", ",") if total else "")
            msg += f"  ·  Hoje: {_contar_hoje(c, 'devolucoes')} devolução(ões)"
            return {"tipo": "aviso" if sem_sku else "ok", "msg": msg, "evento": "devolucao", "item": r}
        return {"tipo": "erro", "msg": "SETOR NÃO ESCOLHIDO", "fazer": "Bipe a etiqueta do SETOR (Separação, Gravação, Expedição ou Devolução) e bipe de novo."}


ANTIGO_DIAS = int(os.environ.get("ANTIGO_DIAS", "4"))


def _tenta_limpar():
    try:
        limpar_etiquetas()
    except Exception as e:
        print("limpeza:", e, flush=True)


def limpar_etiquetas(aplicar=True):
    """Deixa a operacao com o que existe de verdade:
    1) DUPLICADAS: a mesma etiqueta (mesmo pedido, SKU, cor e nomes) que entrou por fontes diferentes
       (lote + PDF + e-mail + bipe) vira uma so (os codigos vao para a que ficou).
    2) ENCERRADAS: nao expedidas que ja sairam (Shopee: enviado), foram canceladas, ou estao paradas ha mais de
       ANTIGO_DIAS dias sem nenhum bipe. NAO mexe no estoque (ele ja esta certo).
    Nada e apagado: aparecem no painel e voltam sozinhas se forem bipadas."""
    hoje = datetime.now(timezone.utc)
    lim = (hoje - timedelta(days=ANTIGO_DIAS)).isoformat()
    with conn() as c:   # procura sem travar os bipes
        vivos = [dict(r) for r in c.execute("SELECT * FROM itens WHERE COALESCE(oculto,0)=0 AND COALESCE(lote,'')<>'DEVOLUCAO'")]
        nev = {r[0]: (r[1], r[2]) for r in c.execute("SELECT item_id, COUNT(*), MAX(em) FROM eventos WHERE desfeito=0 GROUP BY item_id")}
        canc = _ids_cancelados(c)
        no_ups = _codigos_upseller_frescos(c)
        saiu = {r[0] for r in c.execute("""SELECT k.item_id FROM codigos k JOIN shopee_pedidos s ON s.order_sn=k.codigo
                                           WHERE s.status IN ('SHIPPED','TO_CONFIRM_RECEIVE','COMPLETED')""")}
    ordem = {e: n for n, e in enumerate(ETAPAS)}
    grupos = {}
    for i in vivos:
        k = (norm(i["pedido"]), (i["sku"] or "").upper().strip(), (i["cor"] or "").upper().strip(),
             re.sub(r"\s+", " ", (i["nomes"] or "").upper()).strip())
        if k[0]:
            grupos.setdefault(k, []).append(i)
    dup = []   # (manter, juntar)
    for k, L in grupos.items():
        if len(L) < 2 or len({x["lote"] for x in L}) < 2:   # 2 iguais no MESMO lote = 2 etiquetas de verdade
            continue
        L.sort(key=lambda x: (-ordem.get(x["status"], 0), -(nev.get(x["id"], (0, ""))[0]), x["id"]))
        manter = L[0]
        lotes_usados = {manter["lote"]}
        for x in L[1:]:
            if x["lote"] in lotes_usados:   # outra etiqueta do mesmo lote: e outra peca, nao duplicata
                continue
            dup.append((manter["id"], x["id"]))
            lotes_usados.add(x["lote"])
    # mesma etiqueta com o pedido escrito diferente nas fontes: mesmo codigo de barras, mesmo SKU, nomes iguais ou faltando
    ja = {b for _, b in dup}
    por_id = {i["id"]: i for i in vivos}
    with conn() as c:
        cods = {}
        for cod, iid in c.execute("SELECT codigo, item_id FROM codigos"):
            if iid in por_id:
                cods.setdefault(cod, []).append(iid)
    sk = lambda i: re.split(r"[-\s]", (i["sku"] or "").upper().strip())[0]
    for cod, ids in cods.items():
        L = [por_id[x] for x in dict.fromkeys(ids) if x not in ja]
        if len(L) < 2:
            continue
        L.sort(key=lambda x: (-ordem.get(x["status"], 0), -(nev.get(x["id"], (0, ""))[0]), x["id"]))
        manter = L[0]
        for x in L[1:]:
            if x["lote"] == manter["lote"] or x["id"] in ja or (sk(x) and sk(manter) and sk(x) != sk(manter)):
                continue
            nx, nm = (x["nomes"] or "").strip().upper(), (manter["nomes"] or "").strip().upper()
            if nx and nm and nx != nm:
                continue
            if (x["cor"] or "") and (manter["cor"] or "") and x["cor"].upper() != manter["cor"].upper():
                continue
            dup.append((manter["id"], x["id"])); ja.add(x["id"])
    juntadas = {b for _, b in dup}
    enc = []
    for i in vivos:
        if i["id"] in juntadas or i["status"] in ("EXPEDIDO", "DEVOLVIDO"):
            continue
        if i["id"] in canc:
            enc.append((i["id"], "cancelado na Shopee", False))
        elif i["id"] in saiu or i["despachado_em"]:
            enc.append((i["id"], "Shopee: ja enviado (saiu sem o bipe da expedicao)", True))
        elif (i["criado_em"] or "") < lim and (nev.get(i["id"], (0, ""))[1] or "") < lim and not i["falta_material"] \
                and i["id"] not in no_ups:
            enc.append((i["id"], f"parada ha mais de {ANTIGO_DIAS} dias sem bipe", True))
    res = {"duplicadas": len(dup), "encerradas": len(enc),
           "motivos": {m: sum(1 for _, mm, _ in enc if mm == m) for m in {m for _, m, _ in enc}}}
    if not aplicar or not (dup or enc):
        return res
    with _lock, conn() as c:
        for manter, x in dup:
            for (cod,) in c.execute("SELECT codigo FROM codigos WHERE item_id=?", (x,)).fetchall():
                c.execute("INSERT OR IGNORE INTO codigos VALUES(?,?)", (cod, manter))
            c.execute("DELETE FROM codigos WHERE item_id=?", (x,))
            c.execute("UPDATE itens SET oculto=1, oculto_motivo=?, atualizado_em=? WHERE id=?",
                      (f"duplicada da etiqueta #{manter}", agora(), x))
        for iid, motivo, _ in enc:   # o estoque ja esta certo: aqui nao mexe nele
            c.execute("UPDATE itens SET oculto=2, oculto_motivo=?, atualizado_em=? WHERE id=?", (motivo, agora(), iid))
        r = c.execute("SELECT valor FROM meta WHERE chave='limpeza_log'").fetchone()
        log = (json.loads(r[0]) if r and r[0] else [])[-30:] + [{"em": agora(), **res}]
        c.execute("INSERT OR REPLACE INTO meta(chave, valor) VALUES('limpeza_log', ?)", (json.dumps(log, ensure_ascii=False),))
    _op_cache["v"] = None
    print(f"Limpeza: {len(dup)} duplicadas juntadas, {len(enc)} encerradas", flush=True)
    return res


# ------------------------------------------------------------------ espelho do UpSeller
# O UpSeller e a verdade do que ainda esta para sair (Em processo + Para enviar + Para retirada).
# A Central so mexe no que esta AGUARDANDO (sem nenhum bipe):
#   - AGUARDANDO que nao esta mais no UpSeller (ja saiu, cancelou) -> sai da conta (encerrada, volta se bipar);
#   - pedido do UpSeller que a Central nao tem -> entra como AGUARDANDO;
#   - AGUARDANDO que tinha saido da conta (antiga) mas ainda esta no UpSeller -> volta.
# Separado, gravando, gravado e expedido ficam como estao.
UPSELLER_ESTADOS = {"in_process": "Em processo", "to_ship": "Para enviar", "to_pickup": "Para retirada", "pickup_exception": "Para retirada"}
UPSELLER_FRESCO_H = 36
_RX_UPS_SEMPERS = re.compile(r"SEM\s+PERSONALIZ", re.I)
_RX_UPS_PERS = re.compile(r"PERSONALIZ|NOME|PZD", re.I)
_RX_UPS_NAOCOR = re.compile(r"PERSONALIZ|NOME|PZD|\bUND\b|UNIDADE|\b\d+\s*UN\b|^[PMGX]{1,2}\s*-", re.I)


def _ups_canal(p):
    p = (p or "").lower()
    return "TIKTOK" if "tiktok" in p else "SHOPEE" if "shopee" in p else p.upper()


def _ups_linha(x):
    """[productSku, variationSku, productAttr, qtd] -> sku, cor, personalizado (None = nao diz)."""
    sku_p, var, attr, qtd = (list(x) + [None] * 4)[:4]
    sku = re.split(r"[-\s]", str(sku_p or var or "").strip())[0].upper()
    partes = [a.strip() for a in str(attr or "").split(",") if a.strip()]
    cor = partes[0].upper() if partes and not _RX_UPS_NAOCOR.search(partes[0]) else ""
    t = f"{var or ''} {attr or ''}"
    pers = False if _RX_UPS_SEMPERS.search(t) else True if _RX_UPS_PERS.search(t) else None
    try:
        q = max(1, int(qtd or 1))
    except Exception:
        q = 1
    return sku, cor, pers, q, t.strip()


def _ups_codigos(o):
    return {norm(str(o.get(k) or "")) for k in ("id", "t", "n")} - {""}


def upseller_espelho(pedidos, aplicar=False, forcar=False):
    pedidos = [o for o in (pedidos or []) if isinstance(o, dict) and norm(str(o.get("id") or ""))]
    if len(pedidos) < 20 and not forcar:
        return {"ok": False, "erro": f"so vieram {len(pedidos)} pedidos do UpSeller; parece incompleto (nada foi mudado)"}
    cod_ped = {}
    for o in pedidos:
        for k in _ups_codigos(o):
            cod_ped[k] = o
    canais = {_ups_canal(o.get("p")) for o in pedidos}
    margem = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()   # etiqueta que acabou de chegar: deixa
    with conn() as c:
        cods = {}
        for cod, iid in c.execute("SELECT codigo, item_id FROM codigos"):
            cods.setdefault(iid, set()).add(cod)
        itens = {r["id"]: dict(r) for r in c.execute("""SELECT id, pedido, rastreio, canal, loja, sku, cor, status, oculto, oculto_motivo,
                                                         criado_em, lote FROM itens WHERE COALESCE(lote,'')<>'DEVOLUCAO'""")}
    tem = set()        # pedidos do UpSeller que a Central ja tem (qualquer etapa)
    vivos_ped = set()  # pedidos com etiqueta viva na Central
    for iid, it in itens.items():
        cs = cods.get(iid, set()) | {norm(it["pedido"]), norm(it["rastreio"])}
        ped = next((cod_ped[k]["id"] for k in cs if k in cod_ped), None)
        it["_ups"] = ped
        if ped:
            tem.add(ped)
            if not it["oculto"]:
                vivos_ped.add(ped)
    sair, voltar = [], []
    for iid, it in itens.items():
        if it["status"] != "AGUARDANDO":
            continue
        canal = (it["canal"] or "").upper() or canal_por_codigo(it["pedido"], it["rastreio"])[0]
        canal = "TIKTOK" if "TIKTOK" in canal else "SHOPEE" if "SHOPEE" in canal else canal
        if not it["oculto"] and not it["_ups"] and canal in canais and (it["criado_em"] or "") < margem:
            sair.append(it)
        elif it["oculto"] == 2 and it["_ups"] and it["_ups"] not in vivos_ped and "cancel" not in (it["oculto_motivo"] or "").lower():
            voltar.append(it)
            vivos_ped.add(it["_ups"])
    novos = [o for o in pedidos if o["id"] not in tem]
    vivos_ag = sum(1 for it in itens.values() if it["status"] == "AGUARDANDO" and not it["oculto"])
    est = {}
    for o in pedidos:
        e = UPSELLER_ESTADOS.get(o.get("e"), o.get("e") or "?")
        est[e] = est.get(e, 0) + 1
    res = {"ok": True, "pedidos_upseller": len(pedidos), "por_estado": est, "aguardando_antes": vivos_ag,
           "sair": len(sair), "voltar": len(voltar), "novos_pedidos": len(novos),
           "novas_etiquetas": sum(max(1, len(o.get("i") or [])) for o in novos),
           "exemplos_sair": [{"pedido": i["pedido"], "loja": i["loja"], "sku": i["sku"], "cor": i["cor"]} for i in sair[:15]],
           "exemplos_novos": [{"pedido": o["id"], "loja": o.get("l"), "estado": UPSELLER_ESTADOS.get(o.get("e"), o.get("e"))} for o in novos[:15]]}
    if vivos_ag >= 30 and len(sair) > 0.5 * vivos_ag and not forcar:
        res.update(ok=False, erro=f"ia tirar {len(sair)} de {vivos_ag} aguardando; parece lista incompleta (nada foi mudado)")
        return res
    if not aplicar:
        return res
    agora_ = agora()
    with _lock, conn() as c:
        for it in sair:
            c.execute("UPDATE itens SET oculto=2, oculto_motivo=?, atualizado_em=? WHERE id=? AND status='AGUARDANDO' AND COALESCE(oculto,0)=0",
                      ("nao esta mais no UpSeller (ja saiu ou foi cancelado)", agora_, it["id"]))
        for it in voltar:
            c.execute("UPDATE itens SET oculto=0, oculto_motivo='' WHERE id=? AND oculto=2", (it["id"],))
        c.execute("DELETE FROM upseller_pedidos")
        c.executemany("INSERT OR REPLACE INTO upseller_pedidos(codigo, pedido, estado, loja, em, itens) VALUES(?,?,?,?,?,?)",
                      [(k, o["id"], o.get("e") or "", o.get("l") or "", agora_, json.dumps(o.get("i") or [], ensure_ascii=False))
                       for k, o in cod_ped.items()])
        resumo = {"em": agora_, "pedidos": len(pedidos), "por_estado": est, "sair": len(sair), "voltar": len(voltar),
                  "novos_pedidos": len(novos)}
        c.execute("INSERT OR REPLACE INTO meta(chave, valor) VALUES('upseller_resumo', ?)", (json.dumps(resumo, ensure_ascii=False),))
    if novos:
        lst = []
        for o in novos:
            canal = _ups_canal(o.get("p"))
            linhas = o.get("i") or [[None, None, None, 1]]
            for n, x in enumerate(linhas, 1):
                sku, cor, pers, q, txt = _ups_linha(x)
                it = {"pedido": o["id"], "rastreio": o.get("t") or "", "canal": canal, "loja": o.get("l") or "",
                      "sku": sku, "cor": cor, "fonte": "upseller", "seq": f"ups{n}",
                      "obs": ("UpSeller: " + txt)[:200], "codigos": [o.get("n") or ""],
                      "envio": "TIKTOK" if canal == "TIKTOK" else "",
                      "pecas": [{"sku": sku, "cor": cor, "qtd": q}] if sku else []}
                if pers is False:
                    it["tipo"] = "N"
                lst.append(it)
        r = importar_lote({"lote": "UPSELLER " + datetime.now(BR).strftime("%d/%m %H:%M"), "itens": lst})
        res["criadas"] = r.get("novos", 0)
    try:
        garantir_baixas()
    except Exception as e:
        print("garantir baixas (upseller):", e, flush=True)
    _op_cache["v"] = None
    print(f"UpSeller: {len(pedidos)} pedidos; {len(sair)} sairam da conta, {len(voltar)} voltaram, {len(novos)} pedidos novos", flush=True)
    return res


def upseller_resumo():
    with conn() as c:
        r = c.execute("SELECT valor FROM meta WHERE chave='upseller_resumo'").fetchone()
    d = json.loads(r[0]) if r and r[0] else None
    if d:
        idade = (datetime.now(timezone.utc) - datetime.fromisoformat(d["em"])).total_seconds() / 3600
        d["horas"] = round(idade, 1)
        d["fresco"] = idade < UPSELLER_FRESCO_H
    return d


def _codigos_upseller_frescos(c):
    d = c.execute("SELECT valor FROM meta WHERE chave='upseller_resumo'").fetchone()
    if not d or not d[0]:
        return set()
    if (datetime.now(timezone.utc) - datetime.fromisoformat(json.loads(d[0])["em"])).total_seconds() > UPSELLER_FRESCO_H * 3600:
        return set()
    return {r[0] for r in c.execute("SELECT k.item_id FROM codigos k JOIN upseller_pedidos u ON u.codigo=k.codigo")}


def etiquetas_ocultas():
    with conn() as c:
        L = [dict(r) for r in c.execute("""SELECT id, pedido, canal, loja, sku, cor, nomes, status, criado_em, oculto, oculto_motivo, atualizado_em
                                           FROM itens WHERE COALESCE(oculto,0)>0 ORDER BY atualizado_em DESC LIMIT 1500""")]
    return {"ok": True, "duplicadas": sum(1 for x in L if x["oculto"] == 1), "encerradas": sum(1 for x in L if x["oculto"] == 2), "itens": L}


def reabrir_etiqueta(iid):
    with _lock, conn() as c:
        c.execute("UPDATE itens SET oculto=0, oculto_motivo='' WHERE id=? AND oculto=2", (int(iid),))
    _op_cache["v"] = None
    return {"ok": True}


def _contar_hoje(c, o_que, data=None):
    """Contadores do dia (horario de Brasilia): 'despachados' (etiquetas que sairam com a transportadora)
    e 'devolucoes' (pacotes de devolucao bipados/registrados)."""
    ini, fim = dia_utc(data or datetime.now(BR).strftime("%Y-%m-%d"))
    if o_que == "despachados":
        return c.execute("SELECT COUNT(*) FROM itens WHERE despachado_em>=? AND despachado_em<?", (ini, fim)).fetchone()[0]
    return c.execute("SELECT COUNT(*) FROM devolucoes WHERE em>=? AND em<?", (ini, fim)).fetchone()[0]


def contadores_dia(data=None):
    """Pacotes do dia, igual a aba Retirada do UpSeller:
    - na_retirada: etiqueta impressa (esta na Central) e a Shopee ainda nao recebeu (pedido 'PROCESSED');
    - despachados: a transportadora/ponto de coleta recebeu HOJE (Shopee mudou para 'enviado').
    Assim, de 500 na retirada, quando 200 vao para a coleta: 300 na retirada e 200 despachados.
    TikTok ainda nao entra (sem a API do TikTok nao da para saber quando foi coletado)."""
    data = data or datetime.now(BR).strftime("%Y-%m-%d")
    d0 = datetime.strptime(data, "%Y-%m-%d").replace(tzinfo=BR)
    t_ini, t_fim = int(d0.timestamp()), int((d0 + timedelta(days=1)).timestamp())
    ini, fim = dia_utc(data)
    with conn() as c:
        impressos = "order_sn IN (SELECT codigo FROM codigos)"
        ret = c.execute(f"SELECT loja, COUNT(*) n FROM shopee_pedidos WHERE status IN ('READY_TO_SHIP','PROCESSED','RETRY_SHIP') AND {impressos} GROUP BY loja").fetchall()
        # na retirada que vem de dias anteriores (etiqueta entrou na Central antes de hoje)
        ret_ant = c.execute("""SELECT COUNT(DISTINCT s.order_sn) FROM shopee_pedidos s JOIN codigos k ON k.codigo=s.order_sn
                               JOIN itens i ON i.id=k.item_id WHERE s.status IN ('READY_TO_SHIP','PROCESSED','RETRY_SHIP')
                               AND COALESCE(i.oculto,0)=0 AND i.criado_em<?""", (ini,)).fetchone()[0]
        desp = c.execute(f"""SELECT loja, COUNT(*) n FROM shopee_pedidos WHERE despachado>=? AND despachado<?
                             AND status NOT IN ('CANCELLED','IN_CANCEL') AND {impressos} GROUP BY loja""", (t_ini, t_fim)).fetchall()
        dev = c.execute("""SELECT COALESCE(k.nome,'Painel/celular') quem, COUNT(*) n FROM devolucoes d
                           LEFT JOIN colaboradores k ON k.id=d.colaborador_id WHERE d.em>=? AND d.em<? GROUP BY 1""", (ini, fim)).fetchall()
    return {"data": data,
            "despachados": sum(r["n"] for r in desp), "despachados_por_loja": {r["loja"]: r["n"] for r in desp},
            "na_retirada": sum(r["n"] for r in ret) - ret_ant, "na_retirada_anteriores": ret_ant,
            "na_retirada_por_loja": {r["loja"]: r["n"] for r in ret},
            "devolucoes": sum(r["n"] for r in dev), "devolucoes_por_pessoa": {r["quem"]: r["n"] for r in dev}}


def ultimo_op(c, iid):
    r = c.execute("SELECT colaborador_id FROM eventos WHERE item_id=? AND etapa='GRAVACAO_INICIO' AND desfeito=0 "
                  "ORDER BY id DESC LIMIT 1", (iid,)).fetchone()
    return r and r[0]


def dur_gravacao(c, iid):
    r = c.execute("SELECT em FROM eventos WHERE item_id=? AND etapa='GRAVACAO_INICIO' AND desfeito=0 "
                  "ORDER BY id DESC LIMIT 1", (iid,)).fetchone()
    if not r:
        return ""
    s = int((datetime.now(timezone.utc) - datetime.fromisoformat(r[0])).total_seconds())
    return f"{s // 60} min {s % 60:02d} s"


def recalcular(c, iid):
    st, falta = "AGUARDANDO", 0
    for e in c.execute("SELECT etapa FROM eventos WHERE item_id=? AND desfeito=0 ORDER BY id", (iid,)):
        et = e[0]
        if et == "FALTA_MATERIAL":
            falta = 1
        elif et == "SEPARADO":
            st, falta = "SEPARADO", 0
        elif et == "GRAVACAO_INICIO":
            st, falta = "EM_GRAVACAO", 0
        elif et == "GRAVACAO_FIM":
            st = "GRAVADO"
        elif et == "EXPEDIDO":
            st = "EXPEDIDO"
        elif et == "DEVOLVIDO":
            st = "DEVOLVIDO"
    c.execute("UPDATE itens SET status=?, falta_material=?, atualizado_em=? WHERE id=?", (st, falta, agora(), iid))


# ------------------------------------------------------------------ painel
def dia_utc(data):
    d = datetime.strptime(data, "%Y-%m-%d").replace(tzinfo=BR)
    return d.astimezone(timezone.utc).isoformat(), (d + timedelta(days=1)).astimezone(timezone.utc).isoformat()


def painel(data, leve=False):
    ini, fim = dia_utc(data)
    with conn() as c:
        # itens do dia = criados no dia OU com movimento no dia OU ainda nao expedidos
        # o painel "zera" todo dia: so as etiquetas que entraram HOJE ou que foram bipadas HOJE.
        # As que ficaram de dias anteriores sem bipe aparecem a parte ("de dias anteriores"), sem misturar na conta do dia.
        itens = [dict(r) for r in c.execute("""SELECT * FROM itens WHERE COALESCE(oculto,0)=0 AND ((criado_em>=? AND criado_em<?)
            OR id IN (SELECT item_id FROM eventos WHERE em>=? AND em<? AND desfeito=0)) ORDER BY canal, etiqueta, id""",
            (ini, fim, ini, fim))]
        anteriores = [dict(r) for r in c.execute("""SELECT id, pedido, canal, loja, sku, cor, status, criado_em, falta_material FROM itens
            WHERE COALESCE(oculto,0)=0 AND status NOT IN ('EXPEDIDO','DEVOLVIDO') AND COALESCE(lote,'')<>'DEVOLUCAO' AND criado_em<?
            AND id NOT IN (SELECT item_id FROM eventos WHERE em>=? AND em<? AND desfeito=0) ORDER BY criado_em""", (ini, ini, fim))]
        cont = {e: 0 for e in ETAPAS}
        for i in itens:
            cont[i["status"]] += 1
        por_canal = por_grupo_status(itens, ETAPAS)
        evs = [dict(r) for r in c.execute("""SELECT e.*, k.nome FROM eventos e LEFT JOIN colaboradores k
             ON k.id=e.colaborador_id WHERE em>=? AND em<? AND desfeito=0 ORDER BY e.id""", (ini, fim))]
        equipe = _equipe(c, data, ini, fim, leve, evs)
        esp = _espera_expedicao(c, ini, fim)
        espera = {"pecas": len(esp), "media_min": round(sum(esp) / len(esp), 1) if esp else None,
                  "max_min": round(max(esp), 1) if esp else None}
        alertas = []
        for i in itens:
            if i["falta_material"]:
                alertas.append({"tipo": "FALTA DE MATERIAL", "item": i})
        for e in evs:
            if e["alerta"] and e["alerta"] != "falta de material":
                alertas.append({"tipo": e["alerta"].upper(), "item": next((i for i in itens if i["id"] == e["item_id"]), {"pedido": "?"})})
    return {"data": data, "contagem": cont, "total": len(itens), "por_canal": por_canal,
            "pedidos": len({norm(i["pedido"]) for i in itens if i.get("lote") != "DEVOLUCAO"}),
            "anteriores": len(anteriores), "anteriores_lista": anteriores[:300],
            "anteriores_por": {**{e: sum(1 for a in anteriores if a["status"] == e) for e in ETAPAS},
                               "FALTA": sum(1 for a in anteriores if a["falta_material"])},
            "upseller": upseller_resumo(),
            "plataformas": por_plataforma(itens), "equipe": equipe, "alertas": alertas, "itens": itens,
            "dia": contadores_dia(data), "espera_expedicao": espera, "metas_equipe": None if leve else metas_equipe()}


def _equipe(c, data, ini, fim, leve=False, evs=None):
    """Produtividade de cada pessoa no dia (painel do dono): contagens, tempo por peca, parado, almoco e metas."""
    if evs is None:
        evs = [dict(r) for r in c.execute("""SELECT e.*, k.nome FROM eventos e LEFT JOIN colaboradores k
             ON k.id=e.colaborador_id WHERE em>=? AND em<? AND desfeito=0 ORDER BY e.id""", (ini, fim))]
    equipe = {}
    for e in evs:
        if e["colaborador_id"] is None:
            continue  # marcacao automatica (estoque), nao e de ninguem da equipe
        p = equipe.setdefault(e["nome"] or "?", {"separados": 0, "gravados": 0, "expedidos": 0,
                                                 "faltas": 0, "primeiro": e["em"], "ultimo": e["em"]})
        p["ultimo"] = e["em"]
        if e["etapa"] == "SEPARADO": p["separados"] += 1
        if e["etapa"] == "EXPEDIDO": p["expedidos"] += 1
        if e["etapa"] == "FALTA_MATERIAL": p["faltas"] += 1
        if e["etapa"] == "GRAVACAO_INICIO":
            p["gravados"] += 1
    tempos = {}
    for nome, _, m in ([] if leve else _gravacoes(c, ini, fim)):
        tempos.setdefault(nome, []).append(m)
    for n, p in equipe.items():
        m = sorted(tempos.get(n, []))
        p["media_gravacao_min"] = round(m[len(m) // 2], 1) if m else None   # mediana: pausa/engano nao puxa o numero
    for n, q in ({} if leve else _parados(c, data, ini, fim)).items():
        if n in equipe:
            equipe[n].update(q)
    for e in evs:
        if e["colaborador_id"] is not None and e["nome"] in equipe:
            q = equipe[e["nome"]]
            q["despachados"] = q.get("despachados", 0) + (e["etapa"] == "DESPACHADO")
            q["devolucoes"] = q.get("devolucoes", 0) + (e["etapa"] == "DEVOLVIDO")
    if not leve:
        M = metas_equipe(c)
        for n, p in equipe.items():
            p["metas"] = _metas_de(M, n)
            p["atingido"] = _atingido(p, p["metas"], next((v for k, v in M["pessoas"].items() if k.lower() == n.lower()), {}))
    return equipe


def sugestao_metas(dias=21):
    """Sugere metas por pessoa olhando os ultimos dias trabalhados: um pouco acima do normal dela
    (o dia 'bom' = 75% dos dias ela fez menos que isso). Nao salva nada: so preenche a tela."""
    hoje_ = datetime.now(BR).date()
    hist = {}
    with conn() as c:
        for k in range(1, dias + 1):
            d = (hoje_ - timedelta(days=k)).isoformat()
            ini, fim = dia_utc(d)
            for n, p in _equipe(c, d, ini, fim).items():
                if (p.get("no_trabalho_min") or 0) < 240:      # dia curto (meio periodo, saiu cedo) nao entra
                    continue
                hist.setdefault(n, []).append(p)
    def p75(v):
        v = sorted(v)
        return v[min(len(v) - 1, int(round(0.75 * (len(v) - 1))))] if v else None
    out = {}
    for n, L in hist.items():
        if len(L) < 3:
            continue
        m = {}
        for k in ("separados", "gravados", "expedidos"):
            v = [p.get(k) or 0 for p in L if (p.get(k) or 0) > 0]
            if len(v) >= 3:
                m[k] = int(round(p75(v) / 5.0) * 5) or p75(v)
        pr = [p["produtivo_pct"] for p in L if p.get("produtivo_pct") is not None]
        if pr:
            m["produtivo_pct"] = max(60, min(95, p75(pr)))
        gm = [p["media_gravacao_min"] for p in L if p.get("media_gravacao_min")]
        if len(gm) >= 3:
            gm = sorted(gm)
            m["grav_min_peca"] = round(gm[len(gm) // 4], 1)        # o ritmo dos dias mais rapidos dela
        pa = [p["parado_min"] for p in L if p.get("parado_min") is not None]
        if pa:
            m["parado_max_min"] = int(sorted(pa)[len(pa) // 4] // 5 * 5) or 15
        out[n] = {"metas": m, "dias": len(L),
                  "media": {k: round(sum(p.get(k) or 0 for p in L) / len(L), 1) for k in ("separados", "gravados", "expedidos")}}
    return {"dias_olhados": dias, "pessoas": out}


def equipe_dia(data):
    ini, fim = dia_utc(data)
    with conn() as c:
        return {"data": data, "equipe": _equipe(c, data, ini, fim), "metas_equipe": metas_equipe(c)}



_op_cache = {"t": 0, "v": None}
_op_lock = threading.Lock()


def operacao():
    """TV: varias TVs/abas pedem a cada 2 s; calcula no maximo a cada 3 s e entrega a mesma resposta a todas
    (assim a TV nunca deixa o bipe lento)."""
    import time
    with _op_lock:
        if _op_cache["v"] is not None and time.time() - _op_cache["t"] < 3:
            return _op_cache["v"]
        v = _operacao()
        _op_cache.update(t=time.time(), v=v)
        return v


def _operacao():
    """Visao geral SEM dados por funcionario (para a TV da operacao)."""
    hoje = datetime.now(BR).strftime("%Y-%m-%d")
    d = painel(hoje, leve=True)
    itens = [i for i in d["itens"] if i["status"] != "DEVOLVIDO"]
    cont = {e: 0 for e in ETAPAS if e != "DEVOLVIDO"}
    for i in itens:
        cont[i["status"]] += 1
    por_canal = por_grupo_status(itens, list(cont))
    agora_ = datetime.now(timezone.utc)
    ini, _ = dia_utc(hoje)
    ritmo, restante = {}, {
        "SEPARACAO": sum(1 for i in itens if i["status"] == "AGUARDANDO"),
        "GRAVACAO": sum(1 for i in itens if i["personalizado"] and i["status"] in ("AGUARDANDO", "SEPARADO")),
        "EXPEDICAO": sum(1 for i in itens if i["status"] not in ("EXPEDIDO", "DEVOLVIDO")),
    }
    etapa_ev = {"SEPARACAO": "SEPARADO", "GRAVACAO": "GRAVACAO_INICIO", "EXPEDICAO": "EXPEDIDO"}
    previsoes = []
    with conn() as c:
        for posto, et in etapa_ev.items():
            r = c.execute("SELECT COUNT(*), MIN(em) FROM eventos WHERE etapa=? AND desfeito=0 AND em>=?", (et, ini)).fetchone()
            n, primeiro = r[0], r[1]
            por_hora = None
            if n >= 3 and primeiro:
                horas = max((agora_ - datetime.fromisoformat(primeiro)).total_seconds() / 3600, 0.25)
                por_hora = round(n / horas, 1)
                if restante[posto]:
                    previsoes.append(restante[posto] / por_hora)
            ritmo[posto] = {"feitos": n, "por_hora": por_hora, "faltam": restante[posto]}
        # gravacao: soma do tempo aprendido de cada material que falta / gravadores ativos hoje
        tempos = tempos_por_material(c, 30)
        geral = tempos.get("__GERAL__")
        if geral and restante["GRAVACAO"]:
            falta_min = sum(tempos.get((i["sku"] or "").upper(), geral) for i in itens
                            if i["personalizado"] and i["status"] in ("AGUARDANDO", "SEPARADO"))
            ativos = c.execute("""SELECT COUNT(DISTINCT colaborador_id) FROM eventos WHERE etapa='GRAVACAO_INICIO'
                                  AND desfeito=0 AND em>=?""", ((agora_ - timedelta(hours=1)).isoformat(),)).fetchone()[0]
            previsoes.append(falta_min / max(ativos, 1) / 60)
    prev = None
    if restante["EXPEDICAO"] == 0 and itens:
        prev = "concluido"
    elif previsoes:
        prev = (agora_ + timedelta(hours=max(previsoes))).astimezone(BR).strftime("%H:%M")
    return {"contagem": cont, "total": len(itens), "por_canal": por_canal, "ritmo": ritmo,
            "pedidos": d.get("pedidos"), "anteriores": d.get("anteriores"), "anteriores_por": d.get("anteriores_por"),
            "plataformas": d["plataformas"], "upseller": d.get("upseller"),
            "previsao": prev, "falta_material": sum(1 for i in itens if i["falta_material"]), "dia": d["dia"],
            "espera_expedicao": d.get("espera_expedicao"), "gravado_mais_antigo_min": _gravado_mais_antigo()}


def _gravado_mais_antigo():
    """Ha quantos minutos a peca GRAVADA mais antiga esta esperando a expedicao."""
    with conn() as c:
        r = c.execute("""SELECT MIN(e.em) FROM eventos e JOIN itens i ON i.id=e.item_id WHERE i.status='GRAVADO'
                         AND e.etapa='GRAVACAO_FIM' AND e.desfeito=0""").fetchone()
    if not r or not r[0]:
        return None
    return int((datetime.now(timezone.utc) - datetime.fromisoformat(r[0])).total_seconds() // 60)


def _num(v):
    t = str(v or "").strip().replace("R$", "").replace(" ", "")
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    return float(t)


def custo_de(c, sku):
    v = _custo_manual(c, sku)
    if v is not None:
        return v
    total, achou = 0.0, False
    for parte in [x.strip() for x in (sku or "").split("+") if x.strip()]:
        x = xbz_de(c, parte)
        if x and x.get("preco"):
            total += x["preco"]; achou = True
    return round(total, 2) if achou else None


def _custo_manual(c, sku):
    """Custo do SKU (soma se for 'A + B'). Aceita codigo exato ou prefixo cadastrado (ex.: 06016B -> 06016B-PRETA)."""
    if not sku:
        return None
    tabela = {r[0]: r[1] for r in c.execute("SELECT UPPER(sku), custo FROM custos")}
    total, achou = 0.0, False
    for parte in [x.strip().upper() for x in sku.split("+") if x.strip()]:
        if parte in tabela:
            total += tabela[parte]; achou = True; continue
        chaves = [k for k in tabela if parte.startswith(k)]
        if chaves:
            total += tabela[max(chaves, key=len)]; achou = True
    return round(total, 2) if achou else None


def _gravacoes(c, ini, fim=None):
    """Lista (colaborador, sku, minutos) - tempo REAL de cada peca = do 1o bipe (GRAVANDO) ao 2o bipe (GRAVADO)
    na gravacao. A espera ate a expedicao NAO entra. Peca sem o 2o bipe nao conta.
    Dados antigos (antes do 2o bipe existir): intervalo ate o proximo bipe do mesmo gravador (<= 30 min)."""
    filtro = " AND a.em<?" if fim else ""
    args = (ini, fim) if fim else (ini,)
    out = []
    for nome, sku, ia, fa in c.execute(f"""SELECT k.nome, UPPER(COALESCE(i.sku,'')), a.em,
            (SELECT b.em FROM eventos b WHERE b.item_id=a.item_id AND b.etapa='GRAVACAO_FIM' AND b.desfeito=0
               AND COALESCE(b.alerta,'')='' AND b.id>a.id ORDER BY b.id LIMIT 1)
            FROM eventos a JOIN itens i ON i.id=a.item_id LEFT JOIN colaboradores k ON k.id=a.colaborador_id
            WHERE a.etapa='GRAVACAO_INICIO' AND a.desfeito=0 AND a.em>=?{filtro}""", args):
        if fa:
            m = (datetime.fromisoformat(fa) - datetime.fromisoformat(ia)).total_seconds() / 60
            if 0 < m <= 120:
                out.append((nome or "?", sku or "(sem SKU)", m))
    # dados de antes do 2o bipe
    r = c.execute("SELECT MIN(em) FROM eventos WHERE etapa='GRAVACAO_FIM' AND COALESCE(alerta,'')=''").fetchone()
    corte = r[0] if r and r[0] else "9999"
    q = """SELECT e.colaborador_id, k.nome, e.em, UPPER(COALESCE(i.sku,'')) sku FROM eventos e JOIN itens i ON i.id=e.item_id
           LEFT JOIN colaboradores k ON k.id=e.colaborador_id
           WHERE e.etapa='GRAVACAO_INICIO' AND e.desfeito=0 AND e.em>=? AND e.em<?""" + (" AND e.em<?" if fim else "") + \
        " ORDER BY e.colaborador_id, e.em"
    rows = c.execute(q, (ini, corte, fim) if fim else (ini, corte)).fetchall()
    for a, b in zip(rows, rows[1:]):
        if a[0] != b[0]:
            continue
        m = (datetime.fromisoformat(b[2]) - datetime.fromisoformat(a[2])).total_seconds() / 60
        if 0 < m <= 30:
            out.append((a[1] or "?", a[3] or "(sem SKU)", m))
    return out


PARADO_FOLGA_MIN = float(os.environ.get("PARADO_FOLGA_MIN", "3"))   # cada bipe = ~3 min de trabalho (pegar, conferir, embalar)


# ------------------------------------------------------------------ metas da equipe (painel do dono)
METAS_PADRAO = {"almoco_min": 60, "almoco_de": "11:00", "almoco_ate": "14:30",
                "separados": 0, "gravados": 0, "expedidos": 0,          # por dia (0 = sem meta)
                "produtivo_pct": 85, "grav_min_peca": 0, "parado_max_min": 0}
METAS_CAMPOS = ("separados", "gravados", "expedidos", "produtivo_pct", "grav_min_peca", "parado_max_min")


def metas_equipe(c=None):
    if c is None:
        with conn() as c2:
            return metas_equipe(c2)
    r = c.execute("SELECT valor FROM meta WHERE chave='metas_equipe'").fetchone()
    try:
        d = json.loads(r[0]) if r else {}
    except Exception:
        d = {}
    out = {**METAS_PADRAO, **{k: v for k, v in d.items() if k in METAS_PADRAO}}
    out["pessoas"] = d.get("pessoas") or {}
    return out


def salvar_metas(d):
    def num(v, lo=0, hi=100000):
        try:
            return max(lo, min(hi, float(v)))
        except Exception:
            return None
    hora = lambda v, pad: v if re.fullmatch(r"\d{2}:\d{2}", str(v or "")) else pad
    with _lock, conn() as c:
        atual = metas_equipe(c)
        novo = {"almoco_min": num(d.get("almoco_min", atual["almoco_min"]), 0, 180) or 0,
                "almoco_de": hora(d.get("almoco_de"), atual["almoco_de"]),
                "almoco_ate": hora(d.get("almoco_ate"), atual["almoco_ate"])}
        for k in METAS_CAMPOS:
            v = num(d.get(k, atual[k]), 0, 100 if k == "produtivo_pct" else 100000)
            novo[k] = v if v is not None else atual[k]
        pes = {}
        for nome, m in (d.get("pessoas") if isinstance(d.get("pessoas"), dict) else atual["pessoas"]).items():
            mm = {k: num(v) for k, v in (m or {}).items() if k in METAS_CAMPOS and v not in (None, "") and num(v) is not None}
            if mm and str(nome).strip():
                pes[str(nome).strip()] = mm
        novo["pessoas"] = pes
        c.execute("INSERT OR REPLACE INTO meta(chave,valor) VALUES('metas_equipe',?)", (json.dumps(novo, ensure_ascii=False),))
    return {"ok": True, "metas": novo}


def _metas_de(M, nome):
    """Meta da pessoa = a geral, trocada pelo que estiver preenchido so para ela."""
    pm = next((v for k, v in (M.get("pessoas") or {}).items() if k.lower() == (nome or "").lower()), {})
    return {k: pm.get(k, M.get(k, 0)) for k in METAS_CAMPOS}


def _atingido(p, m, propria=None):
    """% da meta em cada coisa (so onde tem meta). Tempo por peca e parado: quanto menor, melhor.
    Meta de quantidade geral so vale para quem fez aquela etapa no dia (quem so grava nao 'perde' a meta de separacao);
    meta colocada so para a pessoa vale sempre."""
    a = {}
    for k in ("separados", "gravados", "expedidos"):
        if m.get(k) and ((p.get(k) or 0) > 0 or k in (propria or {})):
            a[k] = round(100 * (p.get(k) or 0) / m[k])
    if m.get("produtivo_pct") and p.get("produtivo_pct") is not None:
        a["produtivo_pct"] = round(100 * p["produtivo_pct"] / m["produtivo_pct"])
    if m.get("grav_min_peca") and p.get("media_gravacao_min"):
        a["grav_min_peca"] = min(150, round(100 * m["grav_min_peca"] / p["media_gravacao_min"]))
    if m.get("parado_max_min") and p.get("parado_min") is not None:
        a["parado_max_min"] = min(150, round(100 * m["parado_max_min"] / max(p["parado_min"], 1)))
    return a


def _parados(c, data, ini, fim):
    """So para o painel do dono: quanto tempo cada pessoa ficou PARADA no dia (sem separar, gravar, expedir...).
    Ocupado = do 1o ao 2o bipe de cada peca gravando (varias maquinas ao mesmo tempo contam juntas) + uma folga
    depois de cada bipe. Parado = do 1o ao ultimo bipe do dia, menos o ocupado e menos as pausas cadastradas dela."""
    evs = c.execute("""SELECT e.colaborador_id, k.nome, e.item_id, e.etapa, e.em, COALESCE(e.alerta,'') alerta FROM eventos e
                       JOIN colaboradores k ON k.id=e.colaborador_id WHERE e.desfeito=0 AND e.em>=? AND e.em<?
                       ORDER BY e.em, e.id""", (ini, fim)).fetchall()
    pausas = c.execute("SELECT hora, pessoas, COALESCE(duracao,15) d, COALESCE(nome,'') nome FROM pausas").fetchall()
    M = metas_equipe(c)

    def tem_pausa_almoco(nome):
        return any("ALMO" in cor_norm(p["nome"]) and (not p["pessoas"] or nome.lower() in p["pessoas"].lower())
                   for p in pausas)
    d0 = datetime.strptime(data, "%Y-%m-%d").replace(tzinfo=BR)
    agora_ = datetime.now(timezone.utc)
    pes = {}
    for e in evs:
        pes.setdefault(e["nome"] or "?", []).append(e)
    out = {}
    folga = timedelta(minutes=PARADO_FOLGA_MIN)
    for nome, L in pes.items():
        t = [datetime.fromisoformat(e["em"]) for e in L]
        ini_p, fim_p = t[0], t[-1]
        ocup = []
        abertos = {}
        for e, te in zip(L, t):
            ocup.append((te, te + folga))
            if e["etapa"] == "GRAVACAO_INICIO":
                abertos[e["item_id"]] = te
            elif e["etapa"] == "GRAVACAO_FIM" and e["item_id"] in abertos:
                a = abertos.pop(e["item_id"])
                if not e["alerta"] and te - a <= timedelta(hours=2):
                    ocup.append((a, te))
        for a in abertos.values():             # ainda gravando agora (sem o 2o bipe): ocupado ate agora (max 2 h)
            if data == datetime.now(BR).strftime("%Y-%m-%d"):
                ocup.append((a, min(agora_, a + timedelta(hours=2))))
        pausas_dela = []
        for p in pausas:                        # pausa cadastrada (cafe) nao e "parado"
            quem = (p["pessoas"] or "").lower()
            if quem and nome.lower() not in quem:
                continue
            try:
                h, m = [int(x) for x in str(p["hora"]).split(":")[:2]]
            except Exception:
                continue
            a = (d0 + timedelta(hours=h, minutes=m)).astimezone(timezone.utc)
            ocup.append((a, a + timedelta(minutes=int(p["d"] or 15))))
            pausas_dela.append((a, a + timedelta(minutes=int(p["d"] or 15))))
        # junta os intervalos ocupados e mede os buracos entre o 1o e o ultimo bipe
        ocup = sorted((max(a, ini_p), min(b, fim_p)) for a, b in ocup if b > ini_p and a < fim_p)
        cur, buracos = ini_p, []
        for a, b in ocup:
            if a > cur:
                buracos.append([(a - cur).total_seconds() / 60, cur, a])
            cur = max(cur, b)
        if fim_p > cur:
            buracos.append([(fim_p - cur).total_seconds() / 60, cur, fim_p])
        # ALMOCO: se nao tem pausa "almoco" cadastrada para a pessoa, o maior buraco (>= 15 min) dentro da janela
        # do almoco (ex.: 11:00-14:30) e o almoco; desconta ate o tempo do almoco (ex.: 60 min), o que passar e parado.
        almoco = None
        if M["almoco_min"] and not tem_pausa_almoco(nome):
            jd, ja = [(d0 + timedelta(hours=int(x[:2]), minutes=int(x[3:5]))).astimezone(timezone.utc)
                      for x in (M["almoco_de"], M["almoco_ate"])]
            cand = [g for g in buracos if g[0] >= 15 and g[1] < ja and g[2] > jd]
            if cand:
                g = max(cand, key=lambda x: x[0])
                desc = min(g[0], M["almoco_min"])
                almoco = {"min": round(desc), "de": g[1].astimezone(BR).strftime("%H:%M"),
                          "ate": (g[1] + timedelta(minutes=desc)).astimezone(BR).strftime("%H:%M"),
                          "passou_min": round(g[0] - desc)}
                g[0] -= desc
                g[1] = g[1] + timedelta(minutes=desc)
        parado = sum(g[0] for g in buracos)
        maior = max(((g[0], g[1], g[2]) for g in buracos), default=None, key=lambda x: x[0])
        span = (fim_p - ini_p).total_seconds() / 60
        pausa_min = 0.0                         # pausas cadastradas (cafe...) dentro do horario dela
        for a, b in pausas_dela:
            a2, b2 = max(a, ini_p), min(b, fim_p)
            if b2 > a2:
                pausa_min += (b2 - a2).total_seconds() / 60
        base = max(span - pausa_min - (almoco["min"] if almoco else 0), 0)   # tempo que devia estar trabalhando
        out[nome] = {"parado_min": round(parado), "trabalhando_min": round(max(base - parado, 0)),
                     "pct_parado": round(100 * parado / base) if base > 0 else 0,
                     "produtivo_pct": round(100 * max(base - parado, 0) / base) if base > 0 else None,
                     "no_trabalho_min": round(span), "pausas_min": round(pausa_min), "almoco": almoco,
                     "maior_parada": ({"min": round(maior[0]), "de": maior[1].astimezone(BR).strftime("%H:%M"),
                                       "ate": maior[2].astimezone(BR).strftime("%H:%M")} if maior and maior[0] >= 1 else None),
                     "ultimo_bipe_ha_min": (int((agora_ - fim_p).total_seconds() // 60)
                                            if data == datetime.now(BR).strftime("%Y-%m-%d") else None)}
    return out


def _espera_expedicao(c, ini, fim):
    """Minutos que cada peca ficou GRAVADA esperando a expedicao (do 2o bipe da gravacao ao bipe da expedicao)."""
    out = []
    for fa, ea in c.execute("""SELECT f.em, (SELECT x.em FROM eventos x WHERE x.item_id=f.item_id AND x.etapa='EXPEDIDO'
                                  AND x.desfeito=0 AND x.id>f.id ORDER BY x.id LIMIT 1)
                               FROM eventos f WHERE f.etapa='GRAVACAO_FIM' AND f.desfeito=0 AND COALESCE(f.alerta,'')=''
                               AND f.em>=? AND f.em<?""", (ini, fim)):
        if ea:
            out.append((datetime.fromisoformat(ea) - datetime.fromisoformat(fa)).total_seconds() / 60)
    return out


_tempos_cache = {"t": 0, "dias": None, "v": None}


def tempos_por_material(c, dias):
    """Tempo medio de gravacao por material nos ultimos dias (guardado por 10 min: nao muda a cada bipe)."""
    import time
    if _tempos_cache["dias"] == dias and time.time() - _tempos_cache["t"] < 600:
        return _tempos_cache["v"]
    v = _tempos_por_material(c, dias)
    _tempos_cache.update(t=time.time(), dias=dias, v=v)
    return v


def _tempos_por_material(c, dias):
    ini = (datetime.now(timezone.utc) - timedelta(days=dias)).isoformat()
    g = _gravacoes(c, ini)
    por = {}
    for _, sku, m in g:
        por.setdefault(sku, []).append(m)
    res = {k: sum(v) / len(v) for k, v in por.items() if len(v) >= 2}
    if g:
        res["__GERAL__"] = sum(m for _, _, m in g) / len(g)
    return res


def produtividade(de, ate):
    ini, _ = dia_utc(de)
    _, fim = dia_utc(ate)
    with conn() as c:
        g = _gravacoes(c, ini, fim)
        mat, pes_mat = {}, {}
        for nome, sku, m in g:
            mat.setdefault(sku, []).append(m)
            pes_mat.setdefault((nome, sku), []).append(m)
        media = lambda v: round(sum(v) / len(v), 1)
        materiais = sorted(({"sku": k, "pecas": len(v), "media_min": media(v), "min": round(min(v), 1),
                             "max": round(max(v), 1)} for k, v in mat.items()), key=lambda x: -x["pecas"])
        pessoa_material = sorted(({"nome": n, "sku": s, "pecas": len(v), "media_min": media(v)}
                                  for (n, s), v in pes_mat.items()), key=lambda x: (x["nome"], -x["pecas"]))
        pessoas = {}
        for r in c.execute("""SELECT k.nome, e.etapa, COUNT(*), MIN(e.em), MAX(e.em) FROM eventos e
                LEFT JOIN colaboradores k ON k.id=e.colaborador_id WHERE e.desfeito=0 AND e.em>=? AND e.em<?
                AND e.colaborador_id IS NOT NULL GROUP BY k.nome, e.etapa""", (ini, fim)):
            p = pessoas.setdefault(r[0] or "?", {"nome": r[0] or "?", "separados": 0, "gravados": 0, "expedidos": 0,
                                                 "devolucoes": 0, "faltas": 0})
            chave = {"SEPARADO": "separados", "GRAVACAO_INICIO": "gravados", "EXPEDIDO": "expedidos",
                     "DEVOLVIDO": "devolucoes", "FALTA_MATERIAL": "faltas"}.get(r[1])
            if chave:
                p[chave] += r[2]
        dias_ativos = {r[0] or "?": r[1] for r in c.execute("""SELECT k.nome, COUNT(DISTINCT substr(e.em,1,10))
                FROM eventos e LEFT JOIN colaboradores k ON k.id=e.colaborador_id
                WHERE e.desfeito=0 AND e.em>=? AND e.em<? GROUP BY k.nome""", (ini, fim))}
        por_pessoa_grav = {}
        for nome, _, m in g:
            por_pessoa_grav.setdefault(nome, []).append(m)
        for n, p in pessoas.items():
            v = por_pessoa_grav.get(n)
            p["media_gravacao_min"] = media(v) if v else None
            p["dias"] = dias_ativos.get(n, 0)
            p["gravados_por_dia"] = round(p["gravados"] / p["dias"], 1) if p["dias"] else 0
        return {"de": de, "ate": ate, "materiais": materiais, "pessoa_material": pessoa_material,
                "pessoas": sorted(pessoas.values(), key=lambda x: x["nome"])}


def devolucoes(de, ate):
    ini, _ = dia_utc(de)
    _, fim = dia_utc(ate)
    with conn() as c:
        linhas = [dict(r) for r in c.execute("""SELECT i.id, i.pedido, i.rastreio, i.canal, i.loja, i.sku, i.nomes,
                e.em, k.nome quem FROM eventos e JOIN itens i ON i.id=e.item_id
                LEFT JOIN colaboradores k ON k.id=e.colaborador_id
                WHERE e.etapa='DEVOLVIDO' AND e.desfeito=0 AND e.em>=? AND e.em<? ORDER BY e.em DESC""", (ini, fim))]
        por = {}
        for l in linhas:
            l["custo"] = custo_de(c, l["sku"])
            partes = [x.strip().upper() for x in (l["sku"] or "").split("+") if x.strip()] or ["(SEM SKU)"]
            for k in partes:  # pedido com 2 produtos conta em cada produto
                cu = custo_de(c, k) if k != "(SEM SKU)" else None
                a = por.setdefault(k, {"sku": k, "qtd": 0, "custo_unit": cu, "total": 0.0})
                a["qtd"] += 1
                a["total"] = round(a["total"] + (cu or 0), 2)
        return {"linhas": linhas, "por_produto": sorted(por.values(), key=lambda x: -x["total"]),
                "total": round(sum(a["total"] for a in por.values()), 2),
                "sem_custo": sum(1 for l in linhas if l["custo"] is None)}


# ------------------------------------------------------------------ devolucoes (conferencia, estoque, motivos, Shopee a caminho)
DEV_MOTIVOS = ["Cliente desistiu / arrependimento", "Defeito / avaria", "Produto ou cor errada", "Nome errado na gravação",
               "Não entregue / não retirado", "Extravio / pacote violado", "Outro"]


def _dev_criar(c, item, colaborador_id=None):
    """Toda devolucao bipada vira uma ficha 'A CONFERIR' (uma por etiqueta)."""
    if c.execute("SELECT 1 FROM devolucoes WHERE item_id=?", (item["id"],)).fetchone():
        return
    gravado = c.execute("SELECT 1 FROM eventos WHERE item_id=? AND etapa='GRAVACAO_INICIO' AND desfeito=0", (item["id"],)).fetchone()
    sug = "PERDA" if (item["personalizado"] and gravado) else "ESTOQUE"
    ret = c.execute("SELECT return_sn, motivo FROM shopee_devolucoes WHERE order_sn=? ORDER BY criado DESC LIMIT 1",
                    (norm(item["pedido"]),)).fetchone()
    c.execute("""INSERT INTO devolucoes(item_id, pedido, canal, loja, sku, cor, personalizado, gravado, sugestao, situacao,
                 motivo, obs, custo, colaborador_id, em, return_sn) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (item["id"], item["pedido"], item["canal"], item["loja"], item["sku"], item["cor"], item["personalizado"],
               1 if gravado else 0, sug, "CONFERIR", (DEV_CAT_PT[_dev_categoria(ret[1])] if ret and ret[1] else ""), "", custo_de(c, item["sku"]),
               colaborador_id, agora(), ret[0] if ret else ""))


def _dev_estornar(c, dev_id):
    c.execute("DELETE FROM estoque_mov WHERE ref LIKE ?", (f"DEV|{dev_id}|%",))


def dev_decidir(dev_id, destino, motivo="", obs="", sku="", cor="", qtd=None):
    """ESTOQUE: a peca volta para a prateleira (entra no estoque). PERDA: nao volta (gravada, quebrada...).
    Pode mudar de ideia: refazer a decisao desfaz o estoque anterior."""
    destino = (destino or "").upper()
    if destino not in ("ESTOQUE", "PERDA", "CONFERIR"):
        return {"ok": False, "erro": "destino invalido"}
    with _lock, conn() as c:
        d = c.execute("SELECT * FROM devolucoes WHERE id=?", (int(dev_id),)).fetchone()
        if not d:
            return {"ok": False, "erro": "devolucao nao encontrada"}
        it = c.execute("SELECT * FROM itens WHERE id=?", (d["item_id"],)).fetchone()
        _dev_estornar(c, d["id"])
        entrou = []
        if destino == "ESTOQUE":
            if sku:
                pecas = [(sku, cor or "", int(qtd or 1))]
            else:
                pecas = _pecas_do_item(dict(it)) if it else []
            pecas = [(s, co, q) for s, co, q in pecas if s and not s.startswith("(")]
            if not pecas:
                return {"ok": False, "erro": "SKU desconhecido: informe SKU, cor e quantidade para voltar ao estoque"}
            # so devolve ao estoque o que tinha saido dele (etiqueta que deu baixa, ou ja descontada no saldo inicial)
            saiu = sku or (it and (_ja_baixado(c, it["id"]) or _ja_no_snapshot(c, dict(it))))
            if saiu:
                for s, co, q in pecas:
                    s, cn = estoque_chave(s, co)
                    c.execute("INSERT OR IGNORE INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                              (agora(), s, cn, q, "DEVOLUCAO", f"DEV|{d['id']}|{s}|{cn}", f"devolucao pedido {d['pedido']}"))
                    entrou.append({"sku": s, "cor": cn or "PADRAO", "qtd": q})
        c.execute("""UPDATE devolucoes SET situacao=?, motivo=COALESCE(NULLIF(?,''),motivo), obs=COALESCE(NULLIF(?,''),obs),
                     decidido_em=?, sku=CASE WHEN ?<>'' THEN ? ELSE sku END WHERE id=?""",
                  (destino, motivo, obs, agora(), sku, sku, d["id"]))
    return {"ok": True, "id": int(dev_id), "situacao": destino, "entrou_no_estoque": entrou,
            "aviso": "" if entrou or destino != "ESTOQUE" else "essa etiqueta nunca tinha saido do estoque: nada a somar"}


def dev_registrar(codigo):
    """Registrar devolucao pelo celular/painel (sem cracha), igual ao bipe do posto DEVOLUCAO."""
    cod = norm(codigo)
    if not cod:
        return {"ok": False, "erro": "informe o codigo da etiqueta ou o numero do pedido"}
    with _lock, conn() as c:
        itens = _itens_devolucao(c, cod)
        if not itens:
            iid = c.execute("""INSERT INTO itens(chave,lote,pedido,sku,personalizado,status,criado_em,atualizado_em)
                               VALUES(?,?,?,?,0,'AGUARDANDO',?,?)""",
                            (f"DEV|{cod}|{agora()}", "DEVOLUCAO", codigo.strip(), "", agora(), agora())).lastrowid
            c.execute("INSERT OR IGNORE INTO codigos VALUES(?,?)", (cod, iid))
            itens = c.execute("SELECT * FROM itens WHERE id=?", (iid,)).fetchall()
        n = 0
        for i in itens:
            if i["status"] != "DEVOLVIDO":
                c.execute("INSERT INTO eventos(item_id,etapa,colaborador_id,posto,em,alerta) VALUES(?,?,?,?,?,?)",
                          (i["id"], "DEVOLVIDO", None, "DEVOLUCAO", agora(), ""))
                recalcular(c, i["id"])
                n += 1
            _dev_criar(c, dict(c.execute("SELECT * FROM itens WHERE id=?", (i["id"],)).fetchone()))
        ids = [r[0] for r in c.execute(f"SELECT id FROM devolucoes WHERE item_id IN ({','.join('?' * len(itens))})",
                                        [i["id"] for i in itens])]
    return {"ok": True, "registradas": n, "ja_estavam": len(itens) - n, "ids": ids}


def devolucoes_tela(de, ate):
    ini, _ = dia_utc(de)
    _, fim = dia_utc(ate)
    with conn() as c:
        q = """SELECT d.*, k.nome quem FROM devolucoes d LEFT JOIN colaboradores k ON k.id=d.colaborador_id"""
        pend = [dict(r) for r in c.execute(q + " WHERE d.situacao='CONFERIR' ORDER BY d.id")]
        hist = [dict(r) for r in c.execute(q + " WHERE d.em>=? AND d.em<? ORDER BY d.id DESC", (ini, fim))]
        cam = [dict(r) for r in c.execute("""SELECT * FROM shopee_devolucoes WHERE status NOT IN ('CANCELLED','CLOSED')
                                              AND (status<>'ACCEPTED' OR atualizado>?) ORDER BY prazo""",
                                           (int(datetime.now(timezone.utc).timestamp()) - 20 * 86400,))]
        chegou = {r[0] for r in c.execute("SELECT DISTINCT pedido FROM devolucoes")}
    import time as _t
    agora_ts = _t.time()
    chegou_n = {norm(p) for p in chegou}
    with conn() as c:
        for x in cam:
            x["chegou"] = x["order_sn"] in chegou_n
            try:
                det = json.loads(x.pop("detalhe", None) or "{}")
            except Exception:
                det = {}
            inf = _dev_info_pedido(c, x["order_sn"]) if x["status"] in ("REQUESTED", "PROCESSING", "JUDGING", "SELLER_DISPUTE") else None
            x["personalizado"] = bool(inf and inf["personalizado"])
            x["sem_nome"] = bool(inf and inf["sem_nome"])
            x["tem_registro"] = bool(inf and inf["itens"])
            x["plano"] = _dev_plano(x, det, inf, agora_ts) if inf is not None else None
            p_ = x["plano"] or {}
            x["acordo_pendente"] = p_.get("etapa") == "ACORDO"
            x["prazo_loja"] = p_.get("prazo_ts") or 0
            x["responder"] = bool(p_ and x["status"] != "ACCEPTED" and p_.get("acao") != "AGUARDAR SHOPEE"
                                  and (p_.get("horas") is None or p_["horas"] > -1))
            x["fazer"] = p_.get("resumo", "")
    cam.sort(key=lambda x: (not x["responder"], x["prazo_loja"] or 9e12))
    for x in cam:
        x["itens"] = json.loads(x["itens"] or "[]")
        x["status_pt"] = SHOPEE_DEV_PT.get(x["status"], x["status"])
        x["motivo"] = DEV_CAT_PT[_dev_categoria(x["motivo"] or "", x["texto"] or "")]
        pz = x.get("prazo_loja")
        x["prazo_txt"] = datetime.fromtimestamp(pz, BR).strftime("%d/%m %H:%M") if pz else ("quando chegar" if x.get("plano") else "")
    res = {"total": len(hist), "estoque": 0, "perda": 0, "conferir": 0, "custo_perda": 0.0, "por_motivo": {}, "por_produto": {},
           "por_loja": {}}
    for h in hist:
        s = h["situacao"]
        res["estoque" if s == "ESTOQUE" else "perda" if s == "PERDA" else "conferir"] += 1
        if s == "PERDA":
            res["custo_perda"] = round(res["custo_perda"] + (h["custo"] or 0), 2)
        for chave, v in (("por_motivo", h["motivo"] or "(sem motivo)"), ("por_produto", h["sku"] or "(sem SKU)"),
                         ("por_loja", h["loja"] or h["canal"] or "(sem loja)")):
            res[chave][v] = res[chave].get(v, 0) + 1
    for chave in ("por_motivo", "por_produto", "por_loja"):
        res[chave] = sorted(res[chave].items(), key=lambda x: -x[1])
    return {"pendentes": pend, "historico": hist, "resumo": res, "a_caminho": cam, "motivos": list(DEV_CAT_PT.values()),
            "diagnostico": dev_diagnostico(),
            "hoje": contadores_dia()["devolucoes"],
            "shopee": _shopee_dev_status}


# ---- Shopee: devolucoes pedidas pelos compradores (SO LEITURA)
SHOPEE_DEV_PT = {"REQUESTED": "Pedida (responder)", "ACCEPTED": "Aceita - a caminho", "PROCESSING": "Em processamento",
                 "JUDGING": "Em análise da Shopee", "SELLER_DISPUTE": "Em disputa", "CLOSED": "Encerrada",
                 "CANCELLED": "Cancelada"}
_shopee_dev_status = {"em": "", "erro": ""}


def shopee_devolucoes_sincronizar(dias=15):
    """Le as devolucoes das lojas (SO LEITURA). Na 1a vez puxa ~6 meses de historico para aprender o que ganha.
    Para cada devolucao guarda o detalhe (motivo, provas, contestacao, resultado) sem dados pessoais do comprador."""
    import time
    with conn() as c:
        lojas = [dict(r) for r in c.execute("SELECT shop_id, nome FROM shopee_lojas")]
        hist_ok = c.execute("SELECT 1 FROM meta WHERE chave='shopee_dev_historico'").fetchone()
    fim = int(time.time())
    total_dias = dias if hist_ok else 180
    n, erros, detalhes = 0, [], 0
    for l in lojas:
        try:
            loja = _shopee_token_ok(l["shop_id"])
            sns = []
            ate = fim
            while ate > fim - total_dias * 86400:   # a Shopee aceita no maximo 15 dias por consulta
                de = max(fim - total_dias * 86400, ate - 15 * 86400 + 60)
                for pg in range(50):
                    res = _shopee_http("GET", "/api/v2/returns/get_return_list", loja=loja,
                                       params={"page_no": pg, "page_size": 100, "create_time_from": de, "create_time_to": ate})
                    rr = res.get("response") or {}
                    lst = rr.get("return") or rr.get("return_list") or []
                    with _lock, conn() as c:
                        for x in lst:
                            sn = str(x.get("return_sn"))
                            ant = c.execute("SELECT atualizado, detalhe FROM shopee_devolucoes WHERE return_sn=?", (sn,)).fetchone()
                            itens = [{"sku": i.get("variation_sku") or i.get("item_sku") or "", "nome": (i.get("name") or "")[:60],
                                      "qtd": int(i.get("amount") or 1)} for i in (x.get("item") or [])]
                            c.execute("""INSERT INTO shopee_devolucoes(return_sn, shop_id, loja, order_sn, status, motivo, texto, prazo,
                                         rastreio, valor, itens, criado, atualizado) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                                         ON CONFLICT(return_sn) DO UPDATE SET status=excluded.status, motivo=excluded.motivo,
                                         texto=excluded.texto, prazo=excluded.prazo, rastreio=excluded.rastreio, valor=excluded.valor,
                                         itens=excluded.itens, atualizado=excluded.atualizado""",
                                      (sn, l["shop_id"], loja.get("nome") or l["nome"], norm(x.get("order_sn")),
                                       x.get("status") or "", x.get("reason") or "", (x.get("text_reason") or "")[:300],
                                       int(x.get("return_seller_due_date") or x.get("due_date") or 0), norm(x.get("tracking_number") or ""),
                                       float(x.get("refund_amount") or 0), json.dumps(itens, ensure_ascii=False),
                                       int(x.get("create_time") or 0), int(x.get("update_time") or 0)))
                            if not ant or not ant[1] or int(ant[0] or 0) != int(x.get("update_time") or 0):
                                sns.append(sn)
                            n += 1
                    if not rr.get("more"):
                        break
                ate = de - 1
            for sn in dict.fromkeys(sns):   # detalhe so do que mudou (uma vez cada)
                try:
                    det = (_shopee_http("GET", "/api/v2/returns/get_return_detail", loja=loja,
                                        params={"return_sn": sn}).get("response") or {})
                except Exception as e:
                    erros.append(f"detalhe {sn}: {e}"[:150])
                    continue
                for k in ("user", "return_address", "return_pickup_address", "virtual_contact_number", "package_number"):
                    det.pop(k, None)   # nada de dado pessoal do comprador
                with _lock, conn() as c:
                    c.execute("UPDATE shopee_devolucoes SET detalhe=?, resultado=?, contestou=?, rastreio=COALESCE(NULLIF(?,''),rastreio) WHERE return_sn=?",
                              (json.dumps(det, ensure_ascii=False)[:60000], _dev_resultado(det), 1 if _dev_contestou(det) else 0,
                               norm(det.get("tracking_number") or ""), sn))
                detalhes += 1
                time.sleep(0.1)
        except Exception as e:
            erros.append(f"{l['nome'] or l['shop_id']}: {e}"[:200])
    if not erros and lojas:
        with _lock, conn() as c:
            c.execute("INSERT OR IGNORE INTO meta VALUES('shopee_dev_historico', ?)", (agora(),))
    _shopee_dev_status.update(em=datetime.now(BR).strftime("%d/%m %H:%M"), erro=" | ".join(erros[:5]))
    return {"ok": not erros, "devolucoes": n, "detalhes": detalhes, "erros": erros[:10]}


# ---- inteligencia de devolucao: o que fazer em cada caso, aprendendo com o historico das lojas
def _dev_contestou(det):
    sp = det.get("seller_proof") or {}
    return bool(det.get("dispute_reason") or det.get("dispute_text_reason") or
                (sp.get("seller_proof_status") not in (None, "", "NOT_NEEDED", "NOT_REQUIRED")) or
                det.get("status") in ("JUDGING", "SELLER_DISPUTE"))


def _dev_resultado(det):
    """GANHOU (comprador nao recebeu reembolso / compensacao aprovada), PERDEU (reembolsado), ANDAMENTO.
    Regra provisoria: ajustar quando virmos os dados reais de cada loja."""
    st = (det.get("status") or "").upper()
    comp = ((det.get("seller_compensation") or {}).get("seller_compensation_status") or "").upper()
    if "APPROVED" in comp:
        return "GANHOU"
    if st in ("REQUESTED", "PROCESSING", "JUDGING", "SELLER_DISPUTE"):
        return "ANDAMENTO"
    if st == "CANCELLED":
        return "GANHOU"
    if st in ("ACCEPTED", "CLOSED", "REFUND_PAID", "COMPLETED"):
        return "PERDEU" if float(det.get("refund_amount") or 0) > 0 else "GANHOU"
    return "ANDAMENTO"


def _dev_categoria(motivo, texto=""):
    m = f"{motivo} {texto}".upper()
    if "NOME ERRADO" in m or "GRAVA" in m:
        return "nome"
    if "RECEIPT" in m or "NOT_RECEIV" in m or "NAO RECEB" in m or "NÃO RECEB" in m or "EXTRAV" in m:
        return "nao_recebido"
    if "MISSING" in m or "EMPTY" in m or "INCOMPLE" in m or "FALT" in m:
        return "faltando"
    if "DMG" in m or "DAMAG" in m or "BROKEN" in m or "DEFECT" in m or "FUNCTION" in m or "QUEBR" in m or "AVARIA" in m or "DEFEITO" in m:
        return "danificado"
    if "WRONG" in m or "DIFF" in m or "FAKE" in m or "ERRAD" in m or "DIFEREN" in m:
        return "diferente"
    if "CHANGE" in m or "MIND" in m or "EXPECT" in m or "DESIST" in m or "ARREPEND" in m or "NO_LONGER" in m:
        return "arrependimento"
    return "outro"


DEV_CAT_PT = {"arrependimento": "Arrependimento / não quer mais", "danificado": "Chegou quebrado / com defeito",
              "diferente": "Produto, cor ou modelo diferente", "faltando": "Faltou item / pacote vazio",
              "nao_recebido": "Não recebeu", "nome": "Nome/gravação errada", "outro": "Outro motivo",
              "sem_nome": "Personalizado: comprador não mandou o nome"}

# provas que contam em cada caso (sempre verdadeiras: so o que de fato aconteceu)
DEV_VIDEO = ("VÍDEO SEM CORTE, celular na horizontal, boa luz: comece mostrando a ETIQUETA da devolução com o nº do pedido legível, "
             "gire o pacote mostrando os 6 lados FECHADO, abra na frente da câmera e mostre tudo que tem dentro, peça por peça.")
DEV_PLAY = {
    "arrependimento": {
        "fotos": ["Etiqueta da devolução com o nº do pedido legível", "Produto mostrando o NOME GRAVADO bem de perto",
                  "Print do pedido/chat em que o comprador mandou esse nome (Seller Center > pedido > mensagem do comprador)",
                  "Produto inteiro: estado em que voltou (riscos, uso, peças faltando)"],
        "texto": ("Solicito a análise desta devolução. O produto do pedido {pedido} foi PERSONALIZADO sob encomenda com o nome "
                  "\"{nome}\", exatamente como informado pelo comprador na compra (print anexo). Por ser gravado a laser com o nome "
                  "escolhido pelo cliente, o item não pode ser revendido. O produto foi entregue conforme o anúncio e sem defeito "
                  "(fotos e vídeo anexos). Peço que a devolução por desistência não seja aceita ou, se for, que o vendedor seja compensado."),
        "sem_pers": ("Solicito a análise desta devolução do pedido {pedido}. O produto foi enviado conforme o anúncio, sem defeito, "
                     "e retornou {estado}. Seguem fotos e vídeo do recebimento. Peço a compensação pelo valor do item devolvido.")},
    "danificado": {
        "fotos": ["Etiqueta da devolução com o nº do pedido legível", "Embalagem por fora (os 6 lados), mostrando se veio amassada ou violada",
                  "O dano de perto, com uma régua ou moeda para dar escala", "Peso do pacote na balança (comparar com o peso do envio)",
                  "Se tiver: vídeo/foto da EMBALAGEM NO DIA DO ENVIO (proteção usada)"],
        "texto": ("Solicito a análise da devolução do pedido {pedido}. O produto foi enviado em perfeito estado e bem protegido "
                  "({protecao}). Ao receber a devolução, constatamos: {estado}. Seguem vídeo da abertura do pacote sem cortes, fotos "
                  "da embalagem e do produto. {extra}Peço a análise e a compensação ao vendedor caso o dano tenha ocorrido no transporte ou após a entrega.")},
    "diferente": {
        "fotos": ["Etiqueta da devolução com o nº do pedido legível", "Produto devolvido ao lado da etiqueta, mostrando SKU/cor",
                  "Print da variação comprada no pedido (cor/modelo) no Seller Center", "Foto do anúncio mostrando que o produto é o mesmo"],
        "texto": ("Solicito a análise da devolução do pedido {pedido}. O comprador escolheu a variação \"{variacao}\" e recebeu exatamente "
                  "esse produto ({sku} {cor}), como mostram as fotos do item devolvido ao lado da etiqueta e o print do pedido. "
                  "O produto corresponde ao anúncio. Peço que a devolução não seja aceita / que o vendedor seja compensado.")},
    "faltando": {
        "fotos": ["Etiqueta da devolução com o nº do pedido legível", "Peso do pacote na balança",
                  "Etiqueta de ENVIO original mostrando o peso declarado (se tiver)", "Tudo que veio dentro, espalhado na mesa"],
        "texto": ("Solicito a análise da devolução do pedido {pedido}. O pedido foi enviado completo ({qtd} unidade(s) de {sku}). "
                  "Seguem vídeo da abertura da devolução sem cortes e o peso do pacote. {extra}Peço a análise junto à transportadora e a compensação ao vendedor se confirmado o envio completo.")},
    "nao_recebido": {
        "fotos": ["Print do rastreio mostrando ENTREGUE (data e hora)", "Se houver: foto/assinatura do recebedor no rastreio"],
        "texto": ("Solicito a análise do pedido {pedido}. O rastreio da transportadora mostra o pacote ENTREGUE (print anexo). "
                  "Peço que a Shopee confirme a entrega com a transportadora antes de reembolsar o comprador.")},
    "sem_nome": {
        "fotos": ["Print do CHAT mostrando que pedimos o nome e o comprador não respondeu (com data e hora)",
                  "Print do anúncio/foto que explica como mandar o nome"],
        "texto": ("Solicito a análise desta devolução. O produto do pedido {pedido} é PERSONALIZADO e o anúncio informa que o nome "
                  "deve ser enviado pelo chat após a compra. O comprador não informou o nome até o prazo de postagem exigido pela "
                  "Shopee, apesar dos nossos pedidos pelo chat (print anexo). Por isso o pedido foi enviado dentro do prazo, conforme "
                  "as regras da plataforma, e o produto foi entregue conforme o anúncio e sem defeito. Peço que a devolução não seja "
                  "aceita ou, se for, que o vendedor seja compensado.")},
    "nome": {
        "fotos": ["Produto mostrando o NOME GRAVADO bem de perto", "Print do pedido/chat com o nome que o comprador escreveu"],
        "texto": ("Solicito a análise da devolução do pedido {pedido}. O nome gravado (\"{nome}\") é exatamente o nome informado pelo "
                  "comprador na compra, como mostra o print anexo, letra por letra. A gravação foi feita conforme o pedido do cliente. "
                  "Peço que a devolução não seja aceita.")},
    "outro": {
        "fotos": ["Etiqueta da devolução com o nº do pedido legível", "Produto inteiro e de perto", "Embalagem por fora"],
        "texto": ("Solicito a análise da devolução do pedido {pedido}. O produto foi enviado conforme o anúncio. Seguem fotos e vídeo "
                  "do recebimento da devolução. Peço a análise e a compensação ao vendedor, se cabível.")},
}


def dev_aprendizado(c=None):
    """Por motivo: quantas contestamos, quantas ganhamos, e o que as ganhas tinham (prova enviada, texto usado)."""
    fechar = c is None
    c = c or conn()
    try:
        est = {}
        for r in c.execute("SELECT motivo, texto, resultado, contestou, detalhe FROM shopee_devolucoes"):
            cat = _dev_categoria(r["motivo"], r["texto"])
            e = est.setdefault(cat, {"categoria": cat, "nome": DEV_CAT_PT[cat], "casos": 0, "contestadas": 0, "ganhou": 0,
                                     "perdeu": 0, "andamento": 0, "ganhou_com_prova": 0, "perdeu_sem_prova": 0, "textos_que_ganharam": []})
            e["casos"] += 1
            if r["contestou"]:
                e["contestadas"] += 1
            res = (r["resultado"] or "ANDAMENTO").lower()
            e[res if res in ("ganhou", "perdeu", "andamento") else "andamento"] += 1
            try:
                det = json.loads(r["detalhe"] or "{}")
            except Exception:
                det = {}
            prova = (det.get("seller_proof") or {}).get("seller_proof_status") not in (None, "", "NOT_NEEDED", "NOT_REQUIRED")
            if res == "ganhou" and prova:
                e["ganhou_com_prova"] += 1
            if res == "perdeu" and not prova:
                e["perdeu_sem_prova"] += 1
            if res == "ganhou":
                for t in det.get("dispute_text_reason") or []:
                    if t and t not in e["textos_que_ganharam"]:
                        e["textos_que_ganharam"].append(str(t)[:600])
        for e in est.values():
            dec = e["ganhou"] + e["perdeu"]
            e["taxa"] = round(100 * e["ganhou"] / dec) if dec else None
            e["textos_que_ganharam"] = e["textos_que_ganharam"][-3:]
        return sorted(est.values(), key=lambda x: -x["casos"])
    finally:
        if fechar:
            c.close()


def dev_ligar_shopee(dev_id, ref):
    """Liga a devolucao bipada a devolucao da Shopee pelo nº do pedido ou da solicitacao (quando a etiqueta
    da devolucao nao bateu sozinha). Se ainda nao tiver lido essa devolucao, le a Shopee de novo (60 dias)."""
    ref = norm(ref)
    if len(ref) < 8:
        return {"ok": False, "erro": "Digite o nº do pedido Shopee (ex.: 260918ABCD1234) ou o nº da solicitação de devolução."}
    def achar():
        with conn() as c:
            return c.execute("""SELECT * FROM shopee_devolucoes WHERE order_sn=? OR return_sn=? OR (rastreio<>'' AND rastreio=?)
                                ORDER BY criado DESC LIMIT 1""", (ref, ref, ref)).fetchone()
    sr = achar()
    if not sr:
        try:
            shopee_devolucoes_sincronizar(60)
        except Exception:
            pass
        sr = achar()
    if not sr:
        return {"ok": False, "erro": f"Não achei devolução na Shopee com o nº {ref}. Confira o número no Seller Center "
                                     "(Devoluções → nº do pedido)."}
    with _lock, conn() as c:
        d = c.execute("SELECT * FROM devolucoes WHERE id=?", (int(dev_id),)).fetchone()
        if not d:
            return {"ok": False, "erro": "devolucao nao encontrada"}
        c.execute("UPDATE devolucoes SET return_sn=?, loja=COALESCE(NULLIF(loja,''),?), motivo=COALESCE(NULLIF(motivo,''),?) WHERE id=?",
                  (sr["return_sn"], sr["loja"] or "", sr["motivo"] or "", int(dev_id)))
        cod = norm(d["pedido"])
        if cod and cod != sr["order_sn"] and not sr["rastreio"]:   # proxima vez a etiqueta ja bate sozinha
            c.execute("UPDATE shopee_devolucoes SET rastreio=? WHERE return_sn=?", (cod, sr["return_sn"]))
    return {"ok": True, "return_sn": sr["return_sn"], "pedido": sr["order_sn"], "loja": sr["loja"]}


def dev_orientacao(dev_id):
    """O que fazer com esta devolucao: contestar ou nao, prazo, fotos/video e o texto pronto para a Shopee."""
    with conn() as c:
        d = c.execute("SELECT * FROM devolucoes WHERE id=?", (int(dev_id),)).fetchone()
        if not d:
            return {"ok": False, "erro": "devolucao nao encontrada"}
        it = c.execute("SELECT * FROM itens WHERE id=?", (d["item_id"],)).fetchone()
        sr = c.execute("""SELECT * FROM shopee_devolucoes WHERE return_sn=? OR order_sn=? OR (rastreio<>'' AND rastreio=?)
                          ORDER BY criado DESC LIMIT 1""", (d["return_sn"] or "-", norm(d["pedido"]), norm(d["pedido"]))).fetchone()
        apr = {e["categoria"]: e for e in dev_aprendizado(c)}
    it = dict(it) if it else {}
    sr = dict(sr) if sr else None
    cat = _dev_categoria((sr or {}).get("motivo") or d["motivo"] or "", (sr or {}).get("texto") or "")
    pers = bool(d["personalizado"])
    nome = (it.get("nomes") or "").strip()
    if pers and cat in ("arrependimento", "diferente", "nome", "outro") and (not nome or _RX_PEDE_NOME.search(it.get("obs") or "")):
        cat = "sem_nome"   # o comprador nao mandou o nome: o caso e outro (e o mais forte para nos)
    play = DEV_PLAY[cat]
    e = apr.get(cat)
    textos_ok = (e or {}).get("textos_que_ganharam") or []
    campos = {"pedido": (sr or {}).get("order_sn") or d["pedido"], "nome": nome or "(nome gravado)", "sku": d["sku"] or "(SKU)",
              "cor": d["cor"] or "", "variacao": f"{d['sku'] or ''} {d['cor'] or ''}".strip() or "(variação)",
              "qtd": it.get("qtd") or 1, "estado": "[descreva como voltou: lacrado / usado / riscado / sem caixa]",
              "protecao": "plástico bolha + caixa", "extra": "Também anexamos a foto/vídeo da embalagem no dia do envio. " if cat in ("danificado", "faltando") else ""}
    base = play["texto"] if (cat != "arrependimento" or pers) else play["sem_pers"]
    texto = base.format(**campos)
    fotos = list(play["fotos"])
    if pers and cat not in ("arrependimento", "nome"):
        fotos.append("Produto mostrando o NOME GRAVADO (prova de que é personalizado e não pode ser revendido)")
    # chance: historico das lojas para esse motivo + forca da prova
    if cat == "sem_nome":
        rec, porque = "CONTESTAR", ("O comprador não mandou o nome e o pedido saiu no prazo da Shopee. Mande o PRINT DO CHAT com os "
                                    "pedidos de nome sem resposta + o comprovante de produção (🧾). Se a Shopee aceitar mesmo assim, "
                                    "peça compensação.")
    elif cat == "nome" and not nome:
        rec, porque = "CONFERIR", "Confira se o nome gravado é igual ao que o cliente escreveu. Se for igual: CONTESTE. Se nós erramos: aceite a devolução."
    elif cat == "arrependimento" and pers:
        rec, porque = "CONTESTAR", "Produto personalizado com o nome do cliente: é o caso mais forte para contestar."
    elif e and e["taxa"] is not None and (e["ganhou"] + e["perdeu"]) >= 5:
        rec = "CONTESTAR" if e["taxa"] >= 20 else "CONTESTAR COM VÍDEO"
        porque = f"Histórico das lojas nesse motivo: ganhamos {e['ganhou']} de {e['ganhou'] + e['perdeu']} ({e['taxa']}%)."
        if e["perdeu_sem_prova"]:
            porque += f" {e['perdeu_sem_prova']} das perdidas foram SEM prova enviada: mande sempre o vídeo e as fotos."
    else:
        rec, porque = "CONTESTAR COM VÍDEO", "Ainda há pouco histórico nesse motivo: a chance sobe muito com o vídeo sem corte e as fotos abaixo."
    prazo, prazo_vencido = "", False
    if sr and sr.get("prazo"):
        import time as _t
        prazo = datetime.fromtimestamp(sr["prazo"], BR).strftime("%d/%m %H:%M")
        prazo_vencido = sr["prazo"] < _t.time()
    with conn() as c2:
        em = c2.execute("SELECT valor FROM meta WHERE chave='dev_email'").fetchone()
    return {"ok": True, "prazo_vencido": prazo_vencido, "midias": json.loads(d["midias"] or "[]"), "enviado_em": d["enviado_em"] or "",
            "envio": json.loads(d["envio_resp"] or "{}"), "email": em[0] if em else "",
            "id": d["id"], "pedido": campos["pedido"], "loja": d["loja"] or (sr or {}).get("loja") or "",
            "categoria": cat, "motivo": DEV_CAT_PT[cat], "motivo_comprador": (sr or {}).get("texto") or "",
            "shopee": {"return_sn": sr["return_sn"], "situacao": SHOPEE_DEV_PT.get(sr["status"], sr["status"])} if sr else None,
            "prazo": prazo, "recomendacao": rec, "porque": porque, "video": DEV_VIDEO, "fotos": fotos, "texto": texto,
            "textos_que_ganharam": textos_ok, "personalizado": pers, "nome_gravado": nome,
            "como_enviar": ["Seller Center > Devolução/Reembolso > abra a devolução deste pedido",
                            "Toque em Contestar (ou Enviar provas) dentro do prazo",
                            "Anexe o vídeo e as fotos e cole o texto (ajuste o que estiver entre [colchetes])",
                            "Envie SÓ o que é verdade: prova falsa pode bloquear a loja"]}



def dev_relatorio(data=None):
    """Relatorio do dia: devolucoes da Shopee que mudaram no dia (aceitas, ganhas, em analise), o que foi contestado,
    com quantas fotos/videos (das que passaram pela Central) e por que cada perdida perdeu."""
    data = data or datetime.now(BR).strftime("%Y-%m-%d")
    d0 = datetime.strptime(data, "%Y-%m-%d").replace(tzinfo=BR)
    t0, t1 = int(d0.timestamp()), int((d0 + timedelta(days=1)).timestamp())
    with conn() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM shopee_devolucoes WHERE atualizado>=? AND atualizado<?", (t0, t1))]
        cent = {}
        for r in c.execute("SELECT pedido, return_sn, midias, enviado_em, envio_resp, situacao FROM devolucoes"):
            for k in (norm(r["pedido"]), r["return_sn"] or ""):
                if k:
                    cent[k] = dict(r)
    itens, tot = [], {"total": 0, "ganhas": 0, "perdidas": 0, "em_analise": 0, "contestadas": 0, "com_foto": 0, "com_video": 0,
                      "pela_central": 0, "valor_perdido": 0.0, "valor_ganho": 0.0, "perdidas_por_prazo": 0, "perdidas_contestadas": 0,
                      "so_reembolso": 0}
    for r in rows:
        try:
            d = json.loads(r["detalhe"] or "{}")
        except Exception:
            d = {}
        cr = cent.get(r["order_sn"]) or cent.get(r["return_sn"]) or {}
        md = json.loads(cr.get("midias") or "[]") if cr else []
        fotos, videos = sum(m["tipo"] == "foto" for m in md), sum(m["tipo"] == "video" for m in md)
        v = float(r["valor"] or 0)
        res = r["resultado"] or "ANDAMENTO"
        rsd, upd = int(d.get("return_seller_due_date") or 0), int(d.get("update_time") or r["atualizado"] or 0)
        if res == "PERDEU":
            if d.get("return_solution") == 1:
                porque = "reembolso sem devolver o produto (decidido no pedido, 48 h)"
                tot["so_reembolso"] += 1
            elif r["contestou"]:
                porque = "contestada, mas a Shopee aceitou" + (" (sem prova anexada pela API)" if not (fotos or videos) else "")
                tot["perdidas_contestadas"] += 1
            elif rsd and 0 <= upd - rsd < 6 * 3600:
                porque = "prazo para contestar venceu sem resposta"
                tot["perdidas_por_prazo"] += 1
            else:
                porque = "aceita sem contestar"
        elif res == "GANHOU":
            porque = "cancelada pelo comprador / sem reembolso" if r["status"] == "CANCELLED" else "a Shopee deu razão à loja"
        else:
            porque = "em análise / aguardando"
        tot["total"] += 1
        tot["ganhas"] += res == "GANHOU"
        tot["perdidas"] += res == "PERDEU"
        tot["em_analise"] += res == "ANDAMENTO"
        tot["contestadas"] += bool(r["contestou"])
        tot["com_foto"] += bool(fotos)
        tot["com_video"] += bool(videos)
        tot["pela_central"] += bool(cr.get("enviado_em"))
        tot["valor_perdido"] += v if res == "PERDEU" else 0
        tot["valor_ganho"] += v if res == "GANHOU" else 0
        itens.append({"pedido": r["order_sn"], "loja": r["loja"], "motivo": DEV_CAT_PT[_dev_categoria(r["motivo"] or "", r["texto"] or "")],
                      "cliente": (r["texto"] or "")[:120], "valor": round(v, 2), "situacao": SHOPEE_DEV_PT.get(r["status"], r["status"]),
                      "resultado": res, "contestou": bool(r["contestou"]), "fotos": fotos, "videos": videos,
                      "enviado_pela_central": bool(cr.get("enviado_em")), "porque": porque,
                      "hora": datetime.fromtimestamp(upd, BR).strftime("%H:%M") if upd else "",
                      "contestacao": (d.get("dispute_reason") or [""])[0] if isinstance(d.get("dispute_reason"), list) else (d.get("dispute_reason") or "")})
    tot["valor_perdido"] = round(tot["valor_perdido"], 2)
    tot["valor_ganho"] = round(tot["valor_ganho"], 2)
    ordem = {"PERDEU": 0, "ANDAMENTO": 1, "GANHOU": 2}
    itens.sort(key=lambda x: (ordem.get(x["resultado"], 3), not x["contestou"], -x["valor"]))
    return {"ok": True, "data": data, "resumo": tot, "itens": itens}


# ---- PLANO DE ACAO de cada devolucao aberta: prazo certo, o que fazer, fotos, texto, chance e se vale a pena.
# Regras tiradas do historico das 4 lojas (out/2026): ~metade das devolucoes que voltaram foi aceita SOZINHA porque o prazo
# do vendedor passou sem resposta; as contestadas foram sem prova anexada. Chance e estimativa honesta, nao promessa.
DEV_EM_TRANSITO = {"LOGISTICS_NOT_STARTED", "LOGISTICS_PENDING_ARRANGE", "LOGISTICS_REQUEST_CREATED", "LOGISTICS_READY",
                   "LOGISTICS_PICKUP_DONE", "LOGISTICS_PICKUP_RETRY", "LOGISTICS_PICKUP_FAILED", ""}
DEV_VALOR_MIN = float(os.environ.get("DEV_VALOR_MIN", "15"))   # abaixo disso, com chance baixa, nao compensa o tempo


def _dev_plano(x, det, inf, agora_ts):
    cat = _dev_categoria(x.get("motivo") or det.get("reason") or "", x.get("texto") or det.get("text_reason") or "")
    pers, sem_nome = bool(inf and inf["personalizado"]), bool(inf and inf["sem_nome"])
    if pers and sem_nome and cat in ("arrependimento", "diferente", "nome", "outro"):
        cat = "sem_nome"
    valor = float(det.get("refund_amount") or x.get("valor") or 0)
    st = det.get("status") or x.get("status") or ""
    neg = det.get("negotiation") or {}
    rls = det.get("reverse_logistics_status") or ""
    so_reembolso = det.get("return_solution") == 1
    sp = det.get("seller_proof") or {}
    rsd = int(det.get("return_seller_due_date") or 0)
    due = int(det.get("due_date") or x.get("prazo") or 0)
    chegou = bool(x.get("chegou")) or rls == "LOGISTICS_DELIVERY_DONE"
    # etapa e prazo certo de cada uma
    if st in ("JUDGING", "SELLER_DISPUTE"):
        if (sp.get("seller_proof_status") or "") == "PENDING":
            etapa, prazo = "PROVA", int(sp.get("seller_evidence_deadline") or 0)
        else:
            etapa, prazo = "ANALISE", 0
    elif neg.get("negotiation_status") == "PENDING_RESPOND":
        etapa, prazo = "ACORDO", int(neg.get("offer_due_date") or 0) or (due if due > agora_ts else rsd)
    elif st == "REQUESTED":
        etapa, prazo = "PEDIDO", due
    elif chegou:
        etapa, prazo = "CHEGOU", rsd
    else:
        etapa, prazo = "A_CAMINHO", rsd
    horas = round((prazo - agora_ts) / 3600, 1) if prazo else None
    # o que fazer e a chance (honesta)
    fotos = list(DEV_PLAY[cat]["fotos"])
    if pers or cat == "sem_nome":
        fotos.insert(1, "🧾 Comprovante de produção (botão 🧾: a Central monta com os bipes)")
    acao, chance, porque = "CONTESTAR", "MÉDIA", ""
    if cat == "nao_recebido":
        porque = ("Só ganha se o rastreio mostrar ENTREGUE (de preferência com foto/assinatura/endereço). "
                  "Se o rastreio não mostrar entregue, aceite: não tem prova.")
    elif cat == "faltando":
        acao, chance = ("CONTESTAR", "BAIXA") if valor >= 40 else ("ACEITAR", "BAIXA")
        porque = ("Sem vídeo/foto do pacote fechado no envio e sem o peso, a Shopee quase sempre dá razão ao comprador. "
                  "Conteste só com o peso do envio (etiqueta) igual ao peso que deveria ter.")
    elif cat == "danificado":
        chance = "MÉDIA" if not so_reembolso else "BAIXA"
        porque = ("Ganha quando o VÍDEO SEM CORTE da abertura mostra o produto inteiro/sem o defeito ou o pacote violado "
                  "(aí a culpa é do transporte e a Shopee compensa). Sem vídeo, perde.")
    elif cat == "sem_nome":
        chance = "MÉDIA"
        porque = ("O comprador não mandou o nome e o pedido saiu no prazo da Shopee. A prova que decide é o PRINT DO CHAT "
                  "com os nossos pedidos de nome sem resposta (com data e hora).")
    elif cat == "nome":
        acao = "CONFERIR"
        porque = ("Compare o nome gravado com o que o cliente escreveu, letra por letra. Igual: conteste com o print "
                  "(chance ALTA). Nós erramos: aceite e regrave/reenvie.")
    elif cat == "diferente":
        chance = "MÉDIA"
        porque = (("Personalizado: mostre que a gravação/variação é exatamente a do pedido (print do pedido/chat + foto + comprovante 🧾)."
                   if pers else "Mostre que a variação enviada é a comprada (produto ao lado da etiqueta + print da variação no pedido).")
                  + " Se mandamos errado de fato: aceite.")
    elif cat == "arrependimento":
        if pers:
            acao, chance = "ACORDO", "BAIXA"
            porque = ("A Shopee aceita arrependimento por lei (7 dias), mesmo em personalizado: contestar quase nunca ganha. "
                      "O melhor é propor ACORDO: devolver uma parte (30% a 50%) e o cliente fica com o produto.")
        else:
            acao, chance = "ACEITAR", "BAIXA"
            porque = ("Sem personalização o produto volta e entra no estoque: não vale contestar. "
                      "Só conteste se voltar usado, sujo ou quebrado (aí peça compensação).")
    else:
        porque = "Conteste com fotos e vídeo do que voltou."
    if acao == "CONTESTAR" and chance == "BAIXA" and valor < DEV_VALOR_MIN:
        acao = "ACEITAR"
    vale = "SIM" if acao in ("CONTESTAR", "CONFERIR", "ACORDO") and (chance != "BAIXA" or acao == "ACORDO") and valor >= DEV_VALOR_MIN \
        else "TALVEZ" if acao in ("CONTESTAR", "CONFERIR", "ACORDO") else "NÃO"
    # passos na ordem certa para esta etapa
    quando = datetime.fromtimestamp(prazo, BR).strftime("%d/%m às %H:%M") if prazo else ""
    passos = []
    if etapa == "PEDIDO":
        passos.append(f"Responder o PEDIDO de devolução na Shopee até {quando} (sem resposta, a Shopee aceita sozinha).")
    if etapa == "ACORDO":
        of = neg.get("latest_offer_amount")
        passos.append(f"O comprador propôs {('R$ %.2f' % of) if of else 'o reembolso'}. Você pode fazer UMA contraproposta"
                      + (f" até {quando}" if quando else "") + ": botão 🤝 Acordo.")
    if etapa == "A_CAMINHO":
        passos.append("O pacote ainda está vindo. Quando chegar: NÃO abra sem filmar.")
        if quando:
            passos.append(f"Prazo para contestar depois que chegar: {quando}. Se passar, a Shopee aceita sozinha.")
    if etapa in ("CHEGOU", "A_CAMINHO") and acao in ("CONTESTAR", "CONFERIR"):
        passos.append("Grave o VÍDEO SEM CORTE: etiqueta da devolução legível → os 6 lados do pacote fechado → abrir → mostrar tudo.")
        passos.append("Tire as fotos da lista abaixo (no celular: página Devoluções → bipe a etiqueta → 📋 O que fazer).")
        passos.append("Envie a contestação com o texto pronto ANTES do prazo" + (f" ({quando})" if quando else "") + ".")
    if etapa == "PROVA":
        passos.append(f"A Shopee PEDIU PROVA: envie as fotos/vídeo até {quando} (Seller Center → devolução → Enviar provas).")
    if etapa == "ANALISE":
        passos.append("Em análise da Shopee: aguarde. Se pedir prova, aparece aqui com prazo.")
    if acao == "ACEITAR":
        passos.append("Pode aceitar (ou deixar a Shopee decidir). Quando o produto chegar, confira: se voltou usado/danificado, conteste pedindo compensação.")
    if acao == "ACORDO" and etapa != "ACORDO":
        passos.append("Proponha o acordo pelo botão 🤝 (reembolso parcial sem devolver o produto). Se o comprador recusar, conteste com o comprovante 🧾.")
    nome = ", ".join(inf["nomes"]) if inf and inf["nomes"] else "(nome gravado)"
    it0 = (inf["itens"][0] if inf and inf["itens"] else {})
    if not it0:   # a Central nao tem a etiqueta: usa o que a Shopee diz do item
        try:
            its = x["itens"] if isinstance(x.get("itens"), list) else json.loads(x.get("itens") or "[]")
            it0 = {"sku": (its[0].get("sku") or its[0].get("nome") or "") if its else "", "cor": "", "qtd": its[0].get("qtd") if its else 1}
        except Exception:
            it0 = {}
    campos = {"pedido": x.get("order_sn") or "", "nome": nome, "sku": it0.get("sku") or "(SKU)", "cor": it0.get("cor") or "",
              "variacao": f"{it0.get('sku') or ''} {it0.get('cor') or ''}".strip() or "(variação)", "qtd": it0.get("qtd") or 1,
              "estado": "[descreva como voltou: lacrado / usado / riscado / sem caixa]", "protecao": "plástico bolha + caixa",
              "extra": ""}
    base = DEV_PLAY[cat]["texto"] if (cat != "arrependimento" or pers) else DEV_PLAY[cat]["sem_pers"]
    try:
        texto = base.format(**campos)
    except Exception:
        texto = base
    resumo = {"CONTESTAR": "Contestar", "ACORDO": "Propor acordo", "ACEITAR": "Pode aceitar", "CONFERIR": "Conferir o nome e contestar"}[acao] \
        if etapa not in ("ANALISE",) else "Aguardar a Shopee"
    if etapa == "ANALISE":
        acao = "AGUARDAR SHOPEE"
    return {"etapa": etapa, "prazo_ts": prazo, "horas": horas, "acao": acao, "chance": chance, "vale": vale, "categoria": cat,
            "motivo": DEV_CAT_PT[cat], "valor": round(valor, 2), "porque": porque, "passos": passos, "fotos": fotos, "texto": texto,
            "resumo": resumo if acao == "AGUARDAR SHOPEE" else f"{resumo} · chance {chance} · vale a pena: {vale}"}


_diag_cache = {"em": 0, "v": None}


def dev_diagnostico():
    """O que o historico das lojas mostra (para melhorar): onde estamos perdendo dinheiro e por que. (guarda 10 min)"""
    import time as _t
    if _diag_cache["v"] is not None and _t.time() - _diag_cache["em"] < 600:
        return _diag_cache["v"]
    _diag_cache.update(v=_dev_diagnostico(), em=_t.time())
    return _diag_cache["v"]


def _dev_diagnostico():
    with conn() as c:
        rows = c.execute("SELECT status, motivo, texto, valor, contestou, resultado, detalhe FROM shopee_devolucoes").fetchall()
    tot = len(rows)
    if not tot:
        return None
    perdido = tempo = tempo_v = sem_prova = sem_prova_v = cont = cont_ganhou = so_reemb = so_reemb_v = 0
    por_cat = {}
    for r in rows:
        try:
            d = json.loads(r["detalhe"] or "{}")
        except Exception:
            d = {}
        cat = _dev_categoria(r["motivo"] or "", r["texto"] or "")
        e = por_cat.setdefault(cat, {"categoria": DEV_CAT_PT[cat], "casos": 0, "perdidas": 0, "valor": 0.0, "por_prazo": 0})
        e["casos"] += 1
        v = float(r["valor"] or 0)
        if r["contestou"]:
            cont += 1
            cont_ganhou += r["resultado"] == "GANHOU"
            if (d.get("seller_proof") or {}).get("seller_proof_status") in (None, "", "NOT_NEEDED", "NOT_REQUIRED"):
                sem_prova += 1
                sem_prova_v += v if r["resultado"] == "PERDEU" else 0
        if r["resultado"] != "PERDEU":
            continue
        perdido += v
        e["perdidas"] += 1
        e["valor"] += v
        if d.get("return_solution") == 1:
            so_reemb += 1
            so_reemb_v += v
        rsd, upd = int(d.get("return_seller_due_date") or 0), int(d.get("update_time") or 0)
        if rsd and 0 <= upd - rsd < 6 * 3600 and not r["contestou"]:
            tempo += 1
            tempo_v += v
            e["por_prazo"] += 1
    for e in por_cat.values():
        e["valor"] = round(e["valor"])
    achados = [f"Em {tot} devoluções lidas, R$ {perdido:,.0f} voltaram para os compradores.".replace(",", "."),
               f"⏰ {tempo} foram aceitas SOZINHAS porque o prazo para contestar passou sem resposta (R$ {tempo_v:,.0f}). "
               "É o maior vazamento: responder no prazo já muda o jogo.".replace(",", "."),
               f"📸 Contestamos {cont} e {sem_prova} foram SEM prova anexada (a Shopee decide pelas fotos/vídeo: sem prova, perde).",
               f"📦 {so_reemb} foram reembolso SEM devolver o produto (R$ {so_reemb_v:,.0f}): aqui só ganha respondendo o pedido em 48 h com prova "
               "(rastreio entregue, peso, vídeo do empacotamento).".replace(",", ".")]
    return {"total": tot, "perdido": round(perdido), "por_prazo": tempo, "por_prazo_valor": round(tempo_v), "contestadas": cont,
            "contestadas_ganhas": cont_ganhou, "sem_prova": sem_prova, "so_reembolso": so_reemb,
            "por_categoria": sorted(por_cat.values(), key=lambda e: -e["valor"]), "achados": achados}


# ---- devolucao de PERSONALIZADO: comprovante de producao (registros reais da Central) e acordo (reembolso parcial)
def _dev_info_pedido(c, order_sn):
    """O que a Central sabe do pedido: personalizado?, nome pedido, se o comprador mandou o nome, e quem fez cada etapa."""
    on = norm(order_sn)
    ids = [r[0] for r in c.execute("SELECT DISTINCT item_id FROM codigos WHERE codigo=?", (on,))]
    its = [dict(r) for r in c.execute(f"SELECT * FROM itens WHERE id IN ({','.join('?' * len(ids))}) AND COALESCE(lote,'')<>'DEVOLUCAO'"
                                       f" AND COALESCE(oculto,0)<>1", ids)] if ids else []
    if not its:
        its = [dict(r) for r in c.execute("SELECT * FROM itens WHERE pedido=? AND COALESCE(lote,'')<>'DEVOLUCAO'", (order_sn,))]
    ids = [i["id"] for i in its]
    evs = [dict(r) for r in c.execute(f"""SELECT e.item_id, e.etapa, e.em, k.nome FROM eventos e LEFT JOIN colaboradores k
                                          ON k.id=e.colaborador_id WHERE e.desfeito=0 AND e.item_id IN ({','.join('?' * len(ids))})
                                          ORDER BY e.em""", ids)] if ids else []
    sp = c.execute("SELECT * FROM shopee_pedidos WHERE order_sn=?", (on,)).fetchone()
    var = []
    if sp:
        try:
            var = [f"{x.get('sku', '')} {x.get('var', '')}".strip() for x in json.loads(sp["itens"] or "[]")]
        except Exception:
            var = []
    pers = any(i["personalizado"] for i in its)
    nomes = [i["nomes"].strip() for i in its if (i["nomes"] or "").strip()]
    sem_nome = pers and (not nomes or any(_RX_PEDE_NOME.search(i["obs"] or "") for i in its))
    return {"pedido": order_sn, "itens": its, "eventos": evs, "variacoes": var, "personalizado": pers, "nomes": nomes,
            "sem_nome": sem_nome, "loja": (its[0]["loja"] if its else "") or (sp["loja"] if sp else "")}


ETAPA_PT = {"SEPARADO": "Separado", "GRAVACAO_INICIO": "Gravação iniciada", "GRAVACAO_FIM": "Gravação terminada",
            "EXPEDIDO": "Embalado e expedido", "FALTA_MATERIAL": "Aguardou material", "DEVOLVIDO": "Devolução recebida"}


def dev_comprovante_jpeg(order_sn):
    """Imagem 'registro de producao' do pedido, so com o que a Central registrou (bipes com hora e quem fez).
    E prova verdadeira: nao inventa nada; o que nao existe aparece como 'nao registrado'."""
    from PIL import Image, ImageDraw, ImageFont
    with conn() as c:
        info = _dev_info_pedido(c, order_sn)
    if not info["itens"]:
        return None

    def fonte(tam, negrito=False):
        for f in (f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if negrito else ''}.ttf",
                  os.path.join(AQUI, "web", "Poppins-SemiBold.ttf")):
            try:
                return ImageFont.truetype(f, tam)
            except Exception:
                pass
        try:
            return ImageFont.load_default(size=tam)
        except Exception:
            return ImageFont.load_default()
    W = 1080
    linhas = []   # (texto, tamanho, negrito, cor)
    linhas.append(("REGISTRO DE PRODUÇÃO DO PEDIDO", 46, True, (17, 24, 39)))
    linhas.append((f"Pedido {info['pedido']}" + (f"  ·  Loja {info['loja']}" if info["loja"] else ""), 32, True, (55, 65, 81)))
    linhas.append(("", 14, False, None))
    for i in info["itens"]:
        linhas.append((f"Produto: {i['sku'] or '-'} {i['cor'] or ''}".strip() + (f"  ·  {i['qtd']} un." if i.get("qtd") else ""), 30, False, (17, 24, 39)))
        linhas.append(("Tipo: PERSONALIZADO (gravado a laser sob encomenda)" if i["personalizado"] else "Tipo: sem personalização", 30, True,
                       (185, 28, 28) if i["personalizado"] else (55, 65, 81)))
        if i["personalizado"]:
            if (i["nomes"] or "").strip():
                linhas.append((f"Nome pedido pelo comprador: \"{i['nomes'].strip()}\"" + (f"  ·  fonte {i['fonte']}" if i.get("fonte") else ""), 30, False, (17, 24, 39)))
            else:
                linhas.append(("Nome: o comprador NÃO informou o nome até o envio", 30, True, (185, 28, 28)))
        if i.get("etiqueta"):
            linhas.append((f"Etiqueta nº {i['etiqueta']}", 26, False, (75, 85, 99)))
    for v in info["variacoes"][:3]:
        linhas.append((f"Variação comprada na Shopee: {v}", 26, False, (75, 85, 99)))
    linhas.append(("", 14, False, None))
    linhas.append(("Linha do tempo (horário de Brasília):", 30, True, (17, 24, 39)))
    if info["eventos"]:
        for e in info["eventos"][:14]:
            quando = datetime.fromisoformat(e["em"]).astimezone(BR).strftime("%d/%m/%Y %H:%M")
            linhas.append((f"  {quando}  —  {ETAPA_PT.get(e['etapa'], e['etapa'])}" + (f"  ({e['nome']})" if e["nome"] else ""), 28, False, (31, 41, 55)))
    else:
        linhas.append(("  (sem bipes registrados para este pedido)", 28, False, (107, 114, 128)))
    linhas.append(("", 14, False, None))
    linhas.append(("Registro interno do sistema de produção da loja: cada etapa é registrada", 22, False, (107, 114, 128)))
    linhas.append(("pela leitura do código de barras da etiqueta, com data, hora e funcionário.", 22, False, (107, 114, 128)))
    alturas = [(f, fonte(t, b)) for (f, t, b, _) in linhas]
    H = 60 + sum((t + 16) for (_, t, _, _) in linhas) + 40
    img = Image.new("RGB", (W, max(H, 600)), (255, 255, 255))
    dr = ImageDraw.Draw(img)
    dr.rectangle([0, 0, W, 14], fill=(124, 58, 237))
    y = 50
    for (txt, tam, neg, cor), (_, fnt) in zip(linhas, alturas):
        if txt:
            while dr.textlength(txt, font=fnt) > W - 80 and len(txt) > 10:   # corta o que nao cabe
                txt = txt[:-2]
            dr.text((40, y), txt, font=fnt, fill=cor)
        y += tam + 16
    out = io.BytesIO()
    img.save(out, "JPEG", quality=88)
    return out.getvalue()


def dev_comprovante_anexar(dev_id):
    """Coloca o comprovante de producao nas fotos desta devolucao (vai junto na contestacao)."""
    with conn() as c:
        d = c.execute("SELECT pedido FROM devolucoes WHERE id=?", (int(dev_id),)).fetchone()
        sr = c.execute("SELECT order_sn FROM shopee_devolucoes WHERE order_sn=? OR rastreio=?", (norm(d[0] if d else ""),) * 2).fetchone() if d else None
    if not d:
        return {"ok": False, "erro": "devolucao nao encontrada"}
    jpg = dev_comprovante_jpeg(sr[0] if sr else d[0])
    if not jpg:
        return {"ok": False, "erro": "a Central nao tem registro de producao deste pedido"}
    return dev_midia_salvar(dev_id, "foto", "Comprovante de produção (registro da Central)", jpg, "jpg")


def _ret_loja(return_sn):
    with conn() as c:
        r = c.execute("SELECT shop_id, status, valor FROM shopee_devolucoes WHERE return_sn=?", (str(return_sn),)).fetchone()
    if not r:
        raise RuntimeError("devolucao da Shopee nao encontrada (atualize a lista)")
    return _shopee_token_ok(r[0]), r


def _solucoes(res):
    rr = res.get("response") or {}
    lst = rr.get("solution") or rr.get("solutions") or rr.get("solution_list") or rr.get("available_solutions") or []
    if isinstance(lst, dict):
        lst = [lst]
    out = []
    for x in lst if isinstance(lst, list) else []:
        if not isinstance(x, dict):
            continue
        sol = x.get("solution", x.get("solution_type"))
        mx = x.get("max_refund_amount", x.get("max_refund", x.get("refund_amount")))
        try:
            mx = float(mx) if mx is not None else None
        except (TypeError, ValueError):
            mx = None
        out.append({"solucao": sol, "max": mx, "nome": ("Reembolso sem devolver o produto" if str(sol) in ("1", "REFUND_ONLY")
                                                       else "Devolver o produto e reembolsar" if str(sol) in ("0", "RETURN_REFUND")
                                                       else str(sol))})
    return out


def dev_solucoes(return_sn):
    """O que a Shopee deixa propor nesta devolucao (so leitura): ex.: reembolso parcial sem o produto voltar."""
    loja, r = _ret_loja(return_sn)
    res = _shopee_http("GET", "/api/v2/returns/get_available_solutions", loja=loja, params={"return_sn": str(return_sn)},
                       aceitar_erro=True)
    if res.get("error"):
        return {"ok": False, "erro": f"A Shopee não liberou acordo agora: {res.get('error')} {res.get('message', '')}".strip()[:200]}
    sol = _solucoes(res)
    return {"ok": bool(sol), "solucoes": sol, "valor_pedido": r[2], "erro": "" if sol else "A Shopee não ofereceu opção de acordo para esta devolução."}


def dev_oferecer(return_sn, solucao, valor):
    """Propoe o acordo ao comprador (so quando a pessoa confirma no celular/painel). Confere na Shopee que entrou."""
    try:
        valor = round(float(str(valor).replace(",", ".")), 2)
    except (TypeError, ValueError):
        return {"ok": False, "erro": "valor invalido"}
    if valor <= 0:
        return {"ok": False, "erro": "valor invalido"}
    op = dev_solucoes(return_sn)
    if not op.get("ok"):
        return op
    s = next((x for x in op["solucoes"] if str(x["solucao"]) == str(solucao)), None)
    if not s:
        return {"ok": False, "erro": "essa opção não está mais disponível"}
    if s["max"] is not None and valor > s["max"] + 0.001:
        return {"ok": False, "erro": f"o máximo para essa opção é R$ {s['max']:.2f}"}
    loja, _ = _ret_loja(return_sn)
    sol = s["solucao"]
    try:
        sol = int(sol)
    except (TypeError, ValueError):
        pass
    res = _shopee_http("POST", "/api/v2/returns/offer", loja=loja, aceitar_erro=True,
                       corpo={"return_sn": str(return_sn), "proposed_solution": sol, "proposed_adjusted_refund_amount": valor})
    if res.get("error"):
        return {"ok": False, "erro": f"A Shopee recusou: {res.get('error')} {res.get('message', '')}".strip()[:200]}
    try:   # confere
        det = _shopee_http("GET", "/api/v2/returns/get_return_detail", loja=loja, params={"return_sn": str(return_sn)}).get("response") or {}
        neg = det.get("negotiation") or {}
    except Exception:
        neg = {}
    with _lock, conn() as c:
        r = c.execute("SELECT valor FROM meta WHERE chave='dev_acordos'").fetchone()
        log = (json.loads(r[0]) if r and r[0] else [])[-300:] + [{"em": agora(), "return_sn": str(return_sn), "solucao": s["nome"], "valor": valor,
                                                                   "negociacao": neg.get("negotiation_status", "")}]
        c.execute("INSERT OR REPLACE INTO meta(chave, valor) VALUES('dev_acordos', ?)", (json.dumps(log, ensure_ascii=False),))
    return {"ok": True, "valor": valor, "solucao": s["nome"], "negociacao": neg.get("negotiation_status", ""),
            "msg": f"Proposta enviada: {s['nome']} de R$ {valor:.2f}. Agora o comprador aceita ou recusa."}


# ---- fotos/video da devolucao (tirados pelo celular) e envio da contestacao para a Shopee
DEV_MIDIA = os.path.join(os.path.dirname(os.path.abspath(DB)), "midia_devolucoes")
DEV_MIDIA_MAX = 200 * 1024 * 1024   # video de ate ~200 MB


def _dev_pasta(dev_id):
    p = os.path.join(DEV_MIDIA, str(int(dev_id)))
    os.makedirs(p, exist_ok=True)
    return p


def dev_midia_salvar(dev_id, tipo, rotulo, dados, ext):
    """Guarda a foto/video na Central (fica de prova mesmo se a Shopee pedir de novo)."""
    with conn() as c:
        if not c.execute("SELECT 1 FROM devolucoes WHERE id=?", (int(dev_id),)).fetchone():
            return {"ok": False, "erro": "devolucao nao encontrada"}
    if not dados:
        return {"ok": False, "erro": "arquivo vazio"}
    tipo = "video" if tipo == "video" else "foto"
    ext = re.sub(r"[^a-z0-9]", "", (ext or "").lower())[:5] or ("mp4" if tipo == "video" else "jpg")
    nome = f"{tipo}_{datetime.now(BR).strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(3)}.{ext}"
    with open(os.path.join(_dev_pasta(dev_id), nome), "wb") as f:
        f.write(dados)
    with _lock, conn() as c:
        r = c.execute("SELECT midias FROM devolucoes WHERE id=?", (int(dev_id),)).fetchone()
        lst = json.loads(r[0] or "[]") if r else []
        lst.append({"arquivo": nome, "tipo": tipo, "rotulo": str(rotulo or "")[:120], "bytes": len(dados), "em": agora()})
        c.execute("UPDATE devolucoes SET midias=? WHERE id=?", (json.dumps(lst, ensure_ascii=False), int(dev_id)))
    return {"ok": True, "arquivo": nome, "midias": lst}


def dev_midia_apagar(dev_id, arquivo):
    with _lock, conn() as c:
        r = c.execute("SELECT midias FROM devolucoes WHERE id=?", (int(dev_id),)).fetchone()
        lst = [m for m in json.loads((r[0] if r else None) or "[]") if m["arquivo"] != arquivo]
        c.execute("UPDATE devolucoes SET midias=? WHERE id=?", (json.dumps(lst, ensure_ascii=False), int(dev_id)))
    try:
        os.remove(os.path.join(_dev_pasta(dev_id), os.path.basename(arquivo)))
    except Exception:
        pass
    return {"ok": True, "midias": lst}


def _shopee_multipart(path, campos, arquivos, loja=None):
    """POST multipart assinado (imagem/parte de video). arquivos = [(campo, nome, bytes, tipo)]."""
    import urllib.request, urllib.parse, time
    pid, key = _shopee_cred()
    ts = int(time.time())
    q = {"partner_id": pid, "timestamp": ts}
    if loja:
        q.update(access_token=loja["access_token"], shop_id=loja["shop_id"])
        q["sign"] = _shopee_assina(key, pid, path, ts, loja["access_token"], loja["shop_id"])
    else:
        q["sign"] = _shopee_assina(key, pid, path, ts)
    lim = "----boni" + secrets.token_hex(8)
    corpo = b""
    for k, v in (campos or {}).items():
        corpo += f"--{lim}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode()
    for campo, nome, dados, tipo in arquivos:
        corpo += (f"--{lim}\r\nContent-Disposition: form-data; name=\"{campo}\"; filename=\"{nome}\"\r\n"
                  f"Content-Type: {tipo}\r\n\r\n").encode() + dados + b"\r\n"
    corpo += f"--{lim}--\r\n".encode()
    req = urllib.request.Request(SHOPEE_HOST + path + "?" + urllib.parse.urlencode(q), data=corpo, method="POST",
                                 headers={"Content-Type": f"multipart/form-data; boundary={lim}"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            res = json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            res = json.loads(e.read() or b"{}")
        except Exception:
            res = {}
        if not res.get("error"):
            res = {"error": f"http {e.code}", "message": res.get("message", "")}
    if res.get("error"):
        raise RuntimeError(f"{res.get('error')}: {res.get('message', '')}"[:200])
    return res


def _shopee_video(loja, dados):
    """Sobe o video pelo media_space (em partes de 4 MB) e devolve o video_upload_id."""
    import time
    md5 = hashlib.md5(dados).hexdigest()
    ini = _shopee_http("POST", "/api/v2/media_space/init_video_upload", corpo={"file_md5": md5, "file_size": len(dados)})
    vid = (ini.get("response") or {}).get("video_upload_id")
    if not vid:
        raise RuntimeError("video: sem video_upload_id")
    parte, partes, t0 = 4 * 1024 * 1024, [], time.time()
    for seq, k in enumerate(range(0, len(dados), parte)):
        pedaco = dados[k:k + parte]
        _shopee_multipart("/api/v2/media_space/upload_video_part",
                          {"video_upload_id": vid, "part_seq": seq, "content_md5": hashlib.md5(pedaco).hexdigest()},
                          [("part_content", f"parte{seq}", pedaco, "application/octet-stream")])
        partes.append(seq)
    _shopee_http("POST", "/api/v2/media_space/complete_video_upload",
                 corpo={"video_upload_id": vid, "part_seq_list": partes,
                        "report_data": {"upload_cost": int((time.time() - t0) * 1000)}})
    return vid


def _motivo_disputa(lista, cat):
    """Escolhe, na lista de motivos de contestacao que a Shopee oferece, o que combina com o caso."""
    chaves = {"arrependimento": ["PERSONALI", "CUSTOM", "NÃO PODE", "NAO PODE", "CONDI", "USAD", "USED", "CHANGE"],
              "danificado": ["DANIF", "DAMAG", "INTACT", "PERFEIT", "GOOD CONDITION", "TRANSPORT"],
              "diferente": ["CORRET", "CORRECT", "SAME", "MESMO", "DESCRI", "CONFORME"],
              "faltando": ["COMPLET", "PESO", "WEIGHT", "MISSING", "FALT"],
              "nao_recebido": ["ENTREG", "DELIVER", "RECEB", "RECEIV"],
              "nome": ["PERSONALI", "CUSTOM", "SOLICIT", "REQUEST"], "outro": []}.get(cat, [])
    opc = []
    for x in lista or []:
        rid = x.get("reason_id", x.get("dispute_reason_id", x.get("id")))
        txt = str(x.get("reason_text") or x.get("text") or x.get("reason") or "")
        if rid is not None:
            opc.append((rid, txt))
    for ch in chaves:
        for rid, txt in opc:
            if ch in txt.upper():
                return rid, txt
    return (opc[0] if opc else (None, ""))


_CAMPO_FOTO = {"nome": None}


def _urls_em(x):
    if isinstance(x, str):
        return [x] if x.startswith("http") else []
    if isinstance(x, dict):
        return [u for v in x.values() for u in _urls_em(v)]
    if isinstance(x, list):
        return [u for v in x for u in _urls_em(v)]
    return []


FOTO_ALVO = int(os.environ.get("FOTO_ALVO_KB", "180")) * 1024   # a Shopee recusa (HTTP 413) foto grande


def _foto_menor(dados, lado=1280, alvo=FOTO_ALVO):
    """Recomprime a foto (JPEG) ate ficar abaixo do alvo. Sem Pillow devolve como esta."""
    try:
        from PIL import Image, ImageOps
        im = ImageOps.exif_transpose(Image.open(io.BytesIO(dados)))
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
    except Exception:
        return dados
    melhor = dados
    for lado_ in (lado, 1024, 900, 800, 700, 600, 500):
        if lado_ > lado:
            continue
        x = im.copy()
        x.thumbnail((lado_, lado_))
        for q in (85, 75, 65, 55):
            b = io.BytesIO()
            x.save(b, "JPEG", quality=q, optimize=True)
            if len(b.getvalue()) < len(melhor):
                melhor = b.getvalue()
            if len(melhor) <= alvo:
                return melhor
    return melhor


def _dev_converter_foto(loja, nome, dados, sn=""):
    """Sobe a foto para a Shopee (returns/convert_image) e devolve a URL. A Shopee nao documenta bem o nome do campo
    do arquivo: tenta os nomes conhecidos e guarda o que funcionou. Foto grande e diminuida antes (e de novo se der 413)."""
    if len(dados) > FOTO_ALVO:
        dados = _foto_menor(dados)
    nome = re.sub(r"\.[a-z0-9]+$", "", nome, flags=re.I) + ".jpg"
    nomes = [_CAMPO_FOTO["nome"]] if _CAMPO_FOTO["nome"] else ["upload_image", "image", "images", "file", "upload_images"]
    ult, lado = "", 1024
    i = 0
    while i < len(nomes):
        campo = nomes[i]
        try:
            res = _shopee_multipart("/api/v2/returns/convert_image", {"return_sn": sn} if sn else {},
                                    [(campo, nome, dados, "image/jpeg")], loja=loja)
        except Exception as e:
            ult = str(e)
            if "413" in ult:
                menor = _foto_menor(dados, lado, alvo=len(dados) * 6 // 10)
                if lado >= 500 and len(menor) < len(dados):
                    dados, lado = menor, lado - 200
                    continue          # mesma tentativa com a foto menor
                raise RuntimeError(f"foto grande demais para a Shopee ({len(dados) // 1024} KB): apague e tire de novo")
            if "no such file" in ult or "param" in ult.lower():
                i += 1
                continue
            raise RuntimeError(f"foto: {ult}")
        rr = res.get("response") or {}
        us = ([rr["url"]] if isinstance(rr, dict) and rr.get("url") else []) + _urls_em(rr or res)
        if us:
            _CAMPO_FOTO["nome"] = campo
            return us[0]
        ult = "resposta sem URL: " + json.dumps(res)[:150]
        i += 1
    raise RuntimeError(f"foto nao subiu ({ult[:150]})")


_RX_TEMPORARIO = re.compile(r"internal_server_error|retry later|try again|timed? ?out|http 5\d\d|temporar|busy|Remote end closed|Connection reset", re.I)


def _tenta(etapa, fn, vezes=3):
    """Chama a Shopee; se der erro temporario dela (internal_server_error...), espera e tenta de novo.
    O erro final diz em que etapa parou."""
    import time
    for n in range(vezes):
        try:
            return fn()
        except Exception as e:
            if n < vezes - 1 and _RX_TEMPORARIO.search(str(e)):
                time.sleep(3 * (n + 1))
                continue
            raise RuntimeError(f"{etapa}: {e}"[:220])


FOTOS_MAX_DISPUTA = 3   # a Shopee aceita no maximo 3 fotos na contestacao (as outras vao nas provas extras)


def _motivos_shopee(res):
    """get_return_dispute_reason -> [{id, requisito, modulos:[{module_index, requirement, is_required}]}]"""
    rr = res.get("response") or {}
    out = []
    for x in rr.get("dispute_reason_list") or []:
        rid = None
        for k in ("dispute_reason", "dispute_reason_id", "reason_id", "id"):
            try:
                rid = int(str(x.get(k)).strip())
                break
            except Exception:
                pass
        if rid is None:
            continue
        out.append({"id": rid, "requisito": (x.get("dispute_requirement") or "")[:300],
                    "modulos": [{"module_index": int(m.get("module_index") or 0), "requirement": m.get("requirement") or "",
                                 "is_required": bool(m.get("is_required"))} for m in (x.get("evidence_module_list") or [])]})
    return out


def dev_enviar_shopee(dev_id, texto, email="", motivo_id=None):
    """Envia e, se der qualquer erro, devolve junto o que a Shopee respondeu em cada etapa (Detalhes tecnicos)."""
    diag = {}
    try:
        r = _dev_enviar_core(dev_id, texto, email, motivo_id, diag)
    except Exception as e:
        r = {"ok": False, "erro": "A Shopee recusou: " + str(e)[:220]}
    if not r.get("ok") and diag and not r.get("escolher"):
        r["bruto"] = {**diag, **(r.get("bruto") or {})}
        with _lock, conn() as c:
            c.execute("UPDATE devolucoes SET envio_resp=? WHERE id=?",
                      (json.dumps({"erro": r.get("erro"), "bruto": r["bruto"], "em": agora(), "falhou": True},
                                  ensure_ascii=False, default=str)[:20000], int(dev_id)))
    return r


def _dev_enviar_core(dev_id, texto, email, motivo_id, diag):
    """Contesta a devolucao na Shopee com o texto revisado + fotos + video (so quando a pessoa toca em Enviar).
    So diz 'enviado' depois de conferir na propria Shopee que a contestacao entrou."""
    import time
    o = dev_orientacao(dev_id)
    if not o.get("ok"):
        return o
    if not o.get("shopee"):
        return {"ok": False, "erro": "Esta devolucao ainda nao existe na Shopee (o comprador ainda nao abriu o pedido de devolucao)."}
    texto = (texto or "").strip()
    if len(texto) < 20:
        return {"ok": False, "erro": "texto muito curto"}
    if "[" in texto and "]" in texto:
        return {"ok": False, "erro": "Ajuste o que esta entre [colchetes] no texto antes de enviar."}
    sn = o["shopee"]["return_sn"]
    with conn() as c:
        sr = c.execute("SELECT shop_id FROM shopee_devolucoes WHERE return_sn=?", (sn,)).fetchone()
        d = c.execute("SELECT midias, enviado_em FROM devolucoes WHERE id=?", (int(dev_id),)).fetchone()
        if not email:
            r = c.execute("SELECT valor FROM meta WHERE chave='dev_email'").fetchone()
            email = r[0] if r else ""
    if d["enviado_em"]:
        return {"ok": False, "erro": f"Ja foi enviada em {d['enviado_em'][:16].replace('T', ' ')}."}
    if not email or "@" not in email:
        return {"ok": False, "erro": "Preencha o e-mail da loja (a Shopee exige para a contestação)."}
    midias = json.loads(d["midias"] or "[]")
    ordem = o.get("fotos") or []
    # a Shopee so aceita 3 fotos: vai primeiro o que mais prova (a 1a foto da lista + o comprovante de producao)
    fotos = sorted([m for m in midias if m["tipo"] == "foto"],
                   key=lambda m: (0.5 if m["rotulo"].startswith("Comprovante de produção") else
                                  ordem.index(m["rotulo"]) if m["rotulo"] in ordem else 50))
    videos = [m for m in midias if m["tipo"] == "video"]
    if not fotos:
        return {"ok": False, "erro": "Tire pelo menos as fotos da lista antes de enviar."}
    loja = _shopee_token_ok(sr["shop_id"])
    passos, urls, avisos = [], [], []
    diag["return_sn"] = sn
    try:   # situacao atual da devolucao na Shopee (so leitura, para o diagnostico)
        det0 = _shopee_http("GET", "/api/v2/returns/get_return_detail", loja=loja, params={"return_sn": sn}).get("response") or {}
        diag["situacao_antes"] = {k: det0.get(k) for k in ("status", "return_seller_due_date", "due_date", "validation_type",
                                                          "negotiation", "seller_proof", "dispute_reason", "return_solution")
                                  if k in det0}
    except Exception as e:
        diag["situacao_antes"] = "erro: " + str(e)[:150]
    # 1) motivos que a Shopee aceita para ESTA devolucao (cada um com os tipos de prova que pede)
    res_m = _tenta("motivos da contestação", lambda: _shopee_http(
        "GET", "/api/v2/returns/get_return_dispute_reason", loja=loja, params={"return_sn": sn}))
    diag["motivos_resposta"] = json.dumps(res_m.get("response") or res_m, default=str, ensure_ascii=False)[:4000]
    lista = _motivos_shopee(res_m)
    if not lista:
        import time as _t
        sit = diag.get("situacao_antes") if isinstance(diag.get("situacao_antes"), dict) else {}
        lim = int(sit.get("return_seller_due_date") or 0) or int(sit.get("due_date") or 0)
        st_pt = SHOPEE_DEV_PT.get(sit.get("status") or "", sit.get("status") or "?")
        if lim and lim < _t.time():
            return {"ok": False, "erro": f"⛔ O PRAZO para contestar acabou em {datetime.fromtimestamp(lim, BR).strftime('%d/%m às %H:%M')} "
                                         f"(situação na Shopee: {st_pt}). A Shopee não aceita mais contestação desta devolução pelo sistema. "
                                         "Confira no Seller Center se ainda aparece alguma opção (ex.: pedir compensação)."}
        return {"ok": False, "erro": f"A Shopee não ofereceu nenhum motivo de contestação para esta devolução agora (situação: {st_pt}). "
                                     "Conteste pelo Seller Center, botão Disputar."}
    if motivo_id in (None, ""):
        if len(lista) > 1:   # a pessoa escolhe o motivo pelo texto da propria Shopee
            return {"ok": False, "escolher": lista, "erro": "Escolha o motivo da contestação"}
        motivo = lista[0]
    else:
        motivo = next((m for m in lista if m["id"] == int(motivo_id)), None)
        if not motivo:
            return {"ok": False, "escolher": lista, "erro": "Esse motivo não está mais disponível: escolha de novo"}
    # 2) fotos -> URLs da Shopee
    for m in fotos[:9]:
        with open(os.path.join(_dev_pasta(dev_id), m["arquivo"]), "rb") as f:
            dados_f = f.read()
        urls.append(_tenta("foto", lambda: _dev_converter_foto(loja, m["arquivo"], dados_f, sn)))
    passos.append(f"{len(urls)} foto(s) enviada(s)")
    if videos:
        avisos.append("a Shopee não aceita vídeo pela API: anexe o vídeo pelo Seller Center (na contestação já aberta)")
    # 3) as fotos vao em cada bloco de prova que a Shopee pede (obrigatorios; se nenhum for, no primeiro)
    mods = [x for x in motivo["modulos"] if x["is_required"]] or motivo["modulos"][:1]
    image_list = [{"module_index": x["module_index"], "requirement": x["requirement"], "image_url": urls[:FOTOS_MAX_DISPUTA]} for x in mods]
    corpo = {"return_sn": sn, "email": email, "dispute_reason_id": motivo["id"], "dispute_text_reason": texto[:1000]}
    if image_list:
        corpo["image_list"] = image_list
    rid, rtxt, vids = motivo["id"], "", []
    diag["pedido_enviado"] = {**corpo, "dispute_text_reason": corpo["dispute_text_reason"][:80] + "..."}
    bruto = {"motivos_oferecidos": lista[:20], "motivo_escolhido": rid,
             "blocos_de_prova": [x["requirement"][:80] for x in mods]}
    def _ja_contestou():
        try:
            return _dev_contestou(_shopee_http("GET", "/api/v2/returns/get_return_detail", loja=loja,
                                               params={"return_sn": sn}).get("response") or {})
        except Exception:
            return False
    # variacoes do mesmo pedido (a Shopee nao documenta bem o bloco de fotos): completo; sem o texto do requisito;
    # fotos em todos os blocos de prova. Antes de cada nova tentativa confere se ja entrou (nunca duplica).
    variacoes = [corpo]
    if corpo.get("image_list"):
        variacoes.append({**corpo, "image_list": [{"module_index": x["module_index"], "image_url": x["image_url"]}
                                                  for x in corpo["image_list"]]})
        todos = [{"module_index": x["module_index"], "requirement": x["requirement"], "image_url": urls[:FOTOS_MAX_DISPUTA]}
                 for x in motivo["modulos"]]
        if len(todos) > len(corpo["image_list"]):
            variacoes.append({**corpo, "image_list": todos})
    res_d, erro_d, tent = {}, "", []
    for n, cp in enumerate(variacoes + [corpo]):   # a ultima repete o completo depois de uma pausa
        try:
            res_d = _shopee_http("POST", "/api/v2/returns/dispute", loja=loja, corpo=cp)
            erro_d = ""
            break
        except Exception as e:
            erro_d = str(e)
            tent.append(erro_d[:120])
            time.sleep(3 if n < len(variacoes) - 1 else 6)
            if _ja_contestou():          # deu erro, mas entrou: nao manda de novo
                erro_d = ""
                break
            mx = re.search(r"max size is (\d+)", erro_d)
            if mx and not getattr(cp, "_reduzido", False):   # a Shopee diz quantas fotos aceita: manda so essas
                k = max(1, int(mx.group(1)))
                cp2 = {**cp, "image_list": [{**x, "image_url": x["image_url"][:k]} for x in cp.get("image_list") or []]}
                try:
                    res_d = _shopee_http("POST", "/api/v2/returns/dispute", loja=loja, corpo=cp2)
                    erro_d = ""
                    break
                except Exception as e2:
                    erro_d = str(e2)
                    tent.append(erro_d[:120])
            if not _RX_TEMPORARIO.search(erro_d):
                break
    bruto["tentativas"] = tent
    if erro_d:
        bruto["erro_disputa"] = erro_d[:300]
        with _lock, conn() as c:
            c.execute("UPDATE devolucoes SET envio_resp=? WHERE id=?",
                      (json.dumps({"passos": passos, "avisos": avisos, "bruto": bruto, "em": agora(), "falhou": True},
                                  ensure_ascii=False, default=str), int(dev_id)))
        return {"ok": False, "bruto": bruto,
                "erro": f"Fotos subiram, mas a Shopee recusou ABRIR a contestação ({erro_d[:150]}). "
                        "Tentei de novo automaticamente. Conteste agora pelo Seller Center (botão Disputar) usando o texto (Copiar) e as fotos."}
    bruto["resposta_disputa"] = {k: v for k, v in res_d.items() if k != "request_id"}
    if vids or urls:
        try:
            prova = {"return_sn": sn, "photo": [{"url": u, "thumbnail": u} for u in urls], "description": texto[:500]}
            _shopee_http("POST", "/api/v2/returns/upload_proof", loja=loja, corpo=prova)
            passos.append("provas anexadas")
        except Exception as e:
            avisos.append(f"provas extras não anexadas ({str(e)[:80]})")
    # confere na propria Shopee se a contestacao entrou (nunca dizer "enviado" sem ver la)
    det, st = {}, ""
    for _ in range(4):
        time.sleep(2)
        try:
            det = _shopee_http("GET", "/api/v2/returns/get_return_detail", loja=loja, params={"return_sn": sn}).get("response") or {}
        except Exception as e:
            bruto["erro_conferencia"] = str(e)[:200]
            continue
        st = det.get("status") or ""
        if _dev_contestou(det):
            break
    bruto["status_depois"] = st
    bruto["seller_proof"] = det.get("seller_proof")
    if not _dev_contestou(det):
        with _lock, conn() as c:
            c.execute("UPDATE devolucoes SET envio_resp=? WHERE id=?",
                      (json.dumps({"passos": passos, "avisos": avisos, "bruto": bruto, "em": agora(), "falhou": True}, ensure_ascii=False, default=str), int(dev_id)))
        return {"ok": False, "bruto": bruto,
                "erro": f"A Shopee respondeu, mas a contestação NÃO apareceu na devolução (situação: {st or '?'}). "
                        "Conteste agora pelo Seller Center (botão Copiar texto) para não perder o prazo, e mande um print desta mensagem ao Lucas."}
    passos.append("contestação aberta e conferida na Shopee" + (f" (motivo: {rtxt})" if rtxt else ""))
    resp = {"passos": passos, "avisos": avisos, "em": agora(), "bruto": bruto}
    with _lock, conn() as c:
        if email:
            c.execute("INSERT OR REPLACE INTO meta VALUES('dev_email', ?)", (email[:120],))
        c.execute("UPDATE devolucoes SET enviado_em=?, envio_resp=? WHERE id=?",
                  (agora(), json.dumps(resp, ensure_ascii=False), int(dev_id)))
    return {"ok": True, **resp}


def _itens_devolucao(c, cod):
    """Etiqueta da devolucao (rastreio reverso) ou nº do pedido: acha a etiqueta de envio original."""
    itens = c.execute("SELECT i.* FROM itens i JOIN codigos k ON k.item_id=i.id WHERE k.codigo=? ORDER BY i.id", (cod,)).fetchall()
    if itens:
        return itens
    sr = c.execute("SELECT order_sn FROM shopee_devolucoes WHERE rastreio=? OR order_sn=? LIMIT 1", (cod, cod)).fetchone()
    if sr:
        itens = c.execute("SELECT i.* FROM itens i JOIN codigos k ON k.item_id=i.id WHERE k.codigo=? ORDER BY i.id", (sr[0],)).fetchall()
        for i in itens:
            c.execute("INSERT OR IGNORE INTO codigos VALUES(?,?)", (cod, i["id"]))
    return itens


def historico(iid):
    with conn() as c:
        return [dict(r) for r in c.execute("""SELECT e.etapa, e.em, e.posto, e.alerta, e.desfeito, k.nome
            FROM eventos e LEFT JOIN colaboradores k ON k.id=e.colaborador_id WHERE item_id=? ORDER BY e.id""", (iid,))]


def exportar(de, ate):
    ini, _ = dia_utc(de)
    _, fim = dia_utc(ate)
    out = io.StringIO()
    w = csv.writer(out, delimiter=";")
    w.writerow(["data_hora", "colaborador", "posto", "etapa", "pedido", "etiqueta", "canal", "loja", "sku",
                "nomes", "fonte", "alerta"])
    with conn() as c:
        for r in c.execute("""SELECT e.*, k.nome, i.pedido, i.etiqueta, i.canal, i.loja, i.sku, i.nomes, i.fonte
             FROM eventos e JOIN itens i ON i.id=e.item_id LEFT JOIN colaboradores k ON k.id=e.colaborador_id
             WHERE e.em>=? AND e.em<? AND e.desfeito=0 ORDER BY e.id""", (ini, fim)):
            hora = datetime.fromisoformat(r["em"]).astimezone(BR).strftime("%d/%m/%Y %H:%M:%S")
            w.writerow([hora, r["nome"], r["posto"], r["etapa"], r["pedido"], r["etiqueta"], r["canal"],
                        r["loja"], r["sku"], r["nomes"], r["fonte"], r["alerta"]])
    return "﻿" + out.getvalue()


# ------------------------------------------------------------------ HTTP
def assinatura():
    return hmac.new(SECRET, ADMIN_PASSWORD.encode(), hashlib.sha256).hexdigest()


class H(BaseHTTPRequestHandler):
    server_version = "CentralBoni"

    def log_message(self, *a):
        pass

    def _envia(self, codigo, corpo, tipo="application/json; charset=utf-8", extra=None):
        if isinstance(corpo, (dict, list)):
            corpo = json.dumps(corpo, ensure_ascii=False, default=str)
        b = corpo.encode("utf-8") if isinstance(corpo, str) else corpo
        self.send_response(codigo)
        self.send_header("Content-Type", tipo)
        self.send_header("Content-Length", str(len(b)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(b)

    def _pagina(self, nome):
        caminho = os.path.join(AQUI, "web", nome)
        if not os.path.exists(caminho):
            caminho = os.path.join(AQUI, nome)  # paginas soltas na raiz do repositorio
        with open(caminho, encoding="utf-8") as f:
            self._envia(200, f.read(), "text/html; charset=utf-8")

    def _admin(self):
        ck = self.headers.get("Cookie", "")
        return f"cb_admin={assinatura()}" in ck.replace(" ", "")

    def _json(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}") if n else {}

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        p = u.path
        if p in ("/", "/bipar"):
            return self._pagina("bipar.html")
        if p == "/saude":
            return self._envia(200, {"ok": True})
        if p == "/painel":
            return self._pagina("painel.html" if self._admin() else "login.html")
        if p == "/shopee/retorno" or p.startswith("/shopee/retorno/"):
            vale = p[len("/shopee/retorno/"):] if p.startswith("/shopee/retorno/") else ""
            if not (self._admin() or shopee_convite_ok(vale, usar=bool(q.get("code")))):
                return self._envia(200, "<meta charset=utf-8><h2>Entre no painel da Central neste navegador e autorize de novo.</h2>", "text/html; charset=utf-8")
            try:
                r = shopee_retorno(q.get("code", ""), q.get("shop_id", ""), q.get("main_account_id", ""))
                msg = "✅ Autorizada: " + ", ".join(r.get("lojas") or []) if r.get("ok") else "❌ " + r.get("erro", "")
            except Exception as e:
                msg = "❌ Não autorizou: " + str(e)[:150]
            import html as _h
            return self._envia(200, f"<meta charset=utf-8><meta name=viewport content='width=device-width'><h2 style='font-family:Arial'>{_h.escape(msg)}</h2><p style='font-family:Arial'><a href='/painel'>Voltar ao painel</a></p>", "text/html; charset=utf-8")
        if p == "/upseller":
            return self._pagina("upseller.html" if self._admin() else "login.html")
        if p == "/contagem":
            return self._pagina("contagem.html" if self._admin() else "login.html")
        if p in ("/operacao", "/tv"):
            return self._pagina("operacao.html")
        if p == "/api/operacao":
            if not (self._admin() or hmac.compare_digest(self.headers.get("X-Chave", ""), STATION_KEY)):
                return self._envia(403, {"erro": "sem acesso"})
            return self._envia(200, {**operacao(), "central_ouvindo": time.time() - _todos_visto["t"] < 15})
        if p == "/sair":
            return self._envia(302, "", extra={"Location": "/painel",
                               "Set-Cookie": "cb_admin=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"})
        if p == "/devolucoes":
            return self._pagina("devolucoes.html")
        if p.startswith("/api/devolucoes/"):
            # pagina de devolucoes: entra com a senha do painel OU com a chave da operacao (posto)
            if not (self._admin() or hmac.compare_digest(self.headers.get("X-Chave", "") or q.get("k", ""), STATION_KEY)):
                return self._envia(401, {"erro": "chave da operacao necessaria"})
            if p.startswith("/api/devolucoes/arquivo/"):
                partes = p.split("/")
                if len(partes) == 6 and partes[4].isdigit():
                    cam = os.path.join(DEV_MIDIA, partes[4], os.path.basename(partes[5]))
                    if os.path.isfile(cam):
                        with open(cam, "rb") as f:
                            dados = f.read()
                        tipo = "video/mp4" if cam.lower().endswith((".mp4", ".mov", ".webm")) else "image/jpeg"
                        return self._envia(200, dados, tipo)
                return self._envia(404, {"erro": "arquivo nao encontrado"})
            hj = datetime.now(BR).strftime("%Y-%m-%d")
            if p == "/api/devolucoes/tela":
                return self._envia(200, devolucoes_tela(q.get("de") or hj, q.get("ate") or hj))
            if p == "/api/devolucoes/orientacao":
                return self._envia(200, dev_orientacao(q.get("id") or 0))
            if p == "/api/devolucoes/comprovante":
                jpg = dev_comprovante_jpeg(str(q.get("pedido") or ""))
                if not jpg:
                    return self._envia(404, {"ok": False, "erro": "a Central nao tem registro de producao deste pedido"})
                return self._envia(200, jpg, "image/jpeg")
            if p == "/api/devolucoes/relatorio":
                return self._envia(200, dev_relatorio(q.get("data")))
            if p == "/api/devolucoes/solucoes":
                try:
                    return self._envia(200, dev_solucoes(q.get("sn") or ""))
                except Exception as e:
                    return self._envia(200, {"ok": False, "erro": str(e)[:200]})
            if p == "/api/devolucoes/aprendizado":
                return self._envia(200, {"ok": True, "motivos": dev_aprendizado()})
        if p == "/meus":
            return self._pagina("meus.html")
        if p == "/api/bipes/pessoas":
            if not (self._admin() or hmac.compare_digest(self.headers.get("X-Chave", ""), STATION_KEY)):
                return self._envia(403, {"ok": False, "erro": "chave do posto invalida"})
            with conn() as c:
                return self._envia(200, {"ok": True, "pessoas": [{"nome": r[0], "codigo": r[1]} for r in
                                         c.execute("SELECT nome, codigo FROM colaboradores WHERE ativo=1 ORDER BY nome")]})
        if p == "/api/bipes/meus":
            if not (self._admin() or hmac.compare_digest(self.headers.get("X-Chave", ""), STATION_KEY)):
                return self._envia(403, {"ok": False, "erro": "chave do posto invalida"})
            try:
                desde = int(q["desde"]) if q.get("desde", "") != "" else None
            except ValueError:
                desde = None
            return self._envia(200, meus_bipes(q.get("op") or "", desde, marcar=not q.get("tv")))
        if p == "/api/pausas":
            if not (self._admin() or hmac.compare_digest(self.headers.get("X-Chave", ""), STATION_KEY)):
                return self._envia(403, {"erro": "sem acesso"})
            with conn() as c:
                return self._envia(200, [dict(r) for r in c.execute("SELECT * FROM pausas ORDER BY hora")])
        if not self._admin():
            return self._envia(401, {"erro": "login necessario"})
        hoje = datetime.now(BR).strftime("%Y-%m-%d")
        if p == "/api/emails":
            with conn() as c:
                ult = [dict(r) for r in c.execute("SELECT * FROM emails ORDER BY id DESC LIMIT 30")]
            return self._envia(200, {"status": _email_status, "recebidos": ult})
        if p == "/api/estoque":
            return self._envia(200, estoque())
        if p == "/admin/reiniciar":
            with conn() as c:
                por = {r[0]: r[1] for r in c.execute("SELECT status, COUNT(*) FROM itens GROUP BY status")}
                nt = c.execute("SELECT COUNT(*) FROM itens WHERE falta_material=1").fetchone()[0]
                pode_voltar = c.execute("SELECT COUNT(DISTINCT item_id) FROM eventos WHERE desfeito=2").fetchone()[0]
                ini_h, _ = dia_utc(datetime.now(BR).strftime("%Y-%m-%d"))
                n_hoje = c.execute("""SELECT COUNT(*) FROM itens WHERE (status IN ('SEPARADO','EM_GRAVACAO','GRAVADO','EXPEDIDO') OR falta_material=1)
                                      AND (criado_em>=? OR id IN (SELECT item_id FROM eventos WHERE em>=? AND desfeito=0))""",
                                   (ini_h, ini_h)).fetchone()[0]
            n = sum(por.get(k, 0) for k in ("SEPARADO", "EM_GRAVACAO", "GRAVADO", "EXPEDIDO"))
            h = f"""<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Reiniciar bipagem</title>
<body style="font:18px Arial;max-width:640px;margin:20px auto;padding:0 16px">
<h2>Voltar tudo para AGUARDANDO</h2>
<p>Agora: <b>{por.get('AGUARDANDO',0)}</b> aguardando · <b>{por.get('SEPARADO',0)}</b> separados · <b>{por.get('EM_GRAVACAO',0) + por.get('GRAVADO',0)}</b> em gravação/gravados · <b>{por.get('EXPEDIDO',0)}</b> expedidos · <b>{nt}</b> NÃO TEM · {por.get('DEVOLVIDO',0)} devolvidos</p>
<p>Os <b>{n}</b> separados, em gravação e expedidos voltam para <b>AGUARDANDO</b> e os <b>{nt}</b> NÃO TEM são limpos, para bipar tudo de novo. Só as devoluções ficam como estão.
O estoque não baixa duas vezes. Nada é apagado: dá para desfazer.</p>
<p><button style="font-size:20px;padding:12px 18px;background:#7c3aed;color:#fff;border:0;border-radius:8px" onclick="go(0,1)">Só as etiquetas de HOJE ({n_hoje}) voltam para a SEPARAÇÃO</button></p>
<p><button id=b style="font-size:16px;padding:10px 14px;background:#d7263d;color:#fff;border:0;border-radius:8px" onclick="go(0,0)">TUDO, de todos os dias ({n}), volta para AGUARDANDO</button></p>
{'<p><button style="font-size:16px;padding:10px 14px" onclick="go(1)">Desfazer o último reinício (' + str(pode_voltar) + ' itens)</button></p>' if pode_voltar else ''}
<p id=m></p><p><a href="/painel">Voltar ao painel</a></p>
<script>async function go(d,h){{if(!confirm(d?"Desfazer o reinício e voltar como estava?":h?"Voltar as etiquetas de HOJE para a separação?":"Voltar TUDO, de todos os dias, para AGUARDANDO?"))return;
const r=await fetch("/api/admin/reiniciar-etapas",{{method:"POST",headers:{{"Content-Type":"application/json"}},body:JSON.stringify({{desfazer:!!d,so_hoje:!!h}})}});
const j=await r.json();document.getElementById("m").textContent=j.ok?"Pronto: "+j.itens+" itens. Recarregando...":"Erro";setTimeout(()=>location.reload(),1200)}}</script>"""
            return self._envia(200, h, "text/html; charset=utf-8")
        if p in ("/estoque/folha", "/estoque/contagem.pdf"):
            b = folha_contagem(q.get("cego") == "1", q.get("filtro", "")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return
        if p == "/api/shopee/lojas":
            return self._envia(200, shopee_lojas())
        if p == "/api/shopee/autorizar":
            return self._envia(200, shopee_link_autorizacao())
        if p == "/shopee/autorizar":
            r = shopee_link_autorizacao()
            if not r["ok"]:
                return self._envia(200, f"<meta charset=utf-8><h2 style='font-family:Arial'>{r['erro']}</h2>", "text/html; charset=utf-8")
            return self._envia(302, "", extra={"Location": r["url"]})
        if p == "/api/estoque/cores":
            return self._envia(200, cores_para_conferir())
        if p == "/api/xbz/retiradas":
            return self._envia(200, xbz_retiradas_log())
        if p == "/api/xbz/compras":
            return self._envia(200, xbz_compras_painel())
        if p == "/api/etiquetas/ocultas":
            return self._envia(200, etiquetas_ocultas())
        if p == "/api/etiquetas/limpar":
            return self._envia(200, limpar_etiquetas(aplicar=q.get("aplicar") == "1"))
        if p == "/api/personalizados/corrigidos":
            return self._envia(200, personalizados_corrigidos())
        if p == "/personalizados/corrigidos":
            from html import escape as _e
            d = personalizados_corrigidos()
            linhas = "".join(f"<tr><td>{_e(str(x.get('em',''))[:16].replace('T',' '))}</td><td>{_e(str(x['pedido']))}</td><td>{_e(str(x.get('loja') or ''))}</td>"
                             f"<td>{_e(str(x.get('sku') or ''))} {_e(str(x.get('cor') or ''))}</td><td>{x.get('etiqueta') or ''}</td>"
                             f"<td>{_e(str(x.get('status') or ''))}</td><td>{_e(x['motivo'])}</td></tr>" for x in d["itens"])
            return self._envia(200, "<meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>"
                               "<style>body{font:15px Arial;margin:12px}table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #ddd;padding:5px;text-align:left}</style>"
                               f"<h2>Etiquetas corrigidas para PERSONALIZADO ({d['total']})</h2>"
                               "<p>Estavam como sem personalização, mas a etiqueta/pedido mostra que são personalizadas. Agora passam pela gravação.</p>"
                               f"<table><tr><th>Quando</th><th>Pedido</th><th>Loja</th><th>SKU/cor</th><th>Etq</th><th>Etapa (na hora)</th><th>Prova</th></tr>{linhas}</table>",
                               "text/html; charset=utf-8")
        if p == "/api/shopee/pedidos":
            return self._envia(200, shopee_pedidos_resumo())
        if p == "/shopee/pedidos":
            return self._pagina("shopee.html")
        if p == "/shopee/anuncios":
            return self._pagina("anuncios.html")
        if p == "/api/anuncios/status":
            with conn() as c:
                r = c.execute("SELECT valor FROM meta WHERE chave='anuncios_ultimo'").fetchone()
                f = c.execute("SELECT valor FROM meta WHERE chave='fotos_ultimo'").fetchone()
            return self._envia(200, {"auto": anuncios_auto(), "status": _anuncios_status, "intervalo_min": ANUNCIOS_INTERVALO // 60,
                                     "ultimo": json.loads(r[0]) if r else None, "fotos_ultimo": json.loads(f[0]) if f else None})
        if p == "/api/anuncios/simular":
            try:
                return self._envia(200, anuncios_estoque(aplicar=False))
            except Exception as e:
                return self._envia(200, {"ok": False, "erro": str(e)[:200]})
        if p == "/api/anuncios/fotos":
            try:
                return self._envia(200, fotos_aplicar(aplicar=False))
            except Exception as e:
                return self._envia(200, {"ok": False, "erro": str(e)[:200]})
        if p == "/api/shopee/testar":
            try:
                return self._envia(200, shopee_testar(q.get("shop_id", "0")))
            except Exception as e:
                return self._envia(200, {"ok": False, "erro": str(e)[:200]})
        if p == "/api/prateleiras":
            with conn() as c:
                return self._envia(200, [dict(r) for r in c.execute("SELECT * FROM prateleiras ORDER BY prateleira, sku")])
        if p == "/api/compra/sugestao":
            return self._envia(200, sugestao_compra(int(q["dias"]) if (q.get("dias") or "").isdigit() else None))
        if p == "/api/estoque/garantir-baixas":
            _conhecidos_cache["v"] = None
            return self._envia(200, {"linha_certa": corrigir_baixas_erradas(), "separadas": garantir_baixas(q.get("desde"))})
        if p == "/api/previsao":
            pv = previsao(int(q["dias"]) if (q.get("dias") or "").isdigit() else None)
            sku = estoque_chave(q.get("sku") or "")[0]
            itens = {f"{k[0]}|{k[1]}": v for k, v in pv.get("itens", {}).items() if not sku or k[0] == sku}
            return self._envia(200, {**{k: v for k, v in pv.items() if k != "itens"}, "itens": itens})
        if p == "/api/previsao/teste":
            return self._envia(200, previsao_teste())
        if p == "/api/previsao/ler-historico":
            threading.Thread(target=shopee_historico_vendas, daemon=True).start()
            return self._envia(200, {"ok": True, "msg": "lendo o historico da Shopee em segundo plano"})
        if p == "/api/estoque/zerar-faltas":
            return self._envia(200, zerar_faltas_bipadas(q.get("desde")))
        if p == "/api/estoque/corrigir-baixas":
            _conhecidos_cache["v"] = None
            return self._envia(200, corrigir_baixas_erradas())
        if p == "/api/estoque/movimentos":
            with conn() as c:
                return self._envia(200, [dict(r) for r in c.execute(
                    "SELECT * FROM estoque_mov WHERE sku=? ORDER BY id DESC LIMIT 200", (estoque_chave(q.get("sku", ""))[0],))])
        if p == "/api/materiais":
            return self._envia(200, materiais(q.get("de") or hoje, q.get("ate") or hoje))
        if p == "/api/compras":
            return self._envia(200, compras(q.get("de") or hoje[:8] + "01", q.get("ate") or hoje))
        if p == "/api/painel":
            return self._envia(200, painel(q.get("data") or hoje))
        if p == "/api/equipe/sugestao":
            return self._envia(200, sugestao_metas())
        if p == "/api/equipe":
            return self._envia(200, equipe_dia(q.get("data") or hoje))
        if p == "/api/eventos":
            with conn() as c:
                desde = int(q.get("desde") if q.get("desde") not in (None, "") else -1)
                if desde < 0:
                    r = c.execute("SELECT COALESCE(MAX(id),0) FROM eventos").fetchone()[0]
                    return self._envia(200, {"ultimo": r, "eventos": []})
                ev = [dict(r) for r in c.execute("""SELECT e.id, e.etapa, e.posto, e.em, e.alerta, e.desfeito, k.nome,
                      i.pedido, i.sku, i.nomes FROM eventos e JOIN itens i ON i.id=e.item_id
                      LEFT JOIN colaboradores k ON k.id=e.colaborador_id WHERE e.id>? ORDER BY e.id LIMIT 50""", (desde,))]
                return self._envia(200, {"ultimo": ev[-1]["id"] if ev else desde, "eventos": ev})
        if p == "/api/produtividade":
            return self._envia(200, produtividade(q.get("de") or hoje, q.get("ate") or hoje))
        if p == "/api/devolucoes":
            return self._envia(200, devolucoes(q.get("de") or hoje, q.get("ate") or hoje))
        if p == "/api/custos":
            with conn() as c:
                return self._envia(200, [dict(r) for r in c.execute("SELECT * FROM custos ORDER BY sku")])
        if p == "/api/historico":
            return self._envia(200, historico(int(q.get("id", 0))))
        if p == "/api/colaboradores":
            with conn() as c:
                lst = [dict(r) for r in c.execute("SELECT * FROM colaboradores WHERE COALESCE(excluido,0)=0 ORDER BY ativo DESC, nome")]
            for x in lst:
                if x.get("voz") is None:
                    x["voz"] = _voz_de(x["codigo"])
            return self._envia(200, lst)
        if p == "/exportar.csv":
            return self._envia(200, exportar(q.get("de") or hoje, q.get("ate") or hoje), "text/csv; charset=utf-8",
                               {"Content-Disposition": f"attachment; filename=bipagem_{q.get('de') or hoje}.csv"})
        if p == "/backup.db":
            tmp = DB + ".bak"
            if os.path.exists(tmp):
                os.remove(tmp)
            with conn() as c:
                c.execute("VACUUM INTO ?", (tmp,))
            with open(tmp, "rb") as f:
                return self._envia(200, f.read(), "application/octet-stream",
                                   {"Content-Disposition": f"attachment; filename=central_{hoje}.db"})
        if p == "/crachas":
            return self._pagina("crachas.html")
        if p == "/setores":
            return self._pagina("setores.html")
        self._envia(404, {"erro": "nao encontrado"})

    def do_POST(self):
        p = urlparse(self.path).path
        if p == "/api/devolucoes/midia":   # foto/video do celular (binario, nao JSON)
            q = {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}
            if not (self._admin() or hmac.compare_digest(self.headers.get("X-Chave", ""), STATION_KEY)):
                return self._envia(401, {"erro": "chave da operacao necessaria"})
            n = int(self.headers.get("Content-Length") or 0)
            if n <= 0 or n > DEV_MIDIA_MAX:
                return self._envia(413, {"ok": False, "erro": "arquivo vazio ou grande demais (max 200 MB)"})
            dados = self.rfile.read(n)
            return self._envia(200, dev_midia_salvar(q.get("id") or 0, q.get("tipo"), q.get("rotulo"), dados, q.get("ext")))
        try:
            d = self._json()
        except Exception:
            return self._envia(400, {"erro": "json invalido"})
        if p == "/login":
            if hmac.compare_digest(str(d.get("senha", "")), ADMIN_PASSWORD):
                return self._envia(200, {"ok": True}, extra={
                    "Set-Cookie": f"cb_admin={assinatura()}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000"})
            return self._envia(403, {"erro": "senha incorreta"})
        if p == "/api/bipes/setor":
            # o leitor trocou de setor (etiqueta de SETOR): so avisa o computador central, nao muda nada na operacao
            if not hmac.compare_digest(self.headers.get("X-Chave", ""), STATION_KEY):
                return self._envia(403, {"ok": False})
            posto = str(d.get("posto") or "").upper()[:20]
            opr = norm(d.get("operador"))
            with conn() as c:
                col = c.execute("SELECT nome FROM colaboradores WHERE codigo=? AND ativo=1", (opr,)).fetchone() if opr else None
            r = {"tipo": "operador", "msg": "Setor: " + posto, "evento": "operador", "colaborador": col[0] if col else "",
                 "fazer": None if col else "Bipe o CRACHA", "voz": _voz_de(opr) if col else None}
            _feed_add(posto, "CMD" + posto, opr, d.get("leitor") or "", r, quem=opr, origem=d.get("origem") or "")
            return self._envia(200, {"ok": True})
        if p == "/api/bipe":
            if not hmac.compare_digest(self.headers.get("X-Chave", ""), STATION_KEY):
                return self._envia(403, {"tipo": "erro", "msg": "Chave do posto invalida."})
            return self._envia(200, bipar(d.get("posto"), d.get("codigo"), d.get("operador"), d.get("modo"), d.get("leitor") or "",
                                             d.get("origem") or ""))
        if p.startswith("/api/devolucoes/"):
            # pagina de devolucoes: senha do painel OU chave da operacao (posto)
            if not (self._admin() or hmac.compare_digest(self.headers.get("X-Chave", ""), STATION_KEY)):
                return self._envia(401, {"erro": "chave da operacao necessaria"})
            if p == "/api/devolucoes/decidir":
                return self._envia(200, dev_decidir(d.get("id"), d.get("destino"), d.get("motivo") or "", d.get("obs") or "",
                                                    str(d.get("sku") or "").strip().upper(), str(d.get("cor") or "").strip().upper(),
                                                    d.get("qtd")))
            if p == "/api/devolucoes/registrar":
                return self._envia(200, dev_registrar(str(d.get("codigo") or "")))
            if p == "/api/devolucoes/apagar-midia":
                return self._envia(200, dev_midia_apagar(d.get("id") or 0, str(d.get("arquivo") or "")))
            if p == "/api/devolucoes/oferecer":
                try:
                    return self._envia(200, dev_oferecer(d.get("sn") or "", d.get("solucao"), d.get("valor")))
                except Exception as e:
                    return self._envia(200, {"ok": False, "erro": str(e)[:200]})
            if p == "/api/devolucoes/comprovante":
                return self._envia(200, dev_comprovante_anexar(d.get("id") or 0))
            if p == "/api/devolucoes/ligar":
                return self._envia(200, dev_ligar_shopee(d.get("id") or 0, str(d.get("ref") or "")))
            if p == "/api/devolucoes/email":
                em = str(d.get("email") or "").strip()[:120]
                with _lock, conn() as c:
                    c.execute("INSERT OR REPLACE INTO meta VALUES('dev_email', ?)", (em,))
                return self._envia(200, {"ok": True, "email": em})
            if p == "/api/devolucoes/enviar":
                try:
                    return self._envia(200, dev_enviar_shopee(d.get("id") or 0, d.get("texto") or "", str(d.get("email") or "").strip(),
                                                                    d.get("motivo_id")))
                except Exception as e:
                    return self._envia(200, {"ok": False, "erro": "A Shopee recusou: " + str(e)[:200]})
            if p == "/api/devolucoes/shopee":
                try:
                    return self._envia(200, shopee_devolucoes_sincronizar())
                except Exception as e:
                    return self._envia(200, {"ok": False, "erro": str(e)[:200]})
            return self._envia(404, {"erro": "nao encontrado"})
        if p == "/api/importar-pdf":
            if not (self._admin() or hmac.compare_digest(self.headers.get("X-Token", ""), API_TOKEN)):
                return self._envia(403, {"ok": False, "erro": "token invalido"})
            import base64
            try:
                itens, ign = itens_do_pdf(base64.b64decode(d.get("dados") or ""), str(d.get("nome") or ""))
            except Exception as e:
                return self._envia(200, {"ok": False, "erro": f"nao consegui ler o PDF ({e})"})
            r = importar_lote({"lote": "PDF " + str(d.get("nome") or "")[:60], "itens": itens}) if itens else {"novos": 0, "atualizados": 0}
            return self._envia(200, {"ok": True, "etiquetas": len(itens), "ignoradas": ign, "novos": r["novos"], "atualizados": r["atualizados"]})
        if p == "/api/upseller/espelho":
            if not (self._admin() or hmac.compare_digest(self.headers.get("X-Token", ""), API_TOKEN)):
                return self._envia(403, {"ok": False, "erro": "entre no painel da Central neste navegador"})
            try:
                return self._envia(200, upseller_espelho(d.get("pedidos") or [], bool(d.get("aplicar")), bool(d.get("forcar"))))
            except Exception as e:
                return self._envia(200, {"ok": False, "erro": str(e)[:200]})
        if p == "/api/lotes":
            if not (self._admin() or hmac.compare_digest(self.headers.get("X-Token", ""), API_TOKEN)):
                return self._envia(403, {"erro": "token invalido"})
            return self._envia(200, importar_lote(d))
        if not self._admin():
            return self._envia(401, {"erro": "login necessario"})
        if p == "/api/compra/confirmar":
            return self._envia(200, confirmar_pedido_xbz(d.get("itens") or []))
        if p == "/api/estoque/contagem":
            return self._envia(200, contagem_estoque(str(d.get("texto") or "").splitlines()))
        if p == "/api/prateleiras":
            return self._envia(200, salvar_prateleiras(d.get("texto", "")))
        if p == "/api/estoque/status":
            return self._envia(200, marcar_sku(d.get("sku", ""), d.get("cor", ""), d.get("status", "")))
        if p == "/api/admin/reprocessar-emails":
            try:
                return self._envia(200, reprocessar_emails(int(d.get("dias") or 2)))
            except Exception as e:
                return self._envia(200, {"ok": False, "erro": str(e)[:200]})
        if p == "/api/shopee/sincronizar":
            if not self._admin():
                return self._envia(401, {"erro": "login necessario"})
            return self._envia(200, {"ok": True, "lojas": shopee_sincronizar_todas()})
        if p == "/api/anuncios/aplicar":
            if _anuncios_status.get("rodando"):
                return self._envia(200, {"ok": False, "erro": "ja esta atualizando, aguarde"})
            def _rodar():
                _anuncios_status["rodando"] = True
                try:
                    r = anuncios_estoque(aplicar=True)
                    _anuncios_status["erro"] = "" if r.get("ok") else r.get("erro", "")
                except Exception as e:
                    _anuncios_status["erro"] = str(e)[:200]
                _anuncios_status["rodando"] = False
            threading.Thread(target=_rodar, daemon=True).start()
            return self._envia(200, {"ok": True, "msg": "atualizando os anuncios..."})
        if p == "/api/anuncios/auto":
            with _lock, conn() as c:
                c.execute("INSERT OR REPLACE INTO meta VALUES('anuncios_auto', ?)", ("1" if d.get("ligado") else "0",))
            return self._envia(200, {"ok": True, "auto": bool(d.get("ligado"))})
        if p == "/api/anuncios/fotos/aplicar":
            try:
                return self._envia(200, fotos_aplicar(aplicar=True))
            except Exception as e:
                return self._envia(200, {"ok": False, "erro": str(e)[:200]})
        if p == "/api/anuncios/fotos/desfazer":
            try:
                return self._envia(200, fotos_desfazer())
            except Exception as e:
                return self._envia(200, {"ok": False, "erro": str(e)[:200]})
        if p == "/api/estoque/cor":
            return self._envia(200, decidir_cor(d.get("sku"), d.get("cor"), d.get("acao"), d.get("para")))
        if p == "/api/xbz/compras/sincronizar":
            try:
                return self._envia(200, xbz_compras_sincronizar(aplicar=not d.get("simular")))
            except Exception as e:
                return self._envia(200, {"ok": False, "erro": str(e)[:200]})
        if p == "/api/xbz/compras/pendencia":
            return self._envia(200, xbz_resolver_pendencia(d.get("id") or 0, str(d.get("acao") or ""), d.get("sku"), d.get("cor")))
        if p == "/api/xbz/retiradas/ler":
            try:
                return self._envia(200, xbz_retiradas())
            except Exception as e:
                return self._envia(200, {"ok": False, "erro": re.sub(r"(passwd|user)=[^&\s]+", r"\1=***", str(e))[:200]})
        if p == "/api/etiquetas/reabrir":
            return self._envia(200, reabrir_etiqueta(d.get("id") or 0))
        if p == "/api/admin/reiniciar-etapas":
            return self._envia(200, reiniciar_etapas(bool(d.get("desfazer")), bool(d.get("so_hoje"))))
        if p == "/api/estoque/desfazer":
            return self._envia(200, desfazer_contagem(d.get("ref")))
        if p == "/api/estoque/baixa":
            return self._envia(200, baixa_manual(d.get("sku"), d.get("cor"), d.get("qtd"), d.get("motivo")))
        if p == "/api/estoque/movimento":
            return self._envia(200, movimento_manual(str(d.get("texto") or "").splitlines()))
        if p == "/api/estoque/distribuir":
            return self._envia(200, distribuir_cor(d.get("sku", ""), d.get("partes") or {}))
        if p == "/api/xbz/sincronizar":
            threading.Thread(target=xbz_sincronizar, daemon=True).start()
            return self._envia(200, {"ok": True, "msg": "atualizando em segundo plano"})
        if p == "/api/compras/importar":
            import base64
            res = []
            for a in d.get("arquivos") or []:
                try:
                    res.append({"nome": a.get("nome"), **importar_nfe(base64.b64decode(a.get("dados") or ""))})
                except Exception as e:
                    res.append({"nome": a.get("nome"), "ok": False, "erro": str(e)[:200]})
            return self._envia(200, {"ok": True, "resultados": res})
        if p == "/api/custos":
            with _lock, conn() as c:
                if d.get("excluir"):
                    c.execute("DELETE FROM custos WHERE sku=?", (d["sku"],))
                    return self._envia(200, {"ok": True})
                linhas = []
                if d.get("texto"):  # colado da planilha/XBZ: "SKU;custo" ou "SKU<tab>descricao<tab>custo"
                    for ln in d["texto"].splitlines():
                        cols = [x.strip() for x in re.split(r"\t|;", ln) if x.strip()]
                        if len(cols) < 2:
                            continue
                        try:
                            linhas.append((cols[0], " ".join(cols[1:-1]), _num(cols[-1])))
                        except ValueError:
                            continue  # cabecalho ou linha sem preco
                else:
                    try:
                        linhas.append((d["sku"], d.get("descricao", ""), _num(d["custo"])))
                    except (KeyError, ValueError):
                        return self._envia(400, {"erro": "custo invalido"})
                for sku, desc, custo in linhas:
                    if sku.strip():
                        c.execute("""INSERT INTO custos VALUES(?,?,?,?) ON CONFLICT(sku) DO UPDATE SET
                                     descricao=COALESCE(NULLIF(excluded.descricao,''),custos.descricao),
                                     custo=excluded.custo, atualizado_em=excluded.atualizado_em""",
                                  (sku.strip().upper(), desc, custo, agora()))
            return self._envia(200, {"ok": True, "importados": len(linhas)})
        if p == "/api/itens/excluir":
            ids = [int(x) for x in d.get("ids", [])]
            with _lock, conn() as c:
                c.executemany("DELETE FROM itens WHERE id=?", [(i,) for i in ids])
                c.executemany("DELETE FROM estoque_mov WHERE ref LIKE ?", [(f"ETQ|{i}|%",) for i in ids])
            return self._envia(200, {"ok": True, "excluidos": len(ids)})
        if p == "/api/itens/adicionar":
            if not str(d.get("pedido", "")).strip():
                return self._envia(400, {"erro": "informe o pedido"})
            with conn() as c:
                for cod in {norm(d.get("pedido")), norm(d.get("rastreio"))} - {""}:
                    if c.execute("SELECT 1 FROM codigos WHERE codigo=?", (cod,)).fetchone():
                        return self._envia(200, {"ok": False, "erro": "ja cadastrada"})
            it = {k: d.get(k, "") for k in ("pedido", "rastreio", "canal", "envio", "loja", "sku", "cor", "fonte", "obs")}
            if str(it["canal"]).upper() in ("", "OUTROS"):
                it["canal"], it["envio"] = canal_por_codigo(it["pedido"], it["rastreio"]) or ("", "")
            it["nomes"] = [n.strip() for n in str(d.get("nomes", "")).split("|") if n.strip()]
            it["personalizado"] = bool(d.get("personalizado", True))
            it["seq"] = "M" + agora()
            return self._envia(200, importar_lote({"lote": "MANUAL", "itens": [it]}))
        if p == "/api/itens/editar":
            with _lock, conn() as c:
                c.execute("UPDATE itens SET sku=?, atualizado_em=? WHERE id=?",
                          (str(d.get("sku", "")).strip().upper(), agora(), int(d["id"])))
            return self._envia(200, {"ok": True})
        if p == "/api/equipe/metas":
            return self._envia(200, salvar_metas(d))
        if p == "/api/pausas":
            with _lock, conn() as c:
                dur = max(1, min(240, int(d.get("duracao") or 15)))
                if d.get("excluir"):
                    c.execute("DELETE FROM pausas WHERE id=?", (d["id"],))
                elif d.get("id"):
                    c.execute("UPDATE pausas SET duracao=? WHERE id=?", (dur, d["id"]))
                elif re.fullmatch(r"\d{1,2}:\d{2}", d.get("hora", "")):
                    h, m = d["hora"].split(":")
                    c.execute("INSERT INTO pausas(nome,hora,pessoas,duracao) VALUES(?,?,?,?)",
                              (d.get("nome") or "Pausa", f"{int(h):02d}:{m}", d.get("pessoas", ""), dur))
                else:
                    return self._envia(400, {"erro": "hora invalida (use 09:15)"})
            return self._envia(200, {"ok": True})
        if p == "/api/colaboradores":
            with _lock, conn() as c:
                if d.get("excluir"):
                    # sai da lista e o cracha para de funcionar; o historico continua com o nome
                    c.execute("UPDATE colaboradores SET excluido=1, ativo=0 WHERE id=?", (d["id"],))
                elif d.get("id"):
                    c.execute("UPDATE colaboradores SET nome=?, funcao=?, ativo=? WHERE id=?",
                              (d["nome"], d.get("funcao", ""), 1 if d.get("ativo", True) else 0, d["id"]))
                else:
                    codigo = "OP" + secrets.token_hex(3).upper()
                    c.execute("INSERT INTO colaboradores(nome,funcao,codigo) VALUES(?,?,?)",
                              (d["nome"], d.get("funcao", ""), codigo))
            return self._envia(200, {"ok": True})
        self._envia(404, {"erro": "nao encontrado"})


# ------------------------------------------------------------------ compras XBZ (NF-e) e materiais
ESTOQUE_IGNORAR = [x.strip().upper() for x in os.environ.get("ESTOQUE_IGNORAR_CONTAS", "LAURA").split(",") if x.strip()]
ESTOQUE_DIAS_COMPRA = int(os.environ.get("ESTOQUE_DIAS_COMPRA", "6"))


def codigo_xbz_nf(cprod):
    """Codigo da nota da XBZ -> codigo do produto (o mesmo SKU das etiquetas). Ex.: I*4083 -> 04083, I*18781 -> 18781."""
    c = re.sub(r"^[A-Z]\*", "", (cprod or "").upper().strip())
    return c.zfill(5) if c.isdigit() and len(c) < 5 else c


def cor_norm(cor):
    import unicodedata
    t = unicodedata.normalize("NFKD", str(cor or "")).encode("ascii", "ignore").decode().upper()
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", t)).strip()


def sku_base(sku):
    """SKU das etiquetas -> codigo do produto XBZ (ex.: '18726I' fica 18726I; '05063-PRETO' vira 05063)."""
    return re.split(r"[-/ ]", (sku or "").upper().strip())[0]


def _controlado(c, sku, cn):
    """Produto/cor ja contado? (so entao o sistema pode dizer sozinho que 'nao tem')."""
    if c.execute("SELECT 1 FROM estoque_mov WHERE sku=? AND cor=? AND tipo='CONTAGEM' LIMIT 1", (sku, cn)).fetchone():
        return "cor"
    if c.execute("SELECT 1 FROM estoque_mov WHERE sku=? AND cor='' AND tipo='CONTAGEM' LIMIT 1", (sku,)).fetchone():
        return "sku"
    return None


# ------------------------------------------------------------------ conhecimento do agente de estoque (handoff 02/10/2026)
ESTOQUE_ABERTURA = "2026-10-02"   # data-base do snapshot: o que foi impresso antes ja esta descontado nele
# contagem geral feita em 05/10 a tarde: tudo separado a partir de 06/10 sai do estoque, impresso quando for
ESTOQUE_CONTAGEM_GERAL = os.environ.get("ESTOQUE_CONTAGEM_GERAL", "2026-10-06T03:00:00+00:00")
ESTOQUE_SNAPSHOT = """## 01093
Transparente 130
Cinza -60
Preto 400
Verde 280
Vermelho 200
Laranja 130
Azul 360
Roxo/Lilás 20
Fumê 100
## 01228P
Preto 2
Rosa 4
## 01622B
Rosa 4
Azul 3
Branco 2
Salmão -1
## 01895
Azul 19
Cinza 10
Preto 25
Roxo/Lilás 19
Verde 12
Vermelho 9
Laranja 1
## 02090
Azul 3
Verde 1
## 02121
Rosa -23
Roxo/Lilás 78
Verde -25
## 03493
Padrão 8
## 03590
Azul Claro 6
Azul Escuro 6
Roxo/Lilás 5
Branco 8
Laranja 4
Vermelho 0
Cinza 4
Verde 3
Rosa 5
## 03968
Branco 11
Azul Claro 8
Azul Escuro -1
Turquesa 15
Rosa Claro 12
Pink 9
Vermelho 11
## 04205
Branco 30
Azul 29
Preto 29
Creme 26
Verde 20
Inox 29
Rosa 59
## 05063
Prata 5
Preto 1
Laranja 3
Verde 2
Inox 1
Azul 1
Vermelho -2
## 05932
Branco 3
Azul 2
Cinza 1
Preto 1
Verde 2
## 06010P
Preto 37
Verde 20
Azul Escuro 14
Azul Claro 5
Roxo/Lilás 14
Vermelho 22
## 06011G
Preto 19
Verde 15
Azul Escuro 14
Azul Claro 24
Roxo/Lilás 14
Vermelho 18
## 06012
Verde 31
Cinza/Cinza Escuro 28
Branco 16
Azul 52
Rosa -4
## 06016B
Rosa Claro 20
Roxo/Lilás 39
Laranja 100
Preto 40
## 06078
Azul 0
Prata -9
Verde 8
Vermelho -5
## 07030
Preto 12
Verde 8
Laranja 8
Azul 8
Rosa 4
## 07392
Inox 130
## 07447
Padrão 31
## 08280
Preto 2
Azul Claro 3
Roxo/Lilás 2
## 09181
Cinza 2
Verde 22
Rosa 2
Azul Claro 13
Roxo/Lilás 6
Branco e Rosa -5
## 09183
Vermelho 2
Preto 1
## 09185
Dourado 10
Preto 2
Rosa Escuro 11
Prata 8
Verde 5
Vermelho 8
## 09188
Azul 129
Preto 14
Vermelho 14
Verde 31
Branco 38
Prata 17
## 09198
Branco 7
Preto 6
## 09227
Verde 3
Azul 23
Roxo/Lilás 11
Vermelho 22
## 09256
Verde 10
Azul 11
Roxo/Lilás -3
Cinza -1
## 09260
Preto 3
Laranja 4
Vermelho 4
Branco 1
Verde 1
Rosa 2
## 09306
Preto 2
## 09824
Azul 5079
Champagne 540
Dourado 1449
Laranja 600
Preto 2640
Prata 1100
Rosa 1199
Roxo/Lilás 550
Verde 2550
Vermelho 1400
## 14011
Padrão 3
## 18549
Azul -1
Branco 5
Rosa 9
Verde 9
## 18552
Azul 9
Branco 8
Cinza 25
Inox 18
Laranja -2
Prata 13
Preto 13
Verde 7
Vermelho 20
## 18601
Azul 13
Azul Escuro 3
Cinza 13
Preto -3
Verde 8
Rosa 7
Laranja 6
Vermelho 6
## 18622
Azul 10
Inox -6
Preto 14
Verde 17
Rosa 16
## 18623
Branco 11
Inox 8
Preto 7
Azul 8
Cobre 13
## 18637
Chumbo 8
Azul 9
Branco 12
Roxo/Lilás 15
Rosa 20
## 18647P
Rosa Claro 5
Roxo/Lilás 8
Azul 2
Verde 10
Preto 8
## 18647M
Verde 10
Preto 6
## 18647G
Preto 8
Roxo/Lilás 5
Azul 10
Rosa Claro 2
## 18650
Lilás/Roxo 8
Preto 2
Rosa Claro 13
Rosa Escuro 3
Azul -4
## 18677
Preto 1
Azul 9
## 18678
Branco 5
Azul -3
Bronze 7
## 18699
Inox -26
Lilás/Roxo 4
Preto 43
Vermelho 12
## 18726I
Preto 12
Azul Escuro 5
Cinza 19
Verde Escuro 29
Rosa Claro 19
Lilás/Roxo 8
Verde Claro 14
Branco 19
Vermelho 7
## 18781
Cinza 150
Preto 150
Rosa Escuro 401
Verde 6
## 18786
Azul 14
Bege 14
Rosa -11
Verde 13
## 18904
Inox 13
Grafite 26
Laranja 17
Preto 12
Vermelho 18
Branco 4
## 18910
Inox 4
## 18921
Azul 37
Lilás/Roxo 2
Branco 23
Rosa Claro 47
Rosa Escuro 30
Verde/Verde Água 24
## 18949P
Azul 47
Azul Claro -13
Cinza 20
Creme -3
Preto 16
Rosa 27
Verde 11
Branco 26
Avariada -1
## 18949M
Azul 11
Creme 16
Preto 18
Rosa 5
Roxo/Lilás 1
Verde -4
Verde Claro 11
Verde Escuro 6
Branco 5
Laranja 3
Vermelho 8
## 18975
Azul 15
Branco 13
Preto 1
Rosa 14
Roxo/Lilás 13
Verde 12
## 19005
Creme 6
Rosa Escuro 2
Inox 3
Verde 7
Preto 2
Vermelho 4
Azul 3
## 19006
Vermelho 3
Creme 5
Azul Claro 5
Rosa 2
Verde 2
Preto 2
## 19062
Avariada 0
Bege 11
Preto 23
Rosa 9
Verde -3
Azul 8
Inox 6
Cinza Claro 23
Cinza Escuro 15
Vermelho 12
## 19065
Vermelho 1
Rosa 4
Laranja 9
## 19124
Padrão 1
## 19181
Preto -1
## 9139A
Rosa 18
Branco 11
Marrom 4
Verde 16
Vermelho 2
Lilás/Roxo 11
Azul Claro 15
Azul Escuro 8
Preto 7
## FY001
Branco Transparente/Transparente 90
Branco Leitoso 90
Azul Leitoso 320
Azul Transparente -50
Laranja Transparente 80
Vermelho Transparente 330
Vermelho Leitoso 50
Fumê 150
Verde Transparente 70
Verde Leitoso -10
Sortidos -70
"""

# SKU escrito diferente -> SKU certo (confirmados)
SKU_ALIAS = {"18726": "18726I", "09139A": "9139A", "9188": "09188", "9185": "09185", "9227": "09227",
             "018781": "18781", "018921": "18921", "018552": "18552", "01622": "01622B", "1622": "01622B",
             "06016": "06016B", "3493": "03493", "1949": "18949P", "1899": "18949M", "9256": "09256",
             "9181": "09181", "9183": "09183", "9198": "09198", "9260": "09260", "9306": "09306", "9824": "09824"}
COR_ABREV = {"PTO": "PRETO", "BCO": "BRANCO", "VM": "VERMELHO", "VD": "VERDE", "AZU": "AZUL", "CZ": "CINZA"}
COR_ALIAS = {"LILAS": "ROXO", "ROXO LILAS": "ROXO", "LILAS ROXO": "ROXO", "VERDE VERDE AGUA": "VERDE AGUA",
             "CINZA CINZA ESCURO": "CINZA", "BRANCO TRANSPARENTE TRANSPARENTE": "TRANSPARENTE",
             "BRANCO TRANSPARENTE": "TRANSPARENTE", "PADRAO": "", "UNICA": "", "UNICO": ""}
# correcoes de cor por produto (confirmadas)
COR_POR_SKU = {("18726I", "VEDRE"): "VERDE ESCURO", ("19062", "MARROM"): "BEGE", ("9139A", "TURQUESA"): "VERDE",
               ("09181", "ROSA CLARO"): "ROSA", ("18921", "AZUL CLARO"): "AZUL", ("18781", "ROSA"): "ROSA ESCURO",
               ("06012", "CINZA ESCURO"): "CINZA"}
# fornecedor sem estoque / pouca demanda: nao sugerir compra automatica (ate nova evidencia)
XBZ_INDISPONIVEL = {("02121", ""), ("18975", "PRETO"), ("06012", "ROSA"), ("09185", "BRANCO"), ("09256", "CINZA"),
                    ("18678", "AZUL")}
BAIXA_DEMANDA = {("09260", "BRANCO"), ("09260", "VERDE"), ("03493", "")}
ESTRATEGICOS = {"09824"}
# precos XBZ conhecidos (so valem se a API da XBZ nao trouxer o preco)
PRECOS_REF = {"01622B": 9.50, "01895": 45.50, "03590": 12.08, "03968": 16.90, "05063": 36.75, "06012": 18.90,
              "06016B": 2.70, "06078": 8.20, "07030": 13.90, "07392": 6.80, "09181": 27.00, "09188": 10.90,
              "09256": 22.90, "09824": 0.83, "18552": 15.80, "18601": 10.90, "18623": 18.00, "18647": 14.50,
              "18699": 20.90, "18726I": 14.50, "18781": 11.50, "18786": 11.50, "18904": 15.90, "18910": 34.80,
              "18921": 12.20, "18949P": 21.90, "18949M": 31.20, "19062": 15.90, "19124": 37.90, "9139A": 9.45,
              "FY001": 1.00, "18650": 9.50, "01093": 0.60, "18549": 17.60}
# pedidos ja descontados no snapshot (nunca baixar de novo)
UPPUS_PROCESSADOS = set("""UPPUS209361 UPPUS209459 UPPUS209644 UPPUS209680 UPPUS209720 UPPUS209781 UPPUS209845 UPPUS209853
UPPUS209855 UPPUS209856 UPPUS209857 UPPUS209873 UPPUS209894 UPPUS209933 UPPUS210002 UPPUS210003 UPPUS210048 UPPUS210100
UPPUS210111 UPPUS210278 UPPUS210302 UPPUS210331 UPPUS210391 UPPUS210405 UPPUS210443 UPPUS210452 UPPUS210468 UPPUS210469
UPPUS210493 UPPUS210502 UPPUS210525 UPPUS210528 UPPUS210541 UPPUS210551 UPPUS210556 UPPUS210562 UPPUS210563 UPPUS210566
UPPUS210575 UPPUS210581 UPPUS210585 UPPUS210589 UPPUS210592 UPPUS210594 UPPUS210602 UPPUS210611 UPPUS210613 UPPUS210614
UPPUS210615 UPPUS210620 UPPUS210621 UPPUS210622 UPPUS210625 UPPUS210628 UPPUS210639 UPPUS210644 UPPUS210651 UPPUS210652
UPPUS210663 UPPUS210666 UPPUS210676 UPPUS210678 UPPUS210679 UPPUS210681 UPPUS210701 UPPUS210703 UPPUS210709 UPPUS210717
UPPUS210718 UPPUS210720 UPPUS210724 UPPUS210726 UPPUS210727 UPPUS210730 UPPUS210731 UPPUS210732 UPPUS210733 UPPUS210738
UPPUS210739 UPPUS210745 UPPUS210764 UPPUS210765 UPPUS210772 UPPUS210773 UPPUS210774 UPPUS210776 UPPUS210780 UPPUS210781
UPPUS210782 UPPUS210783 UPPUS210786 UPPUS210788 UPPUS210789 UPPUS210790 UPPUS210791 UPPUS210792 UPPUS210793 UPPUS210794
UPPUS210795 UPPUS210799 UPPUS210801 UPPUS210802 UPPUS210803 UPPUS210804 UPPUS210806 UPPUS210809 UPPUS210810 UPPUS210811
UPPUS210812 UPPUS210813 UPPUS210814 UPPUS210815 UPPUS210817 UPPUS210818 UPPUS210819 UPPUS210821 UPPUS210822 UPPUS210823
UPPUS210824
UPPUS210839 UPPUS210840 UPPUS210841""".split())  # + etiquetas impressas em 01/10 19:03 (PDF de teste ja importado)


def estoque_chave(sku, cor="", texto=""):
    """SKU e cor como o estoque conhece (aliases, abreviacoes e correcoes confirmadas)."""
    s_ = sku_base(sku)
    s_ = SKU_ALIAS.get(s_, s_)
    t = (texto or "").upper()
    if s_ == "18949":
        if re.search(r"350\s*ML", t):
            s_ = "18949P"
        elif re.search(r"550\s*ML", t):
            s_ = "18949M"
    cn = " ".join(COR_ABREV.get(w, w) for w in cor_norm(cor).split())
    cn = COR_ALIAS.get(cn, cn)
    if s_ == "18691" and cn == "VERMELHO":
        s_ = "18601"
    cn = COR_POR_SKU.get((s_, cn), cn)
    cn = _COR_ALIAS_DB.get((s_, cn), cn)   # "e a mesma cor" que voce confirmou no painel
    return s_, cn


_COR_ALIAS_DB = {}


def _carregar_cor_alias():
    try:
        with conn() as c:
            _COR_ALIAS_DB.clear()
            _COR_ALIAS_DB.update({(r[0], r[1]): r[2] for r in c.execute("SELECT sku, de, para FROM cor_alias")})
    except Exception:
        pass


def _excluido(c, sku, cor):
    """Cor (ou produto inteiro) que voce excluiu: nao entra de novo em nada (retirada, compra, lista)."""
    return c.execute("SELECT 1 FROM sku_status WHERE status='EXCLUIDO' AND sku=? AND (cor=? OR cor='')",
                     (sku, cor or "")).fetchone() is not None


# palavras que costumam ser a mesma cor escrita de outro jeito
_COR_GRUPOS = [{"ROSA", "PINK", "ROSE", "SALMAO", "CORAL"}, {"ROXO", "LILAS", "VIOLETA", "LAVANDA"}, {"CINZA", "CHUMBO", "GRAFITE", "FUME", "FUMACA"},
               {"BRANCO", "OFF", "GELO"}, {"PRATA", "INOX", "PRATEADO"}, {"MARROM", "CAFE", "CHOCOLATE"},
               {"BEGE", "CREME", "NUDE", "AREIA"}, {"DOURADO", "OURO"}, {"VERMELHO", "VINHO", "BORDO"},
               {"AZUL", "MARINHO"}, {"VERDE", "MILITAR", "AGUA"}]


def _cores_parecidas(a, b):
    if not a or not b or a == b:
        return False
    wa, wb = a.split(), b.split()
    if wa[0] == wb[0]:                       # ROSA x ROSA CLARO (AZUL CLARO x AZUL ESCURO sao cores diferentes)
        return len(wa) == 1 or len(wb) == 1
    return any(wa[0] in g and wb[0] in g for g in _COR_GRUPOS)


def _skus_parecidos(sku):
    """01622B, 1622B, 01622, 1622: o mesmo produto escrito de jeitos diferentes (catalogo XBZ x etiqueta)."""
    b = sku_base(sku)
    v = {sku, b, b.lstrip("0"), "0" + b.lstrip("0"), re.sub(r"[A-Z]+$", "", b), re.sub(r"[A-Z]+$", "", b).lstrip("0")}
    return {x for x in v if x}


def cores_para_conferir():
    """Cores que eu nao reconheco ou que parecem a mesma (para voce decidir: juntar, manter separadas, arquivar ou excluir).
    As opcoes de 'e a mesma que' trazem as cores do nosso estoque E as variacoes da XBZ (ex.: SALMAO, FUME)."""
    lim = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    with conn() as c:
        linhas = c.execute("""SELECT sku, cor, SUM(qtd) saldo, MAX(em) ult, GROUP_CONCAT(DISTINCT tipo) tipos FROM estoque_mov
                              WHERE cor<>'' GROUP BY sku, cor HAVING MAX(em)>=? OR SUM(qtd)<>0""", (lim,)).fetchall()
        saldo_de = {(r[0], r[1]): r[2] for r in c.execute("SELECT sku, cor, SUM(qtd) FROM estoque_mov WHERE cor<>'' GROUP BY sku, cor")}
        ok = {(r[0], r[1], r[2]) for r in c.execute("SELECT sku, cor, outra FROM cor_ok")}
        cat = {}
        for r in c.execute("SELECT codigo, cor FROM xbz WHERE cor<>''"):
            cat.setdefault(r[0], set()).add(cor_norm(r[1]))
        exc = {(r[0], r[1]) for r in c.execute("SELECT sku, cor FROM sku_status WHERE status IN ('EXCLUIDO','ARQUIVADO')")}
        juntou = {}
        for r in c.execute("SELECT sku, de, para FROM cor_alias"):
            juntou.setdefault((r[0], r[2]), []).append(r[1])
        fora = [dict(r) for r in c.execute("""SELECT s.sku, s.cor, s.status, s.em, COALESCE((SELECT SUM(qtd) FROM estoque_mov m
                    WHERE m.sku=s.sku AND m.cor=s.cor), 0) saldo FROM sku_status s WHERE s.status IN ('EXCLUIDO','ARQUIVADO')
                    ORDER BY s.em DESC LIMIT 300""")]
    por = {}
    for r in linhas:
        if (r["sku"], r["cor"]) in exc or (r["sku"], "") in exc:
            continue
        por.setdefault(r["sku"], []).append(dict(r))
    out = []
    for sku, L in por.items():
        cores = [x["cor"] for x in L]
        cat_sku = set()
        for k in _skus_parecidos(sku):
            cat_sku |= cat.get(k, set())
        for x in L:
            motivos, parecidas = [], []
            if cat_sku and x["cor"] not in cat_sku and not any(x["cor"].startswith(k) or k.startswith(x["cor"]) for k in cat_sku):
                motivos.append("a XBZ nao tem essa cor para esse produto")
            if len(x["cor"]) <= 3 or re.search(r"\d|/", x["cor"]):
                motivos.append("parece codigo, nao nome de cor")
            for y in sorted(set(cores) | cat_sku):
                if y != x["cor"] and _cores_parecidas(x["cor"], y) and (sku, x["cor"], y) not in ok and (sku, y, x["cor"]) not in ok:
                    parecidas.append(y)
            if parecidas:
                motivos.append("parecida com " + ", ".join(parecidas))
            if motivos and (sku, x["cor"], "*") not in ok:
                # sugestao: a parecida que a XBZ usa (o nome certo e o do fornecedor); senao a parecida do nosso estoque
                sug = next((p_ for p_ in parecidas if p_ in cat_sku), "")
                if not sug and parecidas and x["cor"] not in cat_sku:
                    sug = parecidas[0]
                opcoes = [{"cor": o, "xbz": o in cat_sku, "nosso": o in cores, "saldo": round(saldo_de.get((sku, o), 0) or 0)}
                          for o in sorted((set(cores) | cat_sku) - {x["cor"]}, key=lambda o: (o not in cat_sku, o))]
                out.append({"sku": sku, "cor": x["cor"], "saldo": round(x["saldo"] or 0), "motivos": motivos,
                            "parecidas": parecidas, "sugestao": sug, "opcoes": opcoes, "nome_xbz": x["cor"] in cat_sku,
                            "cores_xbz": sorted(cat_sku), "ja_juntou": sorted(juntou.get((sku, x["cor"]), []))})
    out.sort(key=lambda d: (d["sku"], d["cor"]))
    todas = sorted({cor_norm(v) for v in XBZ_COR_COD.values()} | {k for v in cat.values() for k in v} - {""})
    return {"ok": True, "itens": out, "fora": fora, "cores_xbz_todas": todas}


def decidir_cor(sku, cor, acao, para=""):
    """acao: JUNTAR (cor e a mesma que 'para': o saldo vai para la e daqui pra frente entra la),
    OUTRA (sao cores diferentes: nao pergunta de novo), EXCLUIR (nao trabalha mais: nunca mais entra)."""
    sku, cor, para = (sku or "").upper().strip(), (cor or "").upper().strip(), (para or "").upper().strip()
    acao = (acao or "").upper()
    if not sku or not cor:
        return {"ok": False, "erro": "dados invalidos"}
    if acao == "EXCLUIR":
        return marcar_sku(sku, cor, "EXCLUIDO")
    if acao == "ARQUIVAR":
        return marcar_sku(sku, cor, "ARQUIVADO")
    if acao == "INCLUIR":   # volta a valer (estava excluida/arquivada); o saldo que ela tinha continua
        return marcar_sku(sku, cor, "")
    para = cor_norm(para)   # "Fumê" = FUME, "salmão" = SALMAO (igual ao resto do estoque)
    if para:
        p2 = estoque_chave(sku, para)[1]
        para = p2 if p2 and p2 != cor else para
    with _lock, conn() as c:
        if acao == "OUTRA":
            outras = [para] if para else ["*"]
            for o in outras:
                c.execute("INSERT OR REPLACE INTO cor_ok VALUES(?,?,?,?)", (sku, cor, o, agora()))
            return {"ok": True}
        if acao == "JUNTAR" and para == cor:   # escolheu a propria cor: ela esta certa (nao pergunta mais)
            c.execute("INSERT OR REPLACE INTO cor_ok VALUES(?,?,?,?)", (sku, cor, "*", agora()))
            return {"ok": True, "confirmada": True}
        if acao != "JUNTAR" or not para:
            return {"ok": False, "erro": "escolha a cor certa"}
        c.execute("INSERT OR REPLACE INTO cor_alias VALUES(?,?,?,?)", (sku, cor, para, agora()))
        c.execute("UPDATE cor_alias SET para=? WHERE sku=? AND para=?", (para, sku, cor))   # sem corrente A->B->C
        n = c.execute("UPDATE estoque_mov SET cor=? WHERE sku=? AND cor=?", (para, sku, cor)).rowcount
        c.execute("DELETE FROM sku_status WHERE sku=? AND cor=?", (sku, cor))
    _carregar_cor_alias()
    return {"ok": True, "movidos": n}


def _ja_no_snapshot(c, item):
    """Etiqueta impressa antes da data-base, ou pedido ja processado pelo agente antigo: ja esta no snapshot."""
    if (item.get("impresso") or "9999") < ESTOQUE_ABERTURA:
        return True
    cods = [r[0] for r in c.execute("SELECT codigo FROM codigos WHERE item_id=?", (item["id"],))]
    return any(x in UPPUS_PROCESSADOS for x in cods)


def carregar_abertura():
    """Saldo inicial (snapshot autoritativo do agente antigo), uma vez so."""
    with conn() as c:
        if c.execute("SELECT 1 FROM meta WHERE chave='abertura_estoque'").fetchone():
            return 0
    linhas, sku = [], None
    for ln in ESTOQUE_SNAPSHOT.splitlines():
        ln = ln.strip()
        if ln.startswith("##"):
            sku = ln[2:].strip()
        elif ln and sku:
            m = re.match(r"(.+?)\s+(-?\d+)$", ln)
            if m:
                linhas.append(f"{sku};{m.group(1)};{m.group(2)}")
    r = contagem_estoque(linhas, tipo_obs="saldo inicial (agente de estoque, 02/10/2026)")
    with _lock, conn() as c:
        c.execute("INSERT OR REPLACE INTO meta VALUES('abertura_estoque', ?)", (agora(),))
    return r["feitos"]


# ---- a baixa tem que cair na linha CERTA do estoque (senao "nao da baixa": o produto contado nao diminui)
# A etiqueta as vezes vem com SKU/cor escritos diferente do estoque: "18949" + "ROSA 550ML", "9188", "018552",
# "10X2356", "LARANJA 100 UN" ou sem a cor. Aqui traduz para a linha que existe no estoque (contada/XBZ).
_conhecidos_cache = {"em": 0, "v": None}


def time_now():
    import time as _t
    return _t.time()


def _estoque_conhecido(c):
    import time as _t
    if _conhecidos_cache["v"] is not None and _t.time() - _conhecidos_cache["em"] < 300:
        return _conhecidos_cache["v"]
    cores = {}
    for sku, cor in c.execute("""SELECT DISTINCT sku, cor FROM estoque_mov
                                 WHERE tipo IN ('CONTAGEM','DISTRIBUI','ENTRADA_NF','ENTRADA_XBZ','AJUSTE')"""):
        cores.setdefault(sku, set()).add(cor or "")
    for cod, cor in c.execute("SELECT codigo, cor FROM xbz WHERE codigo<>''"):
        k = estoque_chave(cod)[0]
        cores.setdefault(k, set()).add(cor_norm(cor or ""))
    _conhecidos_cache.update(v=cores, em=_t.time())
    return cores


_RX_QTD_COR = re.compile(r"\(?\b\d+\s*(?:ML|UN|UND|UNID|UNIDADES|X)\b\.?\)?|\bKIT\b|^\s*\d+\s*", re.I)


def _peca_no_estoque(c, item, sku, cor, _de_novo=True):
    """(sku, cor) da linha do estoque para esta peca."""
    conhecidos = _estoque_conhecido(c)
    if _de_novo and _conhecidos_cache["em"] and (time_now() - _conhecidos_cache["em"]) > 5:
        k0 = estoque_chave(sku, cor)
        if k0[1] not in conhecidos.get(k0[0], set()):   # produto/cor novo (contado agora ha pouco?): rele a lista
            _conhecidos_cache["v"] = None
            conhecidos = _estoque_conhecido(c)
    texto = f"{cor or ''} {item.get('sku') or ''} {item.get('obs') or ''}"
    s0 = _sku_do_texto(sku, conhecidos, texto) or sku
    sku2, cor2 = estoque_chave(s0, cor, texto)
    if sku2 not in conhecidos:
        for v in sorted(_skus_parecidos(sku2)):
            v2 = estoque_chave(v, "", texto)[0]
            if v2 in conhecidos:
                sku2 = v2
                break
    cs = conhecidos.get(sku2, set())
    limpa = cor_norm(_RX_QTD_COR.sub(" ", cor or ""))
    if cor2 not in cs and limpa:
        c3 = estoque_chave(sku2, limpa)[1]
        cor2 = c3 if c3 in cs else (_cor_do_modelo(sku2, [limpa], {sku2: cs}) or cor2)
    if not cor2 and cs - {""}:   # etiqueta sem cor: tenta a variacao do pedido na Shopee
        sp = c.execute("SELECT itens FROM shopee_pedidos WHERE order_sn=?", (norm(item.get("pedido")),)).fetchone()
        if sp:
            try:
                vs = [f"{x.get('var', '')}" for x in json.loads(sp[0] or "[]") if sku_base(x.get("sku") or "") in _skus_parecidos(sku2)
                      or not x.get("sku")]
            except Exception:
                vs = []
            achou = _cor_do_modelo(sku2, vs, {sku2: cs}) if vs else None
            if achou:
                cor2 = achou
    return sku2, cor2


def baixar_estoque(c, iid):
    """Baixa do estoque as pecas da etiqueta quando o material sai para a separacao (uma vez so por etiqueta)."""
    r = c.execute("SELECT * FROM itens WHERE id=?", (iid,)).fetchone()
    if not r or r["lote"] == "DEVOLUCAO" or (agora() < ESTOQUE_CONTAGEM_GERAL and _ja_no_snapshot(c, dict(r))):
        return
    for sku, cor, qtd in _pecas_do_item(dict(r)):
        if not sku or sku.startswith("("):
            continue
        sku, cn = _peca_no_estoque(c, dict(r), sku, cor)
        if not sku or sku.startswith("("):
            continue
        c.execute("INSERT OR IGNORE INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                  (agora(), sku, cn, -qtd, "ETIQUETA", f"ETQ|{iid}|{sku}|{cn}", f"pedido {r['pedido']}"))


def corrigir_baixas_erradas():
    """Baixas de etiqueta que cairam numa linha que o estoque nao conhece (SKU/cor escritos diferente) vao para a
    linha certa. So mexe quando acha a linha certa com seguranca. Pode rodar quantas vezes quiser."""
    feitos = []
    with conn() as c:
        conhecidos = _estoque_conhecido(c)
        movs = c.execute("""SELECT m.id, m.sku, m.cor, m.ref FROM estoque_mov m WHERE m.tipo='ETIQUETA'""").fetchall()
        for m in movs:
            if m["cor"] in conhecidos.get(m["sku"], set()) and (m["cor"] or not (conhecidos.get(m["sku"], set()) - {""})):
                continue
            partes = (m["ref"] or "").split("|")
            if len(partes) < 2 or not partes[1].isdigit():
                continue
            it = c.execute("SELECT * FROM itens WHERE id=?", (int(partes[1]),)).fetchone()
            if not it:
                continue
            sku2, cor2 = _peca_no_estoque(c, dict(it), m["sku"], m["cor"])
            if (sku2, cor2) != (m["sku"], m["cor"]) and cor2 in conhecidos.get(sku2, set()) and (cor2 or not (conhecidos.get(sku2, set()) - {""})):
                cont = c.execute("SELECT MAX(em) FROM estoque_mov WHERE tipo='CONTAGEM' AND sku=? AND cor=?", (sku2, cor2)).fetchone()[0]
                em = c.execute("SELECT em FROM estoque_mov WHERE id=?", (m["id"],)).fetchone()[0]
                feitos.append((m["id"], sku2, cor2, m["sku"], m["cor"], bool(cont and em and cont > em)))
    if feitos:
        with _lock, conn() as c:
            for mid, sku2, cor2, s0, c0, contada in feitos:
                if contada:   # a contagem feita depois ja inclui esta saida: nao desconta de novo
                    c.execute("UPDATE estoque_mov SET sku=?, cor=?, obs=COALESCE(obs,'')||' (era '||?||' '||?||', '||qtd||'; ja estava na contagem)', qtd=0 WHERE id=?",
                              (sku2, cor2, s0, c0 or "sem cor", mid))
                else:
                    c.execute("UPDATE estoque_mov SET sku=?, cor=?, obs=COALESCE(obs,'')||? WHERE id=?",
                              (sku2, cor2, f" (era {s0} {c0 or 'sem cor'})", mid))
        print(f"Estoque: {len(feitos)} baixa(s) de etiqueta foram para a linha certa", flush=True)
    return {"ok": True, "corrigidas": len(feitos), "exemplos": [f"{f[3]} {f[4] or 'sem cor'} -> {f[1]} {f[2] or 'PADRAO'}" for f in feitos[:30]]}


def completar_auto_sem_sku():
    """Etiquetas que entraram no 1o bipe sem SKU (nao estavam na Central) e por isso NAO deram baixa:
    quando o pedido aparece na Shopee ou no espelho do UpSeller, completa SKU/cor/personalizado. Depois a baixa sai
    pelo garantir_baixas (com a hora da separacao)."""
    feitos = 0
    with _lock, conn() as c:
        for it in c.execute("SELECT * FROM itens WHERE lote='AUTO' AND COALESCE(sku,'')=''").fetchall():
            cods = [r[0] for r in c.execute("SELECT codigo FROM codigos WHERE item_id=?", (it["id"],))] + [norm(it["pedido"])]
            pecas, sem, pers, loja = [], [], None, ""
            for k in cods:
                sp = c.execute("SELECT * FROM shopee_pedidos WHERE order_sn=?", (k,)).fetchone()
                if sp:
                    loja = sp["loja"] or ""
                    for i in json.loads(sp["itens"] or "[]"):
                        cr = (i.get("var") or "").split(",")[0].strip()
                        pecas.append({"sku": estoque_chave(i.get("sku") or "")[0] or (i.get("sku") or ""), "cor": cr, "qtd": int(i.get("qtd") or 1)})
                        t = f"{i.get('sku') or ''} {i.get('var') or ''}"
                        sem.append(bool(re.search(r"SEM\s+PERSONALIZ", t, re.I)))
                        pers = 1 if _texto_personalizado(t) else pers
                    break
                up = c.execute("SELECT * FROM upseller_pedidos WHERE codigo=?", (k,)).fetchone()
                if up and up["itens"]:
                    loja = up["loja"] or ""
                    for x in json.loads(up["itens"] or "[]"):
                        s_, cr, p_, q, _t = _ups_linha(x)
                        pecas.append({"sku": s_, "cor": cr, "qtd": q})
                        sem.append(p_ is False)
                        pers = 1 if p_ else pers
                    if up["pedido"]:
                        c.execute("INSERT OR IGNORE INTO codigos VALUES(?,?)", (norm(up["pedido"]), it["id"]))
                    break
            pecas = [p for p in pecas if p["sku"]]
            if not pecas:
                continue
            if pers is None:
                pers = 0 if sem and all(sem) else 1
            sku = " + ".join(dict.fromkeys(p["sku"] for p in pecas))
            cor = " + ".join(dict.fromkeys(p["cor"] for p in pecas if p["cor"]))
            c.execute("""UPDATE itens SET sku=?, cor=?, pecas=?, qtd=?, loja=COALESCE(NULLIF(loja,''),?),
                         personalizado=CASE WHEN status IN ('EXPEDIDO','DEVOLVIDO') THEN personalizado ELSE ? END,
                         obs=REPLACE(COALESCE(obs,''),' - completar SKU no painel',' (SKU completado sozinho)'), atualizado_em=? WHERE id=?""",
                      (sku, cor, json.dumps(pecas, ensure_ascii=False), sum(p["qtd"] for p in pecas), loja, pers, agora(), it["id"]))
            feitos += 1
    if feitos:
        _op_cache["v"] = None
        print(f"{feitos} etiqueta(s) incluida(s) no bipe ganharam SKU (vao dar baixa)", flush=True)
    return feitos


def garantir_baixas(desde=None):
    """Toda etiqueta separada (ou gravada/expedida sem passar pela separacao) desde a contagem geral tem que ter
    saido do estoque. Da a baixa que faltou, com a hora da separacao. Nao desconta de novo o que uma contagem feita
    depois ja pegou. Pode rodar quantas vezes quiser (cada etiqueta baixa uma vez so)."""
    desde = desde or ESTOQUE_CONTAGEM_GERAL
    try:
        completados = completar_auto_sem_sku()
    except Exception as e:
        print("completar auto:", e, flush=True)
        completados = 0
    with conn() as c:
        cand = c.execute("""SELECT e.item_id, MIN(e.em) quando FROM eventos e JOIN itens i ON i.id=e.item_id
                            WHERE e.desfeito=0 AND e.etapa IN ('SEPARADO','GRAVACAO_INICIO','GRAVACAO_FIM','EXPEDIDO')
                            AND COALESCE(i.lote,'')<>'DEVOLUCAO' AND i.status<>'DEVOLVIDO'
                            GROUP BY e.item_id HAVING MIN(e.em)>=?""", (desde,)).fetchall()
        faltam = [(r[0], r[1]) for r in cand if not _ja_baixado(c, r[0])]
    feitos, pulados, sem_sku = [], 0, 0
    with _lock, conn() as c:
        for iid, quando in faltam:
            if _ja_baixado(c, iid):
                continue
            it = c.execute("SELECT * FROM itens WHERE id=?", (iid,)).fetchone()
            if not it:
                continue
            for sku, cor, qtd in _pecas_do_item(dict(it)):
                if not sku or sku.startswith("("):
                    sem_sku += 1
                    continue
                sku2, cn = _peca_no_estoque(c, dict(it), sku, cor)
                if not sku2 or sku2.startswith("("):
                    sem_sku += 1
                    continue
                cont = c.execute("SELECT MAX(em) FROM estoque_mov WHERE tipo='CONTAGEM' AND sku=? AND cor=?", (sku2, cn)).fetchone()[0]
                ref = f"ETQ|{iid}|{sku2}|{cn}"
                if cont and cont > quando:   # contado depois de separar: a contagem ja tirou; so marca (qtd 0)
                    c.execute("INSERT OR IGNORE INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                              (quando, sku2, cn, 0, "ETIQUETA", ref, f"pedido {it['pedido']} (ja estava na contagem)"))
                    pulados += 1
                    continue
                cur = c.execute("INSERT OR IGNORE INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                                (quando, sku2, cn, -qtd, "ETIQUETA", ref, f"pedido {it['pedido']} (baixa da separacao que faltou)"))
                if cur.rowcount:
                    feitos.append((sku2, cn or "PADRAO", qtd))
    por = {}
    for sku, cor, q in feitos:
        por[f"{sku} {cor}"] = por.get(f"{sku} {cor}", 0) + q
    if feitos:
        print(f"Estoque: {len(feitos)} baixa(s) de separacao que faltavam foram feitas", flush=True)
    with conn() as c:
        sem_lista = [dict(r) for r in c.execute("""SELECT pedido, status, em FROM (SELECT i.pedido, i.status, MIN(e.em) em FROM itens i
                     JOIN eventos e ON e.item_id=i.id AND e.desfeito=0 WHERE i.lote='AUTO' AND COALESCE(i.sku,'')='' AND e.em>=?
                     GROUP BY i.id) ORDER BY em DESC LIMIT 300""", (desde,))]
    return {"ok": True, "desde": desde, "completadas": completados, "sem_sku_lista": sem_lista,
            "etiquetas_sem_baixa": len(faltam), "baixas_feitas": len(feitos),
            "pecas": sum(q for _, _, q in feitos), "ja_na_contagem": pulados, "sem_sku": sem_sku,
            "por_produto": sorted(por.items(), key=lambda x: -x[1])[:60]}


def _ja_baixado(c, iid):
    """A etiqueta ja deu baixa no estoque (o material ja saiu da prateleira)."""
    return c.execute("SELECT 1 FROM estoque_mov WHERE ref LIKE ? LIMIT 1", (f"ETQ|{iid}|%",)).fetchone() is not None


def reiniciar_etapas(desfazer=False, so_hoje=False):
    """Volta TUDO que esta separado / em gravacao / expedido para AGUARDANDO, para refazer a bipagem do zero.
    Nao apaga nada: os bipes ficam marcados (desfeito=2) e da para desfazer o reinicio.
    O estoque nao baixa de novo: a etiqueta que ja deu baixa nao baixa outra vez nem conta como reservada.
    Devolucoes ficam como estao; NAO TEM tambem e limpo."""
    with _lock, conn() as c:
        if desfazer:
            ids = [r[0] for r in c.execute("SELECT DISTINCT item_id FROM eventos WHERE desfeito=2")]
            c.execute("UPDATE eventos SET desfeito=0 WHERE desfeito=2")
        else:
            c.execute("UPDATE eventos SET desfeito=3 WHERE desfeito=2")   # reinicio antigo nao volta junto no "desfazer"
            q = "SELECT id FROM itens WHERE (status IN ('SEPARADO','EM_GRAVACAO','GRAVADO','EXPEDIDO') OR falta_material=1)"
            args = ()
            if so_hoje:   # so as etiquetas do dia (criadas hoje ou bipadas hoje)
                ini, _ = dia_utc(datetime.now(BR).strftime("%Y-%m-%d"))
                q += " AND (criado_em>=? OR id IN (SELECT item_id FROM eventos WHERE em>=? AND desfeito=0))"
                args = (ini, ini)
            ids = [r[0] for r in c.execute(q, args)]
            for iid in ids:
                c.execute("""UPDATE eventos SET desfeito=2 WHERE item_id=? AND desfeito=0
                             AND etapa IN ('SEPARADO','GRAVACAO_INICIO','GRAVACAO_FIM','EXPEDIDO','FALTA_MATERIAL')""", (iid,))
        for iid in ids:
            recalcular(c, iid)
        por = {r[0]: r[1] for r in c.execute("SELECT status, COUNT(*) FROM itens GROUP BY status")}
    return {"ok": True, "itens": len(ids), "agora": por}


def _zerar_por_falta(c, item, quando, quem=""):
    """Bipou 'NAO TEM' (falta de material): aquele produto/cor nao esta na prateleira -> o estoque fica ZERO
    (lancado como contagem 0, com hora e quem bipou). So mexe quando a etiqueta e de um produto/cor so."""
    chaves = set()
    for sku, cor, _q in _pecas_do_item(item):
        if not sku or sku.startswith("("):
            continue
        k = _peca_no_estoque(c, item, sku, cor)
        if k[0] and not k[0].startswith("("):
            chaves.add(k)
    if len(chaves) != 1:
        return ""
    sku, cn = chaves.pop()
    saldo = c.execute("SELECT COALESCE(SUM(qtd),0) FROM estoque_mov WHERE sku=? AND cor=?", (sku, cn)).fetchone()[0]
    if saldo == 0:
        return f"{sku} {cn or 'PADRAO'}"
    c.execute("INSERT OR IGNORE INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
              (quando, sku, cn, -saldo, "CONTAGEM", f"FALTA|{item['id']}|{sku}|{cn}|{quando}",
               f"contagem: 0 (NAO TEM bipado{' por ' + quem if quem else ''}: prateleira vazia; estava {saldo:g})"))
    return f"{sku} {cn or 'PADRAO'} (estava {saldo:g})"


def zerar_faltas_bipadas(desde=None):
    """'NAO TEM' bipados (por uma pessoa) desde a contagem geral que ainda nao zeraram o estoque: zera agora,
    se depois do bipe nao entrou material nem houve contagem. Pode rodar quantas vezes quiser."""
    desde = desde or ESTOQUE_CONTAGEM_GERAL
    feitos = []
    with _lock, conn() as c:
        evs = c.execute("""SELECT e.item_id, MAX(e.em) em, k.nome FROM eventos e LEFT JOIN colaboradores k ON k.id=e.colaborador_id
                           WHERE e.etapa='FALTA_MATERIAL' AND e.desfeito=0 AND e.colaborador_id IS NOT NULL
                           AND COALESCE(e.posto,'')<>'ESTOQUE' AND e.em>=? GROUP BY e.item_id""", (desde,)).fetchall()
        ult = {}
        for e in evs:
            it = c.execute("SELECT * FROM itens WHERE id=?", (e["item_id"],)).fetchone()
            if not it:
                continue
            chaves = {_peca_no_estoque(c, dict(it), s_, c_) for s_, c_, _q in _pecas_do_item(dict(it)) if s_ and not s_.startswith("(")}
            if len(chaves) != 1:
                continue
            k = chaves.pop()
            if k not in ult or e["em"] > ult[k][0]:
                ult[k] = (e["em"], dict(it), e["nome"] or "")
        for (sku, cn), (em, it, quem) in ult.items():
            depois = c.execute("""SELECT 1 FROM estoque_mov WHERE sku=? AND cor=? AND em>=? AND
                                  tipo IN ('CONTAGEM','ENTRADA_NF','ENTRADA_XBZ','DISTRIBUI','AJUSTE') LIMIT 1""", (sku, cn, em)).fetchone()
            if depois:
                continue
            z = _zerar_por_falta(c, it, agora(), quem)
            if z and "estava" in z:
                feitos.append(z)
    if feitos:
        print(f"Estoque zerado por NAO TEM bipado: {', '.join(feitos[:20])}", flush=True)
    return {"ok": True, "zerados": feitos}


def _ids_cancelados(c):
    """Etiquetas de pedidos cancelados na Shopee: nao vao sair da prateleira, entao nao ficam reservadas."""
    try:
        return {r[0] for r in c.execute("""SELECT k.item_id FROM codigos k JOIN shopee_pedidos s ON s.order_sn=k.codigo
                                           WHERE s.status='CANCELLED'""")}
    except Exception:
        return set()


def _reservado(c, sku, cn, nivel, antes_de):
    """Unidades de etiquetas que entraram antes desta e ainda nao foram separadas (vao sair da prateleira)."""
    tot = 0
    canc = _ids_cancelados(c)
    for i in c.execute("SELECT * FROM itens WHERE status='AGUARDANDO' AND id<? AND COALESCE(lote,'')<>'DEVOLUCAO' AND COALESCE(oculto,0)=0", (antes_de,)):
        if i["id"] in canc or _ja_no_snapshot(c, dict(i)) or _ja_baixado(c, i["id"]):
            continue
        for s_, c_, q in _pecas_do_item(dict(i)):
            k = estoque_chave(s_, c_)
            if k[0] == sku and (nivel == "sku" or k[1] == cn):
                tot += q
    return tot


def checar_falta(c, iid):
    """Etiqueta nova: se o que tem na prateleira (menos o que ja esta reservado) nao cobre, marca NAO TEM sozinho."""
    r = c.execute("SELECT * FROM itens WHERE id=?", (iid,)).fetchone()
    if not r or r["lote"] == "DEVOLUCAO" or r["status"] != "AGUARDANDO" or r["falta_material"] or _ja_no_snapshot(c, dict(r)) \
            or iid in _ids_cancelados(c):
        return 0
    falta = False
    for sku, cor, qtd in _pecas_do_item(dict(r)):
        sku, cn = estoque_chave(sku, cor)
        nivel = _controlado(c, sku, cn)
        if not nivel:
            continue
        if nivel == "cor":
            saldo = c.execute("SELECT COALESCE(SUM(qtd),0) FROM estoque_mov WHERE sku=? AND cor=?", (sku, cn)).fetchone()[0]
        else:
            saldo = c.execute("SELECT COALESCE(SUM(qtd),0) FROM estoque_mov WHERE sku=?", (sku,)).fetchone()[0]
        if saldo - _reservado(c, sku, cn, nivel, iid) < qtd:
            falta = True
    if falta:
        c.execute("INSERT INTO eventos(item_id,etapa,colaborador_id,posto,em,alerta) VALUES(?,?,?,?,?,?)",
                  (iid, "FALTA_MATERIAL", None, "ESTOQUE", agora(), "sem estoque"))
        recalcular(c, iid)
        return 1
    return 0


def contagem_estoque(linhas, tipo_obs=None):
    """'SKU;COR;QTD' por linha (cor pode ficar vazia). Define o saldo exato daquele produto/cor agora."""
    feitos, erros, refs = 0, [], []
    with _lock, conn() as c:
        for ln in linhas:
            cols = [x.strip() for x in re.split(r"\t|;", ln)]
            if not cols or not cols[0]:
                continue
            try:
                sku, cor = estoque_chave(cols[0], cols[1] if len(cols) > 2 else "")
                qtd = _num(cols[-1])
            except Exception:
                erros.append(ln.strip()); continue
            atual = c.execute("SELECT COALESCE(SUM(qtd),0) FROM estoque_mov WHERE sku=? AND cor=?", (sku, cor)).fetchone()[0]
            ref = f"CONT|{sku}|{cor}|{agora()}|{secrets.token_hex(3)}"
            c.execute("INSERT INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                      (agora(), sku, cor, qtd - atual, "CONTAGEM", ref, tipo_obs or f"contagem: {qtd:g}"))
            feitos += 1
            refs.append({"ref": ref, "sku": sku, "cor": cor, "qtd": qtd, "antes": atual})
    return {"ok": True, "feitos": feitos, "erros": erros, "refs": refs}


def marcar_sku(sku, cor, status):
    """status: ARQUIVADO (sem estoque aqui e no fornecedor), EXCLUIDO (nao trabalha mais) ou "" (reativar).
    Nao apaga historico: so esconde da contagem, da lista e da sugestao de compra. Devolve o status anterior (para o Voltar)."""
    cor = "" if (cor or "").upper() in ("", "PADRAO", "(COR A DEFINIR)") else estoque_chave(sku, cor)[1]
    sku = estoque_chave(sku)[0]
    status = (status or "").upper()
    if not sku or status not in ("", "ARQUIVADO", "EXCLUIDO"):
        return {"ok": False, "erro": "dados invalidos"}
    with _lock, conn() as c:
        ant = c.execute("SELECT status FROM sku_status WHERE sku=? AND cor=?", (sku, cor)).fetchone()
        if status:
            c.execute("INSERT OR REPLACE INTO sku_status VALUES(?,?,?,?)", (sku, cor, status, agora()))
        else:
            c.execute("DELETE FROM sku_status WHERE sku=? AND cor=?", (sku, cor))
    return {"ok": True, "sku": sku, "cor": cor or "PADRAO", "status": status, "anterior": ant[0] if ant else ""}


def desfazer_contagem(ref):
    """Volta uma contagem ou baixa manual confirmada: apaga aquele lancamento (o saldo soma o que mexeu depois)."""
    with _lock, conn() as c:
        r = c.execute("SELECT sku, cor FROM estoque_mov WHERE ref=? AND tipo IN ('CONTAGEM','AJUSTE')", (ref or "",)).fetchone()
        if not r:
            return {"ok": False, "erro": "ja desfeita ou nao encontrada"}
        c.execute("DELETE FROM estoque_mov WHERE ref=? AND tipo IN ('CONTAGEM','AJUSTE')", (ref,))
        saldo = c.execute("SELECT COALESCE(SUM(qtd),0) FROM estoque_mov WHERE sku=? AND cor=?", (r[0], r[1])).fetchone()[0]
    return {"ok": True, "sku": r[0], "cor": r[1], "saldo": saldo}


def movimento_manual(linhas):
    """'SKU;COR;+5' (acrescentar) ou 'SKU;COR;-3' (baixar). Nunca zera negativo: soma de verdade."""
    feitos, erros = [], []
    with _lock, conn() as c:
        for ln in linhas:
            cols = [x.strip() for x in re.split(r"\t|;", ln)]
            if not cols or not cols[0]:
                continue
            try:
                sku, cor = estoque_chave(cols[0], cols[1] if len(cols) > 2 else "")
                q = _num(cols[-1].replace("+", ""))
            except Exception:
                erros.append(ln.strip()); continue
            antes = c.execute("SELECT COALESCE(SUM(qtd),0) FROM estoque_mov WHERE sku=? AND cor=?", (sku, cor)).fetchone()[0]
            c.execute("INSERT INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                      (agora(), sku, cor, q, "AJUSTE", f"AJ|{sku}|{cor}|{agora()}|{secrets.token_hex(3)}",
                       "acrescentar" if q > 0 else "baixar"))
            feitos.append({"sku": sku, "cor": cor or "-", "antes": antes, "mov": q, "depois": antes + q})
    return {"ok": True, "feitos": feitos, "erros": erros}


def baixa_manual(sku, cor, qtd, motivo=""):
    """Tira do estoque na mao (brinde, quebra, venda fora da plataforma...). So aceita SKU/cor que o estoque conhece,
    para um erro de digitacao nao criar produto fantasma. Devolve a ref para o Voltar."""
    try:
        q = int(str(qtd).strip())
    except Exception:
        return {"ok": False, "erro": "quantidade invalida"}
    if q <= 0 or q > 100000:
        return {"ok": False, "erro": "quantidade invalida"}
    cor = "" if (cor or "").strip().upper() in ("", "PADRAO") else cor
    sku, cn = estoque_chave(sku, cor)
    if not sku:
        return {"ok": False, "erro": "informe o SKU"}
    motivo = re.sub(r"\s+", " ", str(motivo or "")).strip()[:80] or "sem motivo"
    with _lock, conn() as c:
        if not c.execute("SELECT 1 FROM estoque_mov WHERE sku=? AND cor=? LIMIT 1", (sku, cn)).fetchone():
            cores = [r[0] or "PADRAO" for r in c.execute("SELECT DISTINCT cor FROM estoque_mov WHERE sku=? ORDER BY cor", (sku,))]
            return {"ok": False, "erro": (f"{sku} nao tem a cor '{cn or 'PADRAO'}'. Cores: " + ", ".join(cores)) if cores
                    else f"SKU {sku} nao existe no estoque (confira a digitacao)"}
        antes = c.execute("SELECT COALESCE(SUM(qtd),0) FROM estoque_mov WHERE sku=? AND cor=?", (sku, cn)).fetchone()[0]
        ref = f"AJ|{sku}|{cn}|{agora()}|{secrets.token_hex(3)}"
        c.execute("INSERT INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                  (agora(), sku, cn, -q, "AJUSTE", ref, "baixa manual: " + motivo))
    return {"ok": True, "ref": ref, "sku": sku, "cor": cn or "PADRAO", "qtd": q, "antes": antes, "depois": antes - q,
            "motivo": motivo}


def salvar_prateleiras(texto):
    """Aceita o prateleiras.csv (com cabecalho SKU/Prateleira em qualquer ordem) ou linhas 'SKU;PRATELEIRA'."""
    linhas = [l for l in (texto or "").replace("\r", "").split("\n") if l.strip()]
    if not linhas:
        return {"ok": False, "erro": "vazio"}
    sep = max(";,\t", key=lambda x: linhas[0].count(x))
    cab = [x.strip().strip('"').upper() for x in linhas[0].split(sep)]
    i_sku, i_pr = 0, 1
    if any("PRATEL" in x for x in cab) or any("SKU" in x for x in cab):
        i_pr = next((i for i, x in enumerate(cab) if "PRATEL" in x or "LOCAL" in x or "ENDERE" in x), 1)
        i_sku = next((i for i, x in enumerate(cab) if "SKU" in x or "COD" in x), 0 if i_pr != 0 else 1)
        linhas = linhas[1:]
    n = 0
    with _lock, conn() as c:
        for l in linhas:
            cols = [x.strip().strip('"') for x in l.split(sep)]
            if len(cols) <= max(i_sku, i_pr) or not cols[i_sku] or not cols[i_pr]:
                continue
            c.execute("INSERT OR REPLACE INTO prateleiras VALUES(?,?)", (estoque_chave(cols[i_sku])[0], cols[i_pr].upper()))
            n += 1
    return {"ok": True, "salvas": n}


def _ordem_prateleira(p):
    """Ordem natural: A2 antes de A10; sem prateleira vai para o fim."""
    if not p:
        return (1, [])
    return (0, [(0, int(t), "") if t.isdigit() else (1, 0, t) for t in re.findall(r"\d+|[A-Z]+", p.upper())])


def folha_contagem(cego=False, filtro=""):
    """Pagina para imprimir (ou salvar em PDF) a contagem: prateleira > SKU > cor, em 2 colunas para gastar pouco papel."""
    import html as H
    itens = [i for i in estoque()["itens"] if not (i["sem_cor"] and not i["fisico"])]
    with conn() as c:
        prat = {r[0]: r[1] for r in c.execute("SELECT sku, prateleira FROM prateleiras")}
    f = (filtro or "").upper().strip()
    rows = []
    for i in itens:
        pr = prat.get(i["sku"]) or prat.get(re.sub(r"[PMG]$", "", i["sku"]), "")
        if f and f not in (pr + " " + i["sku"] + " " + i["nome"]).upper():
            continue
        rows.append((pr, i))
    rows.sort(key=lambda x: (_ordem_prateleira(x[0]), x[1]["sku"], x[1]["cor"]))
    hoje = datetime.now(BR).strftime("%d/%m/%Y %H:%M")
    corpo, atual = [], None
    for pr, i in rows:
        if pr != atual:
            atual = pr
            corpo.append(f'<tr class="g"><td colspan="5">{H.escape(pr or "SEM PRATELEIRA")}</td></tr>')
        sis = "" if cego else f'{i["fisico"]:g}'
        corpo.append(f'<tr><td class="s">{H.escape(i["sku"])}</td><td class="n">{H.escape((i["nome"] or "")[:26])}</td>'
                     f'<td>{H.escape(i["cor"])}</td><td class="q">{sis}</td><td class="c"></td></tr>')
    return f"""<!doctype html><html lang="pt-br"><head><meta charset="utf-8"><title>Contagem de estoque {hoje}</title>
<style>@page{{size:A4;margin:8mm}}body{{font:8.5pt Arial,sans-serif;margin:0}}
h1{{font-size:11pt;margin:0 0 4px}}.top{{display:flex;justify-content:space-between;align-items:end;margin-bottom:4px}}
.cols{{column-count:2;column-gap:6mm}}table{{width:100%;border-collapse:collapse}}
td{{border-bottom:.3pt solid #999;padding:.8mm 1mm;vertical-align:bottom}}tr{{break-inside:avoid}}
tr.g td{{background:#e5e5e5;font-weight:bold;border:0;padding:1mm}}.s{{font-weight:bold;white-space:nowrap}}
.n{{font-size:7pt;color:#444}}.q{{text-align:right;width:9mm;color:#777}}.c{{width:14mm;border-bottom:.8pt solid #000}}
@media screen{{body{{margin:12px}}#bt{{margin-bottom:8px}}}}@media print{{#bt{{display:none}}}}</style></head><body>
<div id="bt"><button onclick="print()" style="font-size:15px;padding:6px 14px">Imprimir / Salvar em PDF</button>
<a href="?cego={0 if cego else 1}&filtro={H.escape(filtro or '')}">{'mostrar' if cego else 'esconder'} o saldo do sistema</a></div>
<div class="top"><h1>Contagem de estoque — Brindes do Boni</h1><span>{hoje} · {len(rows)} linhas · {'contagem às cegas' if cego else 'coluna cinza = sistema'} · Contou: ____________</span></div>
<div class="cols"><table>{''.join(corpo)}</table></div></body></html>"""


def distribuir_cor(sku, partes):
    """Passa unidades que entraram sem cor (nota da XBZ) para as cores certas: partes = {cor: qtd}."""
    sku = estoque_chave(sku)[0]
    with _lock, conn() as c:
        for cor, q in partes.items():
            q = _num(q)
            if not q:
                continue
            marca = f"{agora()}|{secrets.token_hex(3)}"
            c.execute("INSERT INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                      (agora(), sku, "", -q, "DISTRIBUI", f"DIST-|{sku}|{cor}|{marca}", f"para {cor}"))
            c.execute("INSERT INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                      (agora(), sku, estoque_chave(sku, cor)[1], q, "DISTRIBUI", f"DIST+|{sku}|{cor}|{marca}", "entrada sem cor"))
    return {"ok": True}


def xbz_sincronizar():
    """Le o catalogo da XBZ (preco de custo e estoque do fornecedor). So leitura. Nunca registra a URL (tem o token)."""
    import urllib.request
    cnpj, tok = os.environ.get("XBZ_CNPJ", ""), os.environ.get("XBZ_TOKEN", "")
    if not (cnpj and tok):
        return {"ok": False, "erro": "XBZ_CNPJ/XBZ_TOKEN nao configurados no Railway"}
    from urllib.parse import urlencode
    base = os.environ.get("XBZ_URL", "https://api.minhaxbz.com.br:5001/api/clientes/GetListaDeProdutos")
    ini = datetime.now()
    # a XBZ passou a recusar (403) alguns pedidos: manda tambem cnpj/token no cabecalho e com cara de navegador;
    # se ainda der 403, tenta so pelo cabecalho (igual ao PedidosListar)
    dados, msg = None, ""
    for url in (base + "?" + urlencode({"cnpj": cnpj, "token": tok}), base):
        req = urllib.request.Request(url, headers={"cnpj": cnpj, "token": tok, "Accept": "application/json", "User-Agent": XBZ_UA})
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                dados = json.loads(r.read())
            break
        except urllib.error.HTTPError as e:
            msg = f"XBZ respondeu {e.code}" + (" (acesso negado: confira XBZ_CNPJ/XBZ_TOKEN no Railway ou peça à XBZ para liberar o token)" if e.code in (401, 403) else "")
        except Exception as e:
            msg = _xbz_sem_segredo(str(e), cnpj, tok)
    if dados is None:
        msg = _xbz_sem_segredo(re.sub(r"token=[^&\s]+", "token=***", msg), cnpj, tok)
        _xbz_status.update(ok=False, erro=msg[:200], ultima=agora())
        return {"ok": False, "erro": msg[:200]}
    if not isinstance(dados, list) or len(dados) < 500 or any(not x.get("CodigoXbz") for x in dados[:50]):
        _xbz_status.update(ok=False, erro="resposta incompleta da XBZ - catalogo anterior mantido", ultima=agora())
        return {"ok": False, "erro": _xbz_status["erro"]}
    with _lock, conn() as c:
        vendidos = {r[0] for r in c.execute("SELECT DISTINCT sku FROM estoque_mov")}
        antes = {r["codigo_xbz"]: (r["preco"], r["estoque"]) for r in c.execute("SELECT codigo_xbz, preco, estoque FROM xbz")}
        alertas = []
        for x in dados:
            try:
                preco, est = float(x.get("PrecoVenda") or 0), max(0, int(float(x.get("QuantidadeDisponivel") or 0)))
            except Exception:
                continue
            cod = str(x.get("CodigoAmigavel") or "").strip().upper()
            cx = str(x["CodigoXbz"]).strip()
            if cod in vendidos and cx in antes:
                p0, e0 = antes[cx]
                cor_ = cor_norm(x.get("CorWebPrincipal"))
                if e0 > 0 and est == 0:
                    alertas.append((cod, cor_, "XBZ ZEROU", "estoque da XBZ acabou"))
                elif e0 == 0 and est > 0:
                    alertas.append((cod, cor_, "XBZ VOLTOU", f"voltou a ter {est} na XBZ"))
                elif e0 >= 200 > est:
                    alertas.append((cod, cor_, "XBZ ABAIXO DE 200", f"XBZ tem {est}"))
                if p0 and preco and abs(preco - p0) / p0 >= 0.05:
                    alertas.append((cod, cor_, "PRECO MUDOU", f"R$ {p0:.2f} -> R$ {preco:.2f}".replace(".", ",")))
            c.execute("INSERT OR REPLACE INTO xbz VALUES(?,?,?,?,?,?,?,?,?,?)",
                      (str(x["CodigoXbz"]).strip(), str(x.get("CodigoAmigavel") or x.get("IdProduto") or "").strip().upper(),
                       str(x.get("CodigoComposto") or "").strip().upper(), re.sub(r"\s+", " ", str(x.get("Nome") or "")).strip(),
                       cor_norm(x.get("CorWebPrincipal")), preco, est, str(x.get("StatusConfiabilidade") or "")[:120],
                       str(x.get("ReposicaoDataPrevista") or "")[:10], agora()))
        for a in alertas:
            c.execute("INSERT INTO xbz_alertas(em,sku,cor,tipo,detalhe) VALUES(?,?,?,?,?)", (agora(), *a))
    _xbz_status.update(ok=True, erro="", ultima=agora(), registros=len(dados),
                       segundos=round((datetime.now() - ini).total_seconds(), 1))
    return {"ok": True, "registros": len(dados)}


_xbz_status = {"ok": None, "erro": "", "ultima": "", "registros": 0}


XBZ_HORARIOS = ["08:00", "08:45", "09:30", "10:15", "11:00", "11:45", "12:30", "13:15", "14:00", "14:45", "15:30",
                "16:15", "17:00", "17:45", "18:30"]


def _xbz_loop():
    """Consulta a XBZ nos horarios combinados (15 por dia, menos de 24). Falhou: 1 nova tentativa 15 min depois."""
    import time
    xbz_sincronizar()
    while True:
        agora_br = datetime.now(BR)
        prox = None
        for d in range(0, 2):
            for h in XBZ_HORARIOS:
                t = (agora_br + timedelta(days=d)).replace(hour=int(h[:2]), minute=int(h[3:]), second=0, microsecond=0)
                if t > agora_br and (prox is None or t < prox):
                    prox = t
        time.sleep(max(30, (prox - agora_br).total_seconds()))
        r = xbz_sincronizar()
        if not r.get("ok"):
            print("XBZ:", r.get("erro"), flush=True)
            time.sleep(15 * 60)
            xbz_sincronizar()


# ---- RETIRADAS na XBZ (Minha XBZ > Pedidos > finalizados): cada item retirado entra no NOSSO estoque, ja com a cor.
# Login do site nas Variables do Railway: XBZ_SITE_USUARIO e XBZ_SITE_SENHA (so o servidor le; nunca aparece em log).
XBZ_PEDIDOS_URL = os.environ.get("XBZ_PEDIDOS_URL", "https://api.minhaxbz.com.br:5001/api/ruiz/consultaPedidos")
ESTOQUE_XBZ_DESDE = os.environ.get("ESTOQUE_XBZ_DESDE", "2026-10-06")   # contagem de 05/10 a tarde ja inclui o de antes
XBZ_COR_COD = {"PRE": "PRETO", "BCO": "BRANCO", "AZU": "AZUL", "AZC": "AZUL CLARO", "AZE": "AZUL ESCURO", "VM": "VERMELHO",
               "VD": "VERDE", "VDC": "VERDE CLARO", "VDE": "VERDE ESCURO", "ROS": "ROSA", "RSC": "ROSA CLARO",
               "RSE": "ROSA ESCURO", "ROX": "ROXO", "LIL": "LILAS", "CIN": "CINZA", "LAR": "LARANJA", "CRE": "CREME",
               "BEG": "BEGE", "INO": "INOX", "PRA": "PRATA", "DOU": "DOURADO", "MAR": "MARROM", "AMA": "AMARELO",
               "TUR": "TURQUESA", "VIN": "VINHO", "CHA": "CHAMPAGNE", "MAD": "MADEIRA", "KRA": "KRAFT", "PNK": "PINK"}
# tudo que for retirado, de todos os CNPJs, entra (pedido de 06/10). Para ignorar algum: XBZ_RETIRADAS_IGNORAR=LAURA
XBZ_RETIRADAS_IGNORAR = [x.strip().upper() for x in os.environ.get("XBZ_RETIRADAS_IGNORAR", "").split(",") if x.strip()]
_xbz_ret_status = {"ultima": "", "ok": None, "erro": "", "entraram": 0, "lidos": 0}


def xbz_site_configurado():
    return bool(os.environ.get("XBZ_SITE_USUARIO") and os.environ.get("XBZ_SITE_SENHA"))


def _xbz_item_para_estoque(c, x):
    """Item do pedido da XBZ -> (sku, cor) do nosso estoque. Usa o catalogo da XBZ (codigo composto -> cor)."""
    comp = str(x.get("produtoCodigoComposto") or "").strip().upper()
    sis = str(x.get("produtoCodigoSistema") or "").strip().upper()
    r = c.execute("SELECT codigo, cor FROM xbz WHERE composto=? OR codigo_xbz=? OR codigo_xbz=? LIMIT 1",
                  (comp, sis, sis.lstrip("X"))).fetchone()
    if r and r["codigo"]:
        sku, cor = r["codigo"], r["cor"] or ""
    else:
        partes = comp.split("-", 1)
        sku = partes[0]
        cor = " ".join(XBZ_COR_COD.get(p_, p_) for p_ in (partes[1].split("/") if len(partes) > 1 else []))
    return estoque_chave(sku, cor, x.get("produtoNome") or "")


def xbz_retiradas(aplicar=True):
    """Le na Minha XBZ os pedidos RETIRADOS/FINALIZADOS e da entrada no estoque de cada item retirado a partir de
    ESTOQUE_XBZ_DESDE (uma vez so por item), de TODOS os CNPJs do grupo."""
    import urllib.request
    from urllib.parse import urlencode
    if not xbz_site_configurado():
        _xbz_ret_status.update(ok=False, erro="falta XBZ_SITE_USUARIO / XBZ_SITE_SENHA no Railway", ultima=agora())
        return {"ok": False, "erro": _xbz_ret_status["erro"]}
    q = {"user": os.environ["XBZ_SITE_USUARIO"], "passwd": os.environ["XBZ_SITE_SENHA"], "browserFingerPrint": "xbz",
         "idPeriodoSelecionado": "0", "idStatusFinanceiroSelecionado": "1", "idStatusLogisticoSelecionado": "1",
         "idPessoaEmissao": "0", "idTipoListagemSelecionada": "2", "idMostraEntregues": "3", "numeroPedido": ""}
    try:
        with urllib.request.urlopen(XBZ_PEDIDOS_URL + "?" + urlencode(q), timeout=90) as r:
            dados = json.loads(r.read() or b"[]")
    except Exception as e:
        msg = re.sub(r"(passwd|user)=[^&\s]+", r"\1=***", str(e))[:200]   # nunca mostra o login
        _xbz_ret_status.update(ok=False, erro=msg, ultima=agora())
        return {"ok": False, "erro": msg}
    if not isinstance(dados, list):
        _xbz_ret_status.update(ok=False, erro="resposta inesperada da Minha XBZ (login certo?)", ultima=agora())
        return {"ok": False, "erro": _xbz_ret_status["erro"]}
    novos, ignorados, lidos = [], 0, 0
    with conn() as c:
        for x in dados:
            st = str(x.get("statusLogistico") or "").upper()
            if not (x.get("idStatus") == 9 or st.startswith("RETIRADO")):
                continue
            quando = str(x.get("statusData") or "")[:19]
            if quando[:10] < ESTOQUE_XBZ_DESDE:
                continue
            lidos += 1
            obs = f"{x.get('numeroPedidoInternoCliente') or ''} {x.get('razaoSocial') or ''}".upper()
            if any(i_ and i_ in obs for i_ in XBZ_RETIRADAS_IGNORAR):
                ignorados += 1
                continue
            iid, qtd = x.get("pedidoItemId"), x.get("pedidoItemQuantidade")
            if not iid or not qtd:
                continue
            ref = f"XBZ|{iid}"
            if c.execute("SELECT 1 FROM estoque_mov WHERE ref=?", (ref,)).fetchone():
                continue
            sku, cn = _xbz_item_para_estoque(c, x)
            if _excluido(c, sku, cn):
                ignorados += 1
                continue
            try:
                em = datetime.fromisoformat(quando).replace(tzinfo=BR).astimezone(timezone.utc).isoformat()
            except Exception:
                em = agora()
            novos.append((em, sku, cn, float(qtd), "ENTRADA_XBZ", ref,
                          f"retirada XBZ {x.get('numero') or ''} {x.get('produtoCodigoComposto') or ''} {(x.get('razaoSocial') or '')[:30]}"))
    if aplicar and novos:
        with _lock, conn() as c:
            for m in novos:
                c.execute("INSERT OR IGNORE INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)", m)
            r = c.execute("SELECT valor FROM meta WHERE chave='xbz_retiradas_log'").fetchone()
            log = (json.loads(r[0]) if r and r[0] else [])[-300:] + [
                {"em": m[0], "sku": m[1], "cor": m[2] or "(cor a definir)", "qtd": m[3], "obs": m[6]} for m in novos]
            c.execute("INSERT OR REPLACE INTO meta(chave, valor) VALUES('xbz_retiradas_log', ?)", (json.dumps(log, ensure_ascii=False),))
    _xbz_ret_status.update(ok=True, erro="" if dados else "a Minha XBZ nao devolveu nenhum pedido finalizado no ultimo mes: confira usuario/senha no Railway",
                           ultima=agora(), lidos=lidos, entraram=len(novos) if aplicar else 0)
    if novos and aplicar:
        print(f"XBZ retiradas: {len(novos)} item(ns) entraram no estoque", flush=True)
    return {"ok": True, "lidos_desde_corte": lidos, "laura_ignorados": ignorados, "novos": len(novos),
            "itens": [{"sku": m[1], "cor": m[2] or "(cor a definir)", "qtd": m[3], "obs": m[6]} for m in novos]}


def xbz_retiradas_log():
    with conn() as c:
        r = c.execute("SELECT valor FROM meta WHERE chave='xbz_retiradas_log'").fetchone()
    return {"status": _xbz_ret_status, "configurado": xbz_site_configurado(), "desde": ESTOQUE_XBZ_DESDE,
            "itens": (json.loads(r[0]) if r and r[0] else [])[::-1][:200]}


def _xbz_retiradas_loop():
    """De 30 em 30 minutos (o material retirado entra no estoque no mesmo dia)."""
    import time
    time.sleep(60)
    while True:
        try:
            r = xbz_retiradas()
            if not r.get("ok"):
                print("XBZ retiradas:", r.get("erro"), flush=True)
        except Exception as e:
            print("XBZ retiradas:", str(e)[:150], flush=True)
        time.sleep(30 * 60)


# ------------------------------------------------------------------ compras XBZ recebidas (API oficial PedidosListar)
# Variables do Railway (so o servidor le; nunca vai para o navegador nem para o log):
#   XBZ_CNPJ_1 / XBZ_TOKEN_1, XBZ_CNPJ_2 / XBZ_TOKEN_2, ... (todos os CNPJs alimentam o MESMO estoque)
#   (XBZ_CNPJ / XBZ_TOKEN do catalogo tambem entram, se estiverem la)
# Regras: so status que comeca com RETIRADO/ENVIADO/FINALIZADO; cada CNPJ+pedido+SKU XBZ+composto entra UMA vez;
# SKU que nao da para ligar com certeza ao nosso -> pendencia de mapeamento (o estoque NAO muda).
XBZ_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"
XBZ_API_BASE = os.environ.get("XBZ_API_BASE", "https://api.minhaxbz.com.br:5001/api/clientes").rstrip("/")
XBZ_API_DIAS = int(os.environ.get("XBZ_API_DIAS", "10"))
XBZ_STATUS_OK = "RETIRADO/ENVIADO/FINALIZADO"
_xbz_api_lock = threading.Lock()
_xbz_api_status = {"ultima": "", "ok": None, "erro": "", "contas": []}


def _xbz_contas():
    vistas, contas = set(), []
    geral = os.environ.get("XBZ_TOKEN", "")   # o mesmo token serve para todos os CNPJs ligados a nos
    pares = [(os.environ.get(f"XBZ_CNPJ_{n}", ""), os.environ.get(f"XBZ_TOKEN_{n}", "") or geral) for n in range(1, 21)]
    pares += [(x, geral) for x in re.split(r"[,;\s]+", os.environ.get("XBZ_CNPJS", "")) if x.strip()]   # XBZ_CNPJS=cnpj1,cnpj2,...
    pares.append((os.environ.get("XBZ_CNPJ", ""), geral))
    for cnpj, tok in pares:
        cnpj = re.sub(r"\D", "", cnpj or "")
        if cnpj and tok and cnpj not in vistas:
            vistas.add(cnpj)
            contas.append((cnpj, tok.strip()))
    return contas


def xbz_api_configurada():
    return bool(_xbz_contas())


def _cnpj_mascara(cnpj):
    d = re.sub(r"\D", "", cnpj or "")
    return f"**.***.***/{d[8:12]}-{d[12:14]}" if len(d) == 14 else "***" + d[-4:]


def _xbz_buscar_pedidos(cnpj, token, dias=None):
    """GET /PedidosListar (so no servidor). CNPJ e token vao no cabecalho, nunca na URL nem no log."""
    import urllib.request
    url = (f"{XBZ_API_BASE}/PedidosListar?qtd_dias={int(dias or XBZ_API_DIAS)}"
           "&exibir_finalizados=true&exibir_cancelados=false")
    req = urllib.request.Request(url, headers={"cnpj": cnpj, "token": token, "Accept": "application/json", "User-Agent": XBZ_UA})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            dados = json.loads(r.read() or b"[]")
    except urllib.error.HTTPError as e:
        corpo = ""
        try:
            corpo = e.read().decode("utf-8", "replace")[:150]
        except Exception:
            pass
        raise RuntimeError(f"XBZ respondeu {e.code}: {_xbz_sem_segredo(corpo, cnpj, token)}")
    except Exception as e:
        raise RuntimeError(_xbz_sem_segredo(str(e), cnpj, token)[:150])
    if isinstance(dados, dict):   # caso a XBZ embrulhe a lista
        dados = next((v for v in dados.values() if isinstance(v, list)), None)
        if dados is None:
            raise RuntimeError("resposta inesperada da XBZ (sem lista de pedidos)")
    if not isinstance(dados, list):
        raise RuntimeError("resposta inesperada da XBZ")
    return [x for x in dados if isinstance(x, dict)]


def _xbz_sem_segredo(t, cnpj, token):
    t = str(t or "")
    for seg in (token, cnpj):
        if seg and len(seg) >= 6:
            t = t.replace(seg, "***")
    return t


def _xbz_chave(cnpj, x):
    return (cnpj, str(x.get("numero") or "").strip(), str(x.get("produtoCodigoXbz") or "").strip().upper(),
            str(x.get("produtoCodigoComposto") or "").strip().upper())


_RX_DATA = re.compile(r"(\d{4})-(\d{2})-(\d{2})|(\d{2})/(\d{2})/(\d{4}|\d{2})(?!\d)")


def _xbz_data_retirada(x):
    """Data da retirada: a mais recente escrita no status ("RETIRADO/... DATA: 06/10/26 10:00") ou em retiradaDetalhes."""
    datas = []
    for m in _RX_DATA.finditer(f"{x.get('statusLogistico') or ''} {x.get('retiradaDetalhes') or ''}"):
        if m[1]:
            datas.append(f"{m[1]}-{m[2]}-{m[3]}")
        else:
            a = m[6] if len(m[6]) == 4 else "20" + m[6]
            datas.append(f"{a}-{m[5]}-{m[4]}")
    return max(datas) if datas else None


def _sku_conhecido(c, sku):
    return bool(c.execute("""SELECT 1 FROM xbz WHERE codigo=? UNION ALL SELECT 1 FROM estoque_mov WHERE sku=?
                             UNION ALL SELECT 1 FROM itens WHERE sku=? LIMIT 1""", (sku, sku, sku)).fetchone())


def _xbz_mapear_seguro(c, x):
    """(sku, cor, como) do NOSSO estoque, ou (None, None, motivo) quando nao da para ter certeza. Nunca adivinha."""
    comp = str(x.get("produtoCodigoComposto") or "").strip().upper()
    cxbz = str(x.get("produtoCodigoXbz") or "").strip().upper()
    nome = x.get("produtoNome") or ""
    # 1) ligacao feita a mao no painel
    for k in ([comp] if comp else []) + (["X:" + cxbz] if cxbz else []):
        r = c.execute("SELECT sku, cor FROM xbz_mapa WHERE chave=?", (k,)).fetchone()
        if r:
            sku, cor = estoque_chave(r["sku"], r["cor"] or "", nome)
            return sku, cor, "ligacao manual"
    # 2) catalogo da XBZ: o codigo composto (ou o SKU XBZ) aparece exatamente, com o nosso codigo
    rows = []
    if comp:
        rows = c.execute("SELECT codigo, cor FROM xbz WHERE composto=?", (comp,)).fetchall()
    if not rows and cxbz:
        rows = c.execute("SELECT codigo, cor FROM xbz WHERE codigo_xbz=? OR codigo_xbz=?", (cxbz, cxbz.lstrip("X"))).fetchall()
    alvos = {(r["codigo"], (r["cor"] or "").upper()) for r in rows if r["codigo"]}
    if len(alvos) == 1:
        sku, cor = alvos.pop()
        variantes = c.execute("SELECT COUNT(DISTINCT COALESCE(cor,'')) FROM xbz WHERE codigo=?", (sku,)).fetchone()[0]
        if cor or variantes <= 1:
            sku, cor = estoque_chave(sku, cor, nome)
            return sku, cor, "catalogo XBZ"
        return None, None, f"catalogo XBZ sem a cor de {comp or cxbz}"
    if len(alvos) > 1:
        return None, None, f"{comp or cxbz} aparece em mais de um produto do catalogo"
    # 3) codigo composto "18949M-ROS": produto que ja conhecemos + codigo de cor conhecido
    if comp:
        partes = comp.split("-", 1)
        base = partes[0]
        if not _sku_conhecido(c, base):
            return None, None, f"produto {base} nao existe no nosso estoque/catalogo"
        if len(partes) == 1:
            if c.execute("SELECT COUNT(DISTINCT COALESCE(cor,'')) FROM xbz WHERE codigo=?", (base,)).fetchone()[0] > 1:
                return None, None, f"{base} tem varias cores e o codigo nao diz qual"
            sku, cor = estoque_chave(base, "", nome)
            return sku, cor, "codigo composto (produto de uma cor)"
        cods = partes[1].split("/")
        desconhecidas = [p_ for p_ in cods if p_ not in XBZ_COR_COD]
        if desconhecidas:
            return None, None, "codigo de cor desconhecido: " + ", ".join(desconhecidas)
        sku, cor = estoque_chave(base, " ".join(XBZ_COR_COD[p_] for p_ in cods), nome)
        return sku, cor, "codigo composto"
    return None, None, "sem codigo composto e sem SKU XBZ no catalogo"


def _xbz_registrar(c, chave, x, qtd, sku, cor, resultado, mov_id=None):
    cur = c.execute("""INSERT OR IGNORE INTO xbz_pedidos_importados(cnpj, pedido_numero, produto_codigo_xbz,
                       produto_codigo_composto, quantidade, status_logistico, sku, cor, resultado, mov_id, importado_em)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (*chave, int(qtd), str(x.get("statusLogistico") or "")[:80], sku, cor, resultado, mov_id, agora()))
    return cur.rowcount == 1


def _xbz_processar(c, chave, x, qtd, confirmar_data=False):
    """Um item (ja somado). Devolve 'ja', 'entrou', 'antes', 'excluida', 'pendente' ou 'existente'. Tudo dentro da
    mesma transacao: o controle e o movimento entram juntos (ou nenhum dos dois)."""
    if c.execute("""SELECT 1 FROM xbz_pedidos_importados WHERE cnpj=? AND pedido_numero=? AND produto_codigo_xbz=?
                    AND produto_codigo_composto=?""", chave).fetchone():
        return "ja", None
    cnpj, numero, cxbz, comp = chave
    # retirada que ja tinha entrado pela leitura antiga do site (Minha XBZ): so registra, nao soma de novo
    antigo = c.execute("SELECT id FROM estoque_mov WHERE tipo='ENTRADA_XBZ' AND obs LIKE ? AND ref LIKE 'XBZ|%'",
                       (f"retirada XBZ {numero} {comp} %",)).fetchone()
    if antigo:
        _xbz_registrar(c, chave, x, qtd, None, None, "ja estava no estoque (leitura antiga)", antigo[0])
        return "existente", None
    data_ret = _xbz_data_retirada(x)
    emissao = str(x.get("dataEmissao") or "")[:10]
    if not confirmar_data:
        if data_ret and data_ret < ESTOQUE_XBZ_DESDE:
            _xbz_registrar(c, chave, x, qtd, None, None, f"retirado em {data_ret}: antes da contagem do estoque")
            return "antes", None
        if not data_ret and emissao and emissao < ESTOQUE_XBZ_DESDE:
            return "pendente", ("data", f"pedido de {emissao} sem data de retirada: confirme se chegou depois da contagem")
    sku, cor, como = _xbz_mapear_seguro(c, x)
    if not sku:
        return "pendente", ("mapeamento", como)
    if _excluido(c, sku, cor):
        _xbz_registrar(c, chave, x, qtd, sku, cor, "cor/produto excluido: nao entra")
        return "excluida", None
    imp = agora()
    ref = "XBZAPI|" + "|".join(chave)
    mid = c.execute("""INSERT OR IGNORE INTO estoque_mov(em, sku, cor, qtd, tipo, ref, obs, xbz_pedido, importado_em)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (imp, sku, cor, float(qtd), "ENTRADA_XBZ", ref,
                     f"compra XBZ {numero} {comp or cxbz} ({como})" + (f" retirado {data_ret}" if data_ret else ""),
                     numero, imp)).lastrowid
    if not c.execute("SELECT 1 FROM estoque_mov WHERE ref=?", (ref,)).fetchone():
        raise RuntimeError("movimento nao gravou")
    mid = c.execute("SELECT id FROM estoque_mov WHERE ref=?", (ref,)).fetchone()[0]
    _xbz_registrar(c, chave, x, qtd, sku, cor, "entrou no estoque", mid)
    c.execute("UPDATE xbz_pendencias SET resolvido_em=?, resolucao=? WHERE cnpj=? AND pedido_numero=? AND produto_codigo_xbz=? "
              "AND produto_codigo_composto=? AND resolvido_em IS NULL", (imp, f"entrou como {sku} {cor}".strip(), *chave))
    return "entrou", (sku, cor)


def _xbz_pendencia(c, chave, x, qtd, tipo, motivo):
    agora_ = agora()
    c.execute("""INSERT INTO xbz_pendencias(cnpj, pedido_numero, produto_codigo_xbz, produto_codigo_composto, codigo_amigavel,
                 produto_nome, quantidade, status_logistico, data_ref, tipo, motivo, dados, criado_em, atualizado_em)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                 ON CONFLICT(cnpj, pedido_numero, produto_codigo_xbz, produto_codigo_composto) DO UPDATE SET
                 quantidade=excluded.quantidade, tipo=excluded.tipo, motivo=excluded.motivo, dados=excluded.dados,
                 atualizado_em=excluded.atualizado_em WHERE resolvido_em IS NULL""",
              (*chave, str(x.get("produtoCodigoAmigavel") or ""), str(x.get("produtoNome") or "")[:120], int(qtd),
               str(x.get("statusLogistico") or "")[:80], _xbz_data_retirada(x) or str(x.get("dataEmissao") or "")[:10],
               tipo, motivo, json.dumps({k: x.get(k) for k in ("numero", "produtoCodigoXbz", "produtoCodigoComposto",
                                         "produtoCodigoAmigavel", "produtoNome", "statusLogistico", "dataEmissao",
                                         "retiradaDetalhes")}, ensure_ascii=False), agora_, agora_))


def xbz_compras_sincronizar(aplicar=True):
    """Le todas as contas e da entrada no estoque. Rodar 1 ou 100 vezes da o MESMO estoque."""
    contas = _xbz_contas()
    if not contas:
        _xbz_api_status.update(ok=False, erro="falta XBZ_CNPJ_1 / XBZ_TOKEN_1 nas Variables do Railway", ultima=agora())
        return {"ok": False, "erro": _xbz_api_status["erro"]}
    if not _xbz_api_lock.acquire(blocking=False):
        return {"ok": False, "erro": "ja esta sincronizando (tente em 1 minuto)"}
    try:
        res = {"ok": True, "contas": [], "entrou": 0, "ja": 0, "pendentes": 0, "antes_da_contagem": 0, "excluidas": 0,
               "ignorados_status": 0, "itens": []}
        for cnpj, tok in contas:
            cr = {"cnpj": _cnpj_mascara(cnpj), "ok": True, "lidos": 0}
            try:
                dados = _xbz_buscar_pedidos(cnpj, tok)
            except Exception as e:
                cr.update(ok=False, erro=str(e)[:200])
                res["contas"].append(cr)
                res["ok"] = False
                continue
            cr["lidos"] = len(dados)
            soma = {}
            for x in dados:
                if not str(x.get("statusLogistico") or "").strip().upper().startswith(XBZ_STATUS_OK):
                    res["ignorados_status"] += 1
                    continue
                ch = _xbz_chave(cnpj, x)
                if not ch[1] or not (ch[2] or ch[3]):
                    continue
                try:
                    q = int(round(float(x.get("quantidade") or 0)))
                except (TypeError, ValueError):
                    q = 0
                if q <= 0:
                    continue
                if ch in soma:
                    soma[ch][1] += q   # mesma linha repetida no pedido: soma (continua entrando 1 vez so)
                else:
                    soma[ch] = [x, q]
            with _lock, conn() as c:
                for ch, (x, q) in soma.items():
                    if not aplicar:
                        c.execute("SAVEPOINT sim")
                    pj = c.execute("""SELECT tipo FROM xbz_pendencias WHERE cnpj=? AND pedido_numero=? AND produto_codigo_xbz=?
                                      AND produto_codigo_composto=? AND resolvido_em IS NULL""", ch).fetchone()
                    st, info = _xbz_processar(c, ch, x, q, confirmar_data=bool(pj and pj[0] == "mapeamento"))
                    if st == "pendente":
                        if aplicar:
                            _xbz_pendencia(c, ch, x, q, *info)
                        res["pendentes"] += 1
                        res["itens"].append({"pedido": ch[1], "composto": ch[3] or ch[2], "qtd": q, "pendente": info[1]})
                    elif st == "entrou":
                        res["entrou"] += 1
                        res["itens"].append({"pedido": ch[1], "composto": ch[3] or ch[2], "qtd": q, "sku": info[0], "cor": info[1]})
                    elif st == "antes":
                        res["antes_da_contagem"] += 1
                    elif st == "excluida":
                        res["excluidas"] += 1
                    else:
                        res["ja"] += 1
                    if not aplicar:
                        c.execute("ROLLBACK TO sim")
                        c.execute("RELEASE sim")
            res["contas"].append(cr)
        if aplicar:
            _xbz_api_status.update(ok=res["ok"], erro="; ".join(f"{x['cnpj']}: {x['erro']}" for x in res["contas"] if not x["ok"]),
                                   ultima=agora(), contas=res["contas"],
                                   resumo={k: res[k] for k in ("entrou", "ja", "pendentes", "antes_da_contagem", "excluidas")})
            if res["entrou"]:
                _op_cache["v"] = None
                print(f"XBZ compras: {res['entrou']} item(ns) entraram no estoque, {res['pendentes']} pendente(s)", flush=True)
        res["simulacao"] = not aplicar
        return res
    finally:
        _xbz_api_lock.release()


def xbz_compras_painel():
    with conn() as c:
        pend = [dict(r) for r in c.execute("""SELECT id, cnpj, pedido_numero, produto_codigo_xbz, produto_codigo_composto,
                 codigo_amigavel, produto_nome, quantidade, data_ref, tipo, motivo, criado_em FROM xbz_pendencias
                 WHERE resolvido_em IS NULL ORDER BY id DESC LIMIT 300""")]
        imp = [dict(r) for r in c.execute("""SELECT cnpj, pedido_numero, produto_codigo_xbz, produto_codigo_composto, quantidade,
                 sku, cor, resultado, importado_em FROM xbz_pedidos_importados ORDER BY id DESC LIMIT 200""")]
        mapa = [dict(r) for r in c.execute("SELECT chave, sku, cor, em FROM xbz_mapa ORDER BY em DESC LIMIT 200")]
    for x in pend + imp:
        x["cnpj"] = _cnpj_mascara(x["cnpj"])   # nunca manda o CNPJ inteiro para a tela
    return {"ok": True, "configurado": xbz_api_configurada(), "contas": len(_xbz_contas()), "desde": ESTOQUE_XBZ_DESDE,
            "status": {k: v for k, v in _xbz_api_status.items()}, "pendencias": pend, "importados": imp, "mapa": mapa}


def xbz_resolver_pendencia(pid, acao, sku="", cor=""):
    """acao: 'mapear' (liga o codigo XBZ ao nosso SKU/cor e da a entrada), 'entrar' (confirma a data e da a entrada),
    'ignorar' (nunca entra)."""
    sku = str(sku or "").strip().upper()
    cor = str(cor or "").strip().upper()
    with _lock, conn() as c:
        p = c.execute("SELECT * FROM xbz_pendencias WHERE id=? AND resolvido_em IS NULL", (int(pid),)).fetchone()
        if not p:
            return {"ok": False, "erro": "pendencia nao encontrada (ja resolvida?)"}
        x = json.loads(p["dados"] or "{}")
        chave = (p["cnpj"], p["pedido_numero"], p["produto_codigo_xbz"], p["produto_codigo_composto"])
        if acao == "ignorar":
            _xbz_registrar(c, chave, x, p["quantidade"], None, None, "ignorado no painel")
            c.execute("UPDATE xbz_pendencias SET resolvido_em=?, resolucao='ignorado' WHERE id=?", (agora(), p["id"]))
            return {"ok": True, "resultado": "ignorado"}
        if acao == "mapear":
            if not sku:
                return {"ok": False, "erro": "informe o SKU"}
            k = p["produto_codigo_composto"] or "X:" + p["produto_codigo_xbz"]
            c.execute("INSERT OR REPLACE INTO xbz_mapa(chave, sku, cor, em) VALUES(?,?,?,?)", (k, sku, cor, agora()))
        elif acao != "entrar":
            return {"ok": False, "erro": "acao invalida"}
        # tenta de novo esta e as outras pendencias do mesmo codigo
        alvo = [p] + ([r for r in c.execute("""SELECT * FROM xbz_pendencias WHERE resolvido_em IS NULL AND id<>? AND tipo='mapeamento'
                        AND produto_codigo_composto=? AND produto_codigo_xbz=?""",
                       (p["id"], p["produto_codigo_composto"], p["produto_codigo_xbz"]))] if acao == "mapear" else [])
        feitos, falta = 0, ""
        for r in alvo:
            xr = json.loads(r["dados"] or "{}")
            ch = (r["cnpj"], r["pedido_numero"], r["produto_codigo_xbz"], r["produto_codigo_composto"])
            st, info = _xbz_processar(c, ch, xr, r["quantidade"], confirmar_data=(acao == "entrar" or r["tipo"] != "data"))
            if st == "pendente":
                _xbz_pendencia(c, ch, xr, r["quantidade"], *info)
                falta = info[1]
            else:
                c.execute("UPDATE xbz_pendencias SET resolvido_em=?, resolucao=COALESCE(resolucao, ?) WHERE id=? AND resolvido_em IS NULL",
                          (agora(), st, r["id"]))
                feitos += st == "entrou"
    _op_cache["v"] = None
    return {"ok": not falta, "entraram": feitos, "erro": falta}


def _xbz_compras_loop():
    import time
    time.sleep(90)
    while True:
        try:
            r = xbz_compras_sincronizar()
            if not r.get("ok"):
                print("XBZ compras:", r.get("erro") or _xbz_api_status.get("erro"), flush=True)
        except Exception as e:
            print("XBZ compras:", str(e)[:150], flush=True)
        time.sleep(30 * 60)


def xbz_de(c, sku, cor=""):
    """Preco de custo e estoque da XBZ para o produto (e a cor, se achar)."""
    sku = sku_base(sku)
    rows = c.execute("SELECT * FROM xbz WHERE codigo=?", (sku,)).fetchall()
    if not rows:
        return None
    cn = cor_norm(cor)
    sel = [r for r in rows if cn and (r["cor"] == cn or cn.startswith(r["cor"] + " ") or r["cor"].startswith(cn))] or rows
    precos = [r["preco"] for r in sel if r["preco"]]
    return {"preco": round(sum(precos) / len(precos), 2) if precos else None, "estoque": sum(r["estoque"] or 0 for r in sel),
            "nome": rows[0]["nome"], "cor_xbz": ", ".join(sorted({r["cor"] for r in sel})),
            "reposicao": min((r["reposicao"] for r in sel if r["reposicao"] and not r["reposicao"].startswith("0001")), default="")}


def estoque():
    """Uma pagina so: saldo, saidas, cobertura, quanto comprar, preco e estoque da XBZ."""
    hoje = datetime.now(timezone.utc)
    d7, d15 = (hoje - timedelta(days=7)).isoformat(), (hoje - timedelta(days=15)).isoformat()
    with conn() as c:
        linhas = {}
        for r in c.execute("""SELECT sku, cor, SUM(qtd) saldo,
                 -SUM(CASE WHEN tipo IN ('ETIQUETA','CANCELADO') AND em>=? THEN qtd ELSE 0 END) s7,
                 -SUM(CASE WHEN tipo IN ('ETIQUETA','CANCELADO') AND em>=? THEN qtd ELSE 0 END) s15,
                 MAX(CASE WHEN tipo='CONTAGEM' THEN em END) contado,
                 SUM(CASE WHEN tipo IN ('CONTAGEM','DISTRIBUI','ENTRADA_NF','ENTRADA_XBZ','AJUSTE') THEN 1 ELSE 0 END) conhecido,
                 SUM(CASE WHEN tipo IN ('ENTRADA_NF','ENTRADA_XBZ') AND em>=? THEN qtd ELSE 0 END) e15
                 FROM estoque_mov GROUP BY sku, cor""", (d7, d15, d15)):
            linhas[(r["sku"], r["cor"])] = dict(r)
        pend = {}
        canc = _ids_cancelados(c)
        # reservado = etiquetas que ainda nao foram para a separacao (o material ainda esta na prateleira)
        for i in c.execute("SELECT * FROM itens WHERE status='AGUARDANDO' AND COALESCE(lote,'')<>'DEVOLUCAO' AND COALESCE(oculto,0)=0"):
            if i["id"] in canc or _ja_baixado(c, i["id"]):
                continue
            for sku, cor, q in _pecas_do_item(dict(i)):
                if not sku or sku.startswith("("):
                    continue
                k = _peca_no_estoque(c, dict(i), sku, cor)
                if k[0] and not k[0].startswith("("):
                    pend[k] = pend.get(k, 0) + q
        for k in pend:
            linhas.setdefault(k, {"sku": k[0], "cor": k[1], "saldo": 0, "s7": 0, "s15": 0, "contado": None,
                                  "conhecido": 0, "e15": 0})
        com_cor = {k[0] for k in linhas if k[1]}
        prat = {r[0]: r[1] for r in c.execute("SELECT sku, prateleira FROM prateleiras")}
        st = {(r[0], r[1]): (r[2], r[3]) for r in c.execute("SELECT sku, cor, status, em FROM sku_status")}
        out, ocultos = [], []
        try:
            PV = previsao()
            PV = PV["itens"] if PV.get("ok") and PV.get("historico_dias", 0) >= 14 else {}
        except Exception as e:
            print("previsao:", e, flush=True)
            PV = {}
        for (sku, cor), r in linhas.items():
            sem_cor = cor == "" and sku in com_cor  # produto com cores: entrada sem cor ainda precisa ser distribuida
            stt = st.get((sku, cor))
            # EXCLUIDO some de tudo; ARQUIVADO (sem estoque aqui nem no fornecedor) volta sozinho se entrar material
            if stt and (stt[0] == "EXCLUIDO" or (r["saldo"] or 0) <= 0):
                ocultos.append({"sku": sku, "cor": cor or "PADRAO", "status": stt[0], "em": stt[1], "fisico": r["saldo"] or 0})
                continue
            media = (r["s15"] or 0) / 15
            fisico = r["saldo"] or 0
            saldo = fisico - pend.get((sku, cor), 0)  # disponivel
            x = xbz_de(c, sku, cor) or {}
            cu = preco_xbz(c, sku, cor)
            conhecido = bool(r["conhecido"])
            comprar = max(0, round(media * ESTOQUE_DIAS_COMPRA - saldo)) if not sem_cor and conhecido else 0
            pv_ = PV.get((sku, cor))
            prev6 = None
            if pv_:   # previsao pelos pedidos (todas as lojas) e pelo dia da semana
                prev6 = previsao_dias(pv_, ESTOQUE_DIAS_COMPRA)
                media = prev6 / ESTOQUE_DIAS_COMPRA
                comprar = max(0, round(prev6 - saldo)) if not sem_cor and conhecido else 0
            out.append({"sku": sku, "cor": cor or ("(cor a definir)" if sem_cor else "PADRAO"), "sem_cor": sem_cor,
                        "previsao": round(prev6) if prev6 is not None else None,
                        "vendido_7d": pv_["ultimos_7d"] if pv_ else None,
                        "saldo": round(saldo, 2),
                        "fisico": round(fisico, 2), "prateleira": prat.get(sku) or prat.get(re.sub(r"[PMG]$", "", sku), ""),
                        "pendente_hoje": pend.get((sku, cor), 0), "saidas_7d": r["s7"] or 0, "saidas_15d": r["s15"] or 0,
                        "media_dia": round(media, 1),
                        "dias": (previsao_cobertura(pv_, saldo) if pv_ else
                                 (round(saldo / media, 1) if media > 0 and saldo > 0 else (0 if saldo <= 0 else None))),
                        "comprar": comprar, "custo": cu, "valor_compra": round((cu or 0) * comprar, 2),
                        "contado": r["contado"], "xbz_estoque": x.get("estoque"), "xbz_cor": x.get("cor_xbz", ""),
                        "xbz_reposicao": x.get("reposicao", ""), "nome": x.get("nome", ""),
                        "conhecido": conhecido,
                        "alerta": ("FALTA CONTAR" if not conhecido else "") or
                                  ("ESTRATEGICO: ACABANDO" if sku in ESTRATEGICOS and media > 0 and saldo < media * 7 else "") or
                                  ("SEM ESTOQUE" if saldo <= 0 and not sem_cor else "") or
                                  ("XBZ ACABOU" if x and x.get("estoque") == 0 else "") or
                                  ("XBZ ABAIXO DE 200" if x and x.get("estoque") is not None and x["estoque"] < 200 else "")})
    out.sort(key=lambda d: (d["sem_cor"], not d["conhecido"], -(d["comprar"] or 0), d["saldo"], d["sku"]))
    with conn() as c:
        alertas_xbz = [dict(r) for r in c.execute("SELECT * FROM xbz_alertas ORDER BY id DESC LIMIT 40")]
    return {"itens": out, "ocultos": sorted(ocultos, key=lambda d: (d["sku"], d["cor"])),
            "dias_compra": ESTOQUE_DIAS_COMPRA, "xbz": _xbz_status, "alertas_xbz": alertas_xbz,
            "total_compra": round(sum(d["valor_compra"] for d in out), 2)}


ESTOQUE_PRAZO_XBZ = int(os.environ.get("ESTOQUE_PRAZO_XBZ", "2"))
ESTOQUE_LIMITE_SEMANA = float(os.environ.get("ESTOQUE_LIMITE_SEMANA", "50000"))


def _habito_compra(c, sku):
    """Aprende com as notas da XBZ como voces costumam comprar esse produto: de quantos em quantos dias e em qual multiplo."""
    por_data = {}
    for r in c.execute("SELECT data, cprod, qtd FROM compras ORDER BY data"):
        if codigo_xbz_nf(r["cprod"]) == sku:
            por_data[r["data"]] = por_data.get(r["data"], 0) + (r["qtd"] or 0)
    rows = [{"data": d_, "q": q_} for d_, q_ in sorted(por_data.items())]
    datas = [datetime.fromisoformat(r["data"]) for r in rows]
    gaps = sorted((b - a).days for a, b in zip(datas, datas[1:]) if (b - a).days > 0)
    ciclo = max(3, min(14, gaps[len(gaps) // 2])) if gaps else ESTOQUE_DIAS_COMPRA
    qs = [int(r["q"]) for r in rows if r["q"]]
    mult = 1
    for m in (100, 50, 25, 20, 10, 5):
        if qs and sum(1 for q in qs if q % m == 0) >= max(1, 0.7 * len(qs)):
            mult = m
            break
    return {"ciclo": ciclo, "multiplo": mult, "compras": len(qs), "lote_tipico": sorted(qs)[len(qs) // 2] if qs else None}


def preco_xbz(c, sku, cor=""):
    """Preco de custo: API da XBZ primeiro; senao o preco conhecido (agente antigo); senao o custo cadastrado."""
    x = xbz_de(c, sku, cor) or {}
    if x.get("preco"):
        return x["preco"]
    if sku in PRECOS_REF:
        return PRECOS_REF[sku]
    base = re.sub(r"[PMG]$", "", sku)
    return PRECOS_REF.get(base) or custo_de(c, sku)



# ================================================================== PREVISAO DE VENDAS (para estoque e compra)
# Historico dos pedidos de TODAS as lojas da Shopee (lido pela API, so leitura) + TikTok (etiquetas da Central).
# Previsao por produto/cor = ritmo recente (mais peso nos ultimos dias) x o jeito de cada DIA DA SEMANA vender.
VENDAS_HIST_DIAS = int(os.environ.get("VENDAS_HIST_DIAS", "120"))
VENDAS_JANELA = 56            # 8 semanas para o dia da semana e o ritmo
VENDAS_CANCEL = ("CANCELLED", "IN_CANCEL", "UNPAID")
_vendas_status = {"rodando": False, "em": "", "erro": "", "pedidos": 0}
_prev_cache = {"em": 0, "v": None}
DIAS_PT = ["seg", "ter", "qua", "qui", "sex", "sáb", "dom"]


def shopee_historico_vendas(dias=None):
    """Le os pedidos criados nos ultimos 'dias' de cada loja (so os itens: SKU, variacao, quantidade e status).
    Continua de onde parou; os ultimos 3 dias sao sempre relidos (status muda: cancelado etc.)."""
    import time
    if _vendas_status["rodando"]:
        return {"ok": False, "erro": "ja esta lendo"}
    _vendas_status.update(rodando=True, erro="")
    dias = dias or VENDAS_HIST_DIAS
    total, erros = 0, []
    try:
        with conn() as c:
            lojas = [r[0] for r in c.execute("SELECT shop_id FROM shopee_lojas ORDER BY shop_id")]
        agora_s = int(time.time())
        alvo = agora_s - dias * 86400
        for sid in lojas:
            try:
                loja = _shopee_token_ok(sid)
                if not loja:
                    continue
                chave = f"vendas_hist_ate_{sid}"
                with conn() as c:
                    r = c.execute("SELECT valor FROM meta WHERE chave=?", (chave,)).fetchone()
                feito_ate = int(r[0]) if r and str(r[0]).isdigit() else agora_s
                janelas = [(agora_s - 3 * 86400, agora_s)]
                t1 = min(feito_ate, agora_s - 3 * 86400)
                while t1 > alvo:
                    t0 = max(alvo, t1 - 15 * 86400 + 60)
                    janelas.append((t0, t1))
                    t1 = t0
                for t0, t1 in janelas:
                    sns, cursor = [], ""
                    for _ in range(200):
                        res = _shopee_http("GET", "/api/v2/order/get_order_list", loja=loja,
                                           params={"time_range_field": "create_time", "time_from": t0, "time_to": t1,
                                                   "page_size": 100, "cursor": cursor})
                        rr = res.get("response") or {}
                        sns += [o["order_sn"] for o in (rr.get("order_list") or []) if o.get("order_sn")]
                        if not rr.get("more"):
                            break
                        cursor = rr.get("next_cursor") or ""
                        time.sleep(0.1)
                    linhas = []
                    for k in range(0, len(sns), 50):
                        res = _shopee_http("GET", "/api/v2/order/get_order_detail", loja=loja,
                                           params={"order_sn_list": ",".join(sns[k:k + 50]), "response_optional_fields": "item_list"})
                        for o in (res.get("response") or {}).get("order_list") or []:
                            itens = [{"sku": (i.get("model_sku") or i.get("item_sku") or "").strip(), "nome": (i.get("item_name") or "")[:80],
                                      "var": (i.get("model_name") or "")[:60], "qtd": int(i.get("model_quantity_purchased") or 1)}
                                     for i in (o.get("item_list") or [])]
                            linhas.append((norm(o.get("order_sn")), int(sid), loja.get("nome") or str(sid), int(o.get("create_time") or 0),
                                           o.get("order_status") or "", json.dumps(itens, ensure_ascii=False)))
                        time.sleep(0.1)
                    with _lock, conn() as c:
                        c.executemany("""INSERT INTO vendas_hist(order_sn, shop_id, loja, criado, status, itens) VALUES(?,?,?,?,?,?)
                                         ON CONFLICT(order_sn) DO UPDATE SET status=excluded.status, itens=excluded.itens""", linhas)
                        if t0 < feito_ate and t1 <= agora_s - 3 * 86400 + 1:
                            c.execute("INSERT OR REPLACE INTO meta(chave, valor) VALUES(?,?)", (chave, str(t0)))
                    total += len(linhas)
            except Exception as e:
                erros.append(f"{sid}: {e}"[:200])
    finally:
        _vendas_status.update(rodando=False, em=datetime.now(BR).strftime("%d/%m %H:%M"), erro=" | ".join(erros), pedidos=total)
        _prev_cache["v"] = None
    return {"ok": not erros, "pedidos_lidos": total, "erros": erros}


def _vendas_loop():
    import time
    time.sleep(240)
    while True:
        try:
            shopee_historico_vendas()
        except Exception as e:
            _vendas_status["erro"] = str(e)[:200]
        time.sleep(6 * 3600)


def _dia_br(ts):
    return datetime.fromtimestamp(ts, BR).date()


def _vendas_diarias(c, dias=VENDAS_JANELA + 7):
    """{(sku, cor): {data: qtd}}, total por dia, primeiro dia com dado e o que nao deu para ligar ao estoque."""
    hoje = datetime.now(BR).date()
    t0 = int(datetime.combine(hoje - timedelta(days=dias), datetime.min.time(), BR).timestamp())
    _conhecidos_cache["v"] = None   # le de novo a lista de produtos/cores do estoque
    conhecidos = _estoque_conhecido(c)
    pedidos = {}
    for r in c.execute("SELECT order_sn, criado, status, itens FROM shopee_pedidos WHERE criado>=?", (t0,)):
        pedidos[r[0]] = (r[1], r[2], r[3])
    for r in c.execute("SELECT order_sn, criado, status, itens FROM vendas_hist WHERE criado>=?", (t0,)):
        pedidos[r[0]] = (r[1], r[2], r[3])
    cache, daily, total, sem_par = {}, {}, {}, {}
    primeiro = None
    for sn, (criado, st, itens) in pedidos.items():
        if not criado or (st or "") in VENDAS_CANCEL:
            continue
        d = _dia_br(criado)
        primeiro = d if primeiro is None or d < primeiro else primeiro
        for i in json.loads(itens or "[]"):
            ck = (i.get("sku") or "", i.get("var") or "", i.get("nome") or "")
            if ck not in cache:
                st_, var, nome = ck
                sku = _sku_do_texto(st_, conhecidos, f"{var} {nome}") or _sku_do_texto(nome, conhecidos, var)
                if not sku or sku not in conhecidos:
                    cache[ck] = None
                else:
                    cor = _cor_do_modelo(sku, [var, st_.split("-", 1)[1] if "-" in st_ else ""], conhecidos)
                    cache[ck] = None if cor is None else (sku, cor, _kit_mult(st_, var))
            m = cache[ck]
            q = int(i.get("qtd") or 1)
            if not m:
                sem_par[ck[0] or ck[2]] = sem_par.get(ck[0] or ck[2], 0) + q
                continue
            k = (m[0], m[1])
            daily.setdefault(k, {})
            daily[k][d] = daily[k].get(d, 0) + q * m[2]
            total[d] = total.get(d, 0) + q * m[2]
    # TikTok: ainda sem API -> etiquetas da Central (dia em que a etiqueta entrou)
    vistos = set()
    ini_iso = datetime.combine(hoje - timedelta(days=dias), datetime.min.time(), BR).astimezone(timezone.utc).isoformat()
    for it in c.execute("""SELECT * FROM itens WHERE criado_em>=? AND COALESCE(lote,'')<>'DEVOLUCAO' AND COALESCE(oculto,0)<>1
                           AND (UPPER(COALESCE(canal,'')) LIKE '%TIKTOK%')""", (ini_iso,)):
        if norm(it["pedido"]) in pedidos or it["id"] in vistos:
            continue
        vistos.add(it["id"])
        d = datetime.fromisoformat(it["criado_em"]).astimezone(BR).date()
        for s_, c_, q in _pecas_do_item(dict(it)):
            if not s_ or s_.startswith("("):
                continue
            k = _peca_no_estoque(c, dict(it), s_, c_)
            daily.setdefault(k, {})
            daily[k][d] = daily[k].get(d, 0) + q
            total[d] = total.get(d, 0) + q
    return daily, total, primeiro, sem_par


def _fatores_semana(serie, dias_validos):
    """Peso de cada dia da semana (seg..dom) = media do dia / media geral."""
    soma, n = [0.0] * 7, [0] * 7
    for d in dias_validos:
        soma[d.weekday()] += serie.get(d, 0)
        n[d.weekday()] += 1
    medias = [soma[w] / n[w] if n[w] else None for w in range(7)]
    validas = [m for m in medias if m is not None]
    geral = sum(validas) / len(validas) if validas else 0
    if not geral:
        return [1.0] * 7
    return [min(1.8, max(0.4, (m / geral) if m is not None else 1.0)) for m in medias]


def _nivel(serie, dias_validos, f, ate):
    """(ritmo, tendencia por dia) 'sem o efeito do dia da semana'. Ritmo: mais peso nos dias recentes (meia-vida de
    5 dias, ate 21 dias). Tendencia: ultimos 7 dias contra os 14 anteriores (subindo ou caindo), com freio."""
    num = den = 0.0
    rec, ant = [], []
    for d in dias_validos:
        if d >= ate:
            continue
        idade = (ate - d).days
        x = serie.get(d, 0) / f[d.weekday()]
        if idade <= 21:
            w = 0.5 ** (idade / 5)
            num += w * x
            den += w
        if idade <= 7:
            rec.append(x)
        elif idade <= 21:
            ant.append(x)
    L = num / den if den else 0.0
    t = 0.0
    if len(rec) >= 5 and len(ant) >= 7:
        t = (sum(rec) / len(rec) - sum(ant) / len(ant)) / 10.5
    return L, t


def _prev_dia(L, t, f, d, j):
    """Venda prevista no dia d (j dias a frente): ritmo + tendencia amortecida, vezes o peso do dia da semana."""
    amort = sum(0.9 ** k for k in range(1, j + 1))
    base = min(max(L + t * amort, 0.6 * L), 1.6 * L) if L > 0 else max(0.0, t * amort)
    return max(0.0, base) * f[d.weekday()]


def previsao(dias=None):
    """Previsao de venda de cada produto/cor para os proximos 'dias' (a partir de amanha) + o que ainda vende hoje."""
    import time
    dias = int(dias or ESTOQUE_DIAS_COMPRA)
    if _prev_cache["v"] is not None and time.time() - _prev_cache["em"] < 600 and _prev_cache["v"]["dias"] == dias:
        return _prev_cache["v"]
    with conn() as c:
        daily, total, primeiro, sem_par = _vendas_diarias(c)
    hoje = datetime.now(BR).date()
    if not primeiro:
        v = {"ok": False, "dias": dias, "itens": {}, "historico_dias": 0, "erro": "sem historico de vendas ainda"}
        _prev_cache.update(v=v, em=time.time())
        return v
    ini = max(primeiro, hoje - timedelta(days=VENDAS_JANELA))
    dias_validos = [ini + timedelta(days=k) for k in range((hoje - ini).days)]   # dias completos (ate ontem)
    fg = _fatores_semana(total, dias_validos) if len(dias_validos) >= 14 else [1.0] * 7
    itens = {}
    for k, serie in daily.items():
        prim_k = min(serie)
        dv = [d for d in dias_validos if d >= prim_k]
        if not dv:
            dv = []
        vol = sum(serie.get(d, 0) for d in dv)
        if len(dv) >= 21 and vol >= 60:   # produto com bastante venda: usa o proprio jeito de vender na semana
            fp = _fatores_semana(serie, dv)
            a = min(1.0, vol / 300)
            f = [a * fp[w] + (1 - a) * fg[w] for w in range(7)]
        else:
            f = fg
        L, t = _nivel(serie, dv, f, hoje)
        vendido_hoje = serie.get(hoje, 0)
        resto_hoje = max(0.0, _prev_dia(L, t, f, hoje, 0) - vendido_hoje)
        prox = [(hoje + timedelta(days=j), _prev_dia(L, t, f, hoje + timedelta(days=j), j)) for j in range(1, dias + 1)]
        tot = resto_hoje + sum(q for _, q in prox)
        u7 = sum(serie.get(hoje - timedelta(days=j), 0) for j in range(1, 8))
        itens[k] = {"nivel": L, "tendencia": t, "fatores": f, "resto_hoje": resto_hoje, "vendido_hoje": vendido_hoje,
                    "proximos": [(d.isoformat(), round(q, 1)) for d, q in prox], "total": tot, "media_dia": tot / max(1, dias),
                    "ultimos_7d": u7, "dias_hist": len(dv)}
    v = {"ok": True, "dias": dias, "itens": itens, "fatores_gerais": fg, "historico_dias": len(dias_validos),
         "desde": ini.isoformat(), "sem_par": sorted(sem_par.items(), key=lambda x: -x[1])[:30],
         "status": dict(_vendas_status)}
    _prev_cache.update(v=v, em=time.time())
    return v


def previsao_dias(p, dias_a_frente):
    """Quanto vende nos proximos N dias (inclui o resto de hoje), usando o ritmo e o dia da semana."""
    hoje = datetime.now(BR).date()
    return p["resto_hoje"] + sum(_prev_dia(p["nivel"], p.get("tendencia", 0), p["fatores"], hoje + timedelta(days=j), j)
                                 for j in range(1, int(dias_a_frente) + 1))


def previsao_cobertura(p, disponivel, max_dias=90):
    """Em quantos dias o disponivel acaba, no ritmo previsto (dia a dia, respeitando o dia da semana)."""
    if disponivel <= 0:
        return 0.0
    hoje = datetime.now(BR).date()
    resto = disponivel - p["resto_hoje"]
    if resto <= 0:
        return 0.0
    for j in range(1, max_dias + 1):
        q = _prev_dia(p["nivel"], p.get("tendencia", 0), p["fatores"], hoje + timedelta(days=j), j)
        if q <= 0:
            continue
        if resto <= q:
            return round(j - 1 + resto / q, 1)
        resto -= q
    return None


def previsao_teste(semanas=3, dias=6):
    """Confere a previsao no passado: para cada dia das ultimas 'semanas', preve os 'dias' seguintes so com o que se
    sabia ate ali e compara com o que vendeu. Mostra o erro da previsao nova e da media simples (15 dias)."""
    with conn() as c:
        daily, total, primeiro, _ = _vendas_diarias(c, dias=VENDAS_JANELA + semanas * 7 + dias + 7)
    hoje = datetime.now(BR).date()
    if not primeiro:
        return {"ok": False, "erro": "sem historico"}
    top = sorted(daily, key=lambda k: -sum(daily[k].values()))[:40]
    err_n = err_v = real_t = 0.0
    dia_n = dia_v = dia_real = 0.0
    casos = 0
    for a in range(semanas * 7 + dias, dias, -1):
        ancora = hoje - timedelta(days=a)
        ini = max(primeiro, ancora - timedelta(days=VENDAS_JANELA))
        dv = [ini + timedelta(days=k) for k in range((ancora - ini).days)]
        if len(dv) < 14:
            continue
        fg = _fatores_semana(total, dv)
        for k in top:
            s = daily[k]
            dvk = [d for d in dv if d >= min(s)]
            if len(dvk) < 7:
                continue
            L, t = _nivel(s, dvk, fg, ancora)
            pd_ = [_prev_dia(L, t, fg, ancora + timedelta(days=j), j) for j in range(dias)]
            m15 = sum(s.get(ancora - timedelta(days=j), 0) for j in range(1, 16)) / 15
            rd = [s.get(ancora + timedelta(days=j), 0) for j in range(dias)]
            err_n += abs(sum(pd_) - sum(rd))
            err_v += abs(m15 * dias - sum(rd))
            real_t += sum(rd)
            dia_n += sum(abs(a - b) for a, b in zip(pd_, rd))
            dia_v += sum(abs(m15 - b) for b in rd)
            dia_real += sum(rd)
            casos += 1
    if not real_t:
        return {"ok": False, "erro": "pouco historico para testar"}
    return {"ok": True, "casos": casos, "erro_previsao_nova_%": round(100 * err_n / real_t, 1),
            "erro_media_simples_%": round(100 * err_v / real_t, 1),
            "erro_por_dia_nova_%": round(100 * dia_n / dia_real, 1), "erro_por_dia_media_simples_%": round(100 * dia_v / dia_real, 1),
            "explicacao": f"Para cada um dos ultimos {semanas * 7} dias, previ os {dias} dias seguintes so com o que se sabia ate ali, "
                          f"nos {len(top)} produtos que mais vendem, e comparei com o que vendeu de verdade."}


def sugestao_compra(dias=None):
    """Projecao de compra para a XBZ: venda dos ultimos 30 dias com mais peso na ultima semana (sem extrapolar pico),
    estoque fisico, reservado nas etiquetas, prazo da XBZ, estoque e preco da XBZ; arredonda no multiplo que voces
    costumam pedir e aplica o fator aprendido com os pedidos confirmados. Nunca envia pedido sozinho."""
    import math
    agora_ = datetime.now(timezone.utc)
    d7, d30 = (agora_ - timedelta(days=7)).isoformat(), (agora_ - timedelta(days=30)).isoformat()
    est = {(i["sku"], "" if i["cor"] in ("PADRAO", "(cor a definir)") else i["cor"]): i for i in estoque()["itens"]}
    out, sem_contagem, fora = [], [], []
    with conn() as c:
        prim = c.execute("SELECT MIN(em) FROM estoque_mov WHERE tipo='ETIQUETA'").fetchone()[0]
        dias_hist = max(1, min(30, (agora_ - datetime.fromisoformat(prim)).days + 1)) if prim else 1
        vendas = {}
        for r in c.execute("""SELECT sku, cor, -SUM(CASE WHEN em>=? THEN qtd ELSE 0 END) v7, -SUM(qtd) v30 FROM estoque_mov
                              WHERE tipo='ETIQUETA' AND em>=? GROUP BY sku, cor""", (d7, d30)):
            vendas[(r["sku"], r["cor"])] = (r["v7"] or 0, r["v30"] or 0)
        fatores = {r["sku"]: r["fator"] for r in c.execute("SELECT sku, fator FROM compra_aprendizado")}
        try:
            PV = previsao()
            PV = PV["itens"] if PV.get("ok") and PV.get("historico_dias", 0) >= 14 else {}
        except Exception:
            PV = {}
        vendas.update({k: vendas.get(k, (0, 0)) for k in PV})
        habitos = {}
        chaves = set(vendas) | {k for k, v in est.items() if v["pendente_hoje"]} | \
            {k for k, v in est.items() if k[0] in ESTRATEGICOS and v.get("conhecido")}
        exc = {(r[0], r[1]) for r in c.execute("SELECT sku, cor FROM sku_status WHERE status='EXCLUIDO'")}
        for sku, cor in sorted(chaves):
            if (sku, cor) in exc or (sku, "") in exc:
                continue
            if not cor and (est.get((sku, ""), {}).get("sem_cor") or not est.get((sku, ""), {}).get("conhecido")):
                continue
            v7, v30 = vendas.get((sku, cor), (0, 0))
            m7, m30 = v7 / 7, v30 / max(7, dias_hist)
            # aceleracao clara pesa mais; pico isolado nao e extrapolado (no maximo o dobro da media do mes)
            media = m30 + 0.6 * (min(m7, 2 * m30 if m30 else m7) - m30) if m7 > m30 else 0.5 * m7 + 0.5 * m30
            h = habitos.setdefault(sku, _habito_compra(c, sku))
            horizonte = (dias if dias else h["ciclo"]) + ESTOQUE_PRAZO_XBZ
            e = est.get((sku, cor), {})
            reservado = e.get("pendente_hoje", 0)
            fisico = e.get("fisico", 0)
            pv_ = PV.get((sku, cor))
            if pv_:   # previsao pelos pedidos de todas as lojas, respeitando o dia da semana
                media = previsao_dias(pv_, horizonte) / max(1, horizonte)
                alvo = previsao_dias(pv_, horizonte) * fatores.get(sku, 1.0)
            else:
                alvo = media * horizonte * fatores.get(sku, 1.0)
            precisa = alvo + reservado - fisico  # estoque negativo aumenta a compra (falta fisica)
            x = xbz_de(c, sku, cor) or {}
            preco = preco_xbz(c, sku, cor)
            linha = {"sku": sku, "cor": cor, "nome": x.get("nome", e.get("nome", "")), "media_dia": round(media, 1),
                     "na_prateleira": fisico, "reservado": reservado, "ciclo_dias": h["ciclo"], "multiplo": h["multiplo"],
                     "projecao": round(alvo), "fator": round(fatores.get(sku, 1.0), 2), "preco": preco,
                     "xbz_estoque": x.get("estoque"), "estrategico": sku in ESTRATEGICOS,
                     "disponivel": round(fisico - reservado), "falta_agora": max(0, round(reservado - fisico))}
            # quando acaba (no ritmo de venda atual), contando o que ja esta reservado nas etiquetas
            if pv_ and fisico - reservado > 0:
                dd = previsao_cobertura(pv_, fisico - reservado)
                dd = 90.0 if dd is None else dd
                linha["acaba_dias"] = round(dd, 1)
                linha["acaba_em"] = (datetime.now(BR) + timedelta(days=dd)).strftime("%d/%m") if dd > 0 else "JÁ FALTA"
            elif media > 0:
                dd = max(0.0, (fisico - reservado) / media)
                linha["acaba_dias"] = round(dd, 1)
                linha["acaba_em"] = (datetime.now(BR) + timedelta(days=dd)).strftime("%d/%m") if dd > 0 else "JÁ FALTA"
            elif fisico - reservado <= 0 and reservado > 0:
                linha["acaba_dias"], linha["acaba_em"] = 0, "JÁ FALTA"
            if (sku, cor) in XBZ_INDISPONIVEL or (sku, "") in XBZ_INDISPONIVEL or x.get("estoque") == 0:
                if precisa > 0:
                    fora.append({**linha, "motivo": "fornecedor sem estoque", "qtd": math.ceil(precisa)})
                continue
            if (sku, cor) in BAIXA_DEMANDA or (sku, "") in BAIXA_DEMANDA:
                if precisa > 0:
                    fora.append({**linha, "motivo": "demanda baixa (so com nova evidencia)", "qtd": math.ceil(precisa)})
                continue
            if not e.get("conhecido"):
                linha["qtd"] = math.ceil(alvo + reservado)
                if linha["qtd"] > 0:
                    sem_contagem.append(linha)
                continue
            if precisa <= 0:
                continue
            q = math.ceil(precisa / h["multiplo"]) * h["multiplo"]
            if x.get("estoque") is not None and q > x["estoque"]:
                linha["aviso"] = f"XBZ so tem {x['estoque']}"
                q = x["estoque"]
            if q <= 0:
                continue
            linha["qtd"] = q
            linha["total"] = round((preco or 0) * q, 2)
            linha["critico"] = fisico - reservado <= 0 or (media > 0 and (fisico - reservado) / media < 2)
            out.append(linha)
    out.sort(key=lambda l: (not l.get("critico"), l.get("acaba_dias", 999), l["sku"], l["cor"]))
    total = round(sum(l.get("total", 0) for l in out), 2)
    return {"itens": out, "sem_contagem": sem_contagem, "fora": fora, "total": total,
            "falta_agora": sum(l.get("falta_agora", 0) for l in out),
            "unidades": sum(l["qtd"] for l in out), "criticos": sum(1 for l in out if l.get("critico")),
            "negativos": sum(1 for v in est.values() if v.get("fisico", 0) < 0 and not v.get("sem_cor")),
            "limite_semana": ESTOQUE_LIMITE_SEMANA, "acima_limite": total > ESTOQUE_LIMITE_SEMANA,
            "prazo_xbz": ESTOQUE_PRAZO_XBZ, "dias_historico": dias_hist, "dias": dias}


def confirmar_pedido_xbz(itens):
    """Voces confirmam o que pediram de verdade; o sistema aprende a diferenca (fator por produto) para a proxima vez."""
    with _lock, conn() as c:
        por_sku = {}
        for it in itens:
            sug, q = float(it.get("sugerido") or 0), float(it.get("qtd") or 0)
            a = por_sku.setdefault(estoque_chave(it.get("sku"))[0], [0.0, 0.0])
            a[0] += sug; a[1] += q
        for sku, (sug, q) in por_sku.items():
            if sug <= 0:
                continue
            r = c.execute("SELECT fator, pedidos FROM compra_aprendizado WHERE sku=?", (sku,)).fetchone()
            f, n = (r[0], r[1]) if r else (1.0, 0)
            f = max(0.3, min(3.0, 0.7 * f + 0.3 * (q / sug)))
            c.execute("INSERT OR REPLACE INTO compra_aprendizado VALUES(?,?,?,?)", (sku, f, n + 1, agora()))
        total = sum(float(i.get("qtd") or 0) * float(i.get("preco") or 0) for i in itens)
        c.execute("INSERT INTO pedidos_xbz(em,itens,total) VALUES(?,?,?)", (agora(), json.dumps(itens, ensure_ascii=False), total))
    return {"ok": True}


def ler_nfe(xml):
    """NF-e (XML) -> nf, data, conta (destinatario) e itens. Aceita nfeProc ou NFe."""
    import xml.etree.ElementTree as ET
    raiz = ET.fromstring(xml)
    def acha(no, nome):
        return next((e for e in no.iter() if e.tag.split("}")[-1] == nome), None)
    def txt(no, nome):
        e = acha(no, nome) if no is not None else None
        return (e.text or "").strip() if e is not None else ""
    inf = acha(raiz, "infNFe")
    if inf is None:
        return None
    chave = (inf.get("Id") or "").replace("NFe", "")
    ide, emit, dest = acha(inf, "ide"), acha(inf, "emit"), acha(inf, "dest")
    data = (txt(ide, "dhEmi") or txt(ide, "dEmi"))[:10]
    itens = []
    for det in [e for e in inf.iter() if e.tag.split("}")[-1] == "det"]:
        pr = acha(det, "prod")
        f = lambda n: float(txt(pr, n) or 0)
        itens.append({"item": det.get("nItem") or str(len(itens) + 1), "cprod": txt(pr, "cProd").upper(),
                      "descricao": txt(pr, "xProd"), "qtd": f("qCom"), "unit": f("vUnCom"), "total": f("vProd")})
    return {"chave": chave, "nf": txt(ide, "nNF"), "data": data, "emitente": txt(emit, "xNome"),
            "conta": txt(dest, "xNome") or txt(dest, "CNPJ") or txt(dest, "CPF"), "itens": itens}


ESTOQUE_NF_DESDE = os.environ.get("ESTOQUE_NF_DESDE", "2026-10-06")   # contagem de 05/10 a tarde ja inclui o que chegou antes


def _nf_entra_no_estoque(data_nf):
    """Nota fiscal so entra no estoque se a leitura das retiradas da Minha XBZ (com cor) NAO estiver ligada;
    e so a partir do dia seguinte a contagem."""
    if xbz_site_configurado() or xbz_api_configurada():
        return False   # o estoque entra pelas retiradas/compras da XBZ (com cor); a nota fica so para custo
    return (data_nf or "9999") >= ESTOQUE_NF_DESDE


def corrigir_nf_antes_da_contagem():
    """Uma vez: tira do estoque a entrada de notas ANTERIORES ao corte que foi lancada DEPOIS da contagem daquele
    produto (a contagem ja tinha contado essas pecas: contaria 2 vezes). Guarda o que fez no log."""
    with _lock, conn() as c:
        if c.execute("SELECT 1 FROM meta WHERE chave=?", ("nf_corte_" + ESTOQUE_NF_DESDE,)).fetchone():
            return []
        tirados = []
        for m in c.execute("""SELECT m.id, m.sku, m.qtd, m.em, m.ref, p.data FROM estoque_mov m
                              JOIN compras p ON 'NF|' || p.chave = m.ref WHERE m.tipo='ENTRADA_NF' AND p.data < ?""",
                           (ESTOQUE_NF_DESDE,)).fetchall():
            cont = c.execute("SELECT MAX(em) FROM estoque_mov WHERE sku=? AND tipo='CONTAGEM'", (m["sku"],)).fetchone()[0]
            if cont and m["em"] >= cont:
                c.execute("DELETE FROM estoque_mov WHERE id=?", (m["id"],))
                tirados.append({"sku": m["sku"], "qtd": m["qtd"], "nf_data": m["data"], "lancado": m["em"]})
        c.execute("INSERT OR REPLACE INTO meta(chave, valor) VALUES(?,?)",
                  ("nf_corte_" + ESTOQUE_NF_DESDE, json.dumps({"em": agora(), "tirados": tirados}, ensure_ascii=False)))
    if tirados:
        print(f"Estoque: {len(tirados)} entrada(s) de nota anterior a contagem retirada(s)", flush=True)
    return tirados


def importar_nfe(xml):
    """Guarda a compra (sem duplicar) e atualiza o custo de cada produto com o ultimo preco pago na XBZ."""
    n = ler_nfe(xml)
    if not n or not n["itens"]:
        return {"ok": False, "erro": "nao e uma NF-e valida"}
    novos = 0
    with _lock, conn() as c:
        for it in n["itens"]:
            r = c.execute("""INSERT OR IGNORE INTO compras(chave,nf,data,conta,cprod,descricao,qtd,unit,total,importado_em)
                             VALUES(?,?,?,?,?,?,?,?,?,?)""", (f"{n['chave'] or n['nf']}|{it['item']}", n["nf"], n["data"],
                             n["conta"], it["cprod"], it["descricao"], it["qtd"], it["unit"], it["total"], agora()))
            novos += r.rowcount
            if it["cprod"] and it["qtd"] and _nf_entra_no_estoque(n["data"]) \
                    and not any(x and x in (n["conta"] or "").upper() for x in ESTOQUE_IGNORAR):
                # entrada no estoque proprio (a nota nao traz a cor: entra como "cor a definir")
                c.execute("INSERT OR IGNORE INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                          (agora(), estoque_chave(codigo_xbz_nf(it["cprod"]))[0], "", it["qtd"], "ENTRADA_NF",
                           f"NF|{n['chave'] or n['nf']}|{it['item']}", f"NF {n['nf']} {n['conta']}"))
    return {"ok": True, "nf": n["nf"], "data": n["data"], "conta": n["conta"], "itens": len(n["itens"]), "novos": novos,
            "estoque": "entrou no estoque" if _nf_entra_no_estoque(n["data"]) else
                       ("so registrada: o estoque entra pelas retiradas da Minha XBZ (com cor)" if xbz_site_configurado()
                        else f"nota antes de {ESTOQUE_NF_DESDE}: so registrada (a contagem ja inclui)"),
            "total": round(sum(i["total"] for i in n["itens"]), 2)}


def _pecas_do_item(i):
    try:
        pec = json.loads(i.get("pecas") or "[]")
    except Exception:
        pec = []
    if pec:
        return [(str(p.get("sku") or "").upper(), str(p.get("cor") or ""), int(p.get("qtd") or 1)) for p in pec]
    skus = [x.strip().upper() for x in (i.get("sku") or "").split("+") if x.strip()] or ["(SEM SKU)"]
    q = int(i.get("qtd") or 1) if len(skus) == 1 else 1
    return [(s_, i.get("cor") or "", q) for s_ in skus]


def materiais(de, ate):
    """Produto por produto (SKU + cor): quanto subiu, separou, expediu, falta (bipado 'nao tem'), custo XBZ."""
    ini, _ = dia_utc(de)
    _, fim = dia_utc(ate)
    hoje = datetime.now(BR).strftime("%Y-%m-%d")
    with conn() as c:
        q = "SELECT * FROM itens WHERE COALESCE(oculto,0)=0 AND ((criado_em>=? AND criado_em<?)"
        if ate >= hoje:
            q += " OR status NOT IN ('EXPEDIDO','DEVOLVIDO')"
        itens = [dict(r) for r in c.execute(q + ") AND COALESCE(lote,'')<>'DEVOLUCAO'", (ini, fim))]
        prod, plat, lojas = {}, {}, {}
        for i in itens:
            g = grupo_envio(i)
            for sku, cor, qtd in _pecas_do_item(i):
                k = (sku, cor.upper())
                d = prod.setdefault(k, {"sku": sku, "cor": cor, "pedidos": 0, "unidades": 0, "aguardando": 0,
                                        "separadas": 0, "expedidas": 0, "devolvidas": 0, "falta": 0})
                d["pedidos"] += 1
                d["unidades"] += qtd
                st = i["status"]
                if st == "DEVOLVIDO":
                    d["devolvidas"] += qtd
                elif st == "EXPEDIDO":
                    d["expedidas"] += qtd
                elif st == "AGUARDANDO":
                    d["aguardando"] += qtd
                else:
                    d["separadas"] += qtd
                if i["falta_material"] and st not in ("EXPEDIDO", "DEVOLVIDO"):
                    d["falta"] += qtd
                cu = custo_de(c, sku) or 0
                for chave, mapa in ((g, plat), ((i.get("loja") or "").strip().upper() or ("TIKTOK" if g == "TIKTOK" else "SEM LOJA"), lojas)):
                    x = mapa.setdefault(chave, {"nome": chave, "unidades": 0, "custo": 0.0})
                    x["unidades"] += qtd
                    x["custo"] += cu * qtd
        lista = []
        for d in prod.values():
            cu = custo_de(c, d["sku"])
            d["custo_unit"] = cu
            d["custo_total"] = round((cu or 0) * d["unidades"], 2)
            lista.append(d)
    lista.sort(key=lambda d: (-d["unidades"], d["sku"]))
    arred = lambda m: sorted(({**v, "custo": round(v["custo"], 2)} for v in m.values()), key=lambda v: -v["unidades"])
    return {"de": de, "ate": ate, "produtos": lista, "comprar": [d for d in lista if d["falta"]],
            "por_plataforma": arred(plat), "por_loja": arred(lojas),
            "sem_custo": sorted({d["sku"] for d in lista if d["custo_unit"] is None})}


def compras(de, ate):
    with conn() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM compras WHERE data>=? AND data<=? ORDER BY data, nf", (de, ate))]
    por_prod, por_conta, nfs = {}, {}, {}
    for r in rows:
        p = por_prod.setdefault(r["cprod"], {"cprod": r["cprod"], "descricao": r["descricao"], "qtd": 0, "total": 0.0})
        p["qtd"] += r["qtd"]; p["total"] += r["total"]
        k = por_conta.setdefault(r["conta"] or "-", {"conta": r["conta"] or "-", "nfs": set(), "total": 0.0})
        k["nfs"].add(r["nf"]); k["total"] += r["total"]
        n = nfs.setdefault(r["nf"], {"nf": r["nf"], "data": r["data"], "conta": r["conta"], "itens": 0, "total": 0.0})
        n["itens"] += 1; n["total"] += r["total"]
    with conn() as c:
        for p in por_prod.values():
            p["total"] = round(p["total"], 2); p["unit_medio"] = round(p["total"] / p["qtd"], 2) if p["qtd"] else None
            p["codigo"] = codigo_xbz_nf(p["cprod"])
            x = xbz_de(c, p["codigo"]) or {}
            p["nome_xbz"] = x.get("nome", "")
            p["preco_xbz"] = x.get("preco")
            p["custo_real"] = round(x["preco"] * p["qtd"], 2) if x.get("preco") else None
    contas = [{"conta": k["conta"], "nfs": len(k["nfs"]), "total": round(k["total"], 2)} for k in por_conta.values()]
    return {"de": de, "ate": ate, "total": round(sum(r["total"] for r in rows), 2),
            "total_real": round(sum(p["custo_real"] or 0 for p in por_prod.values()), 2),
            "por_produto": sorted(por_prod.values(), key=lambda p: -p["total"]),
            "por_conta": sorted(contas, key=lambda k: -k["total"]),
            "notas": sorted(({**n, "total": round(n["total"], 2)} for n in nfs.values()), key=lambda n: n["data"], reverse=True)}


# ------------------------------------------------------------------ etiquetas por e-mail
# Mande (ou encaminhe) o PDF de etiquetas do UpSeller para o e-mail da Central: ela le e inclui sozinha.
EMAIL_USUARIO = os.environ.get("EMAIL_USUARIO", "").strip()
EMAIL_SENHA = os.environ.get("EMAIL_SENHA", "").replace(" ", "")
EMAIL_IMAP = os.environ.get("EMAIL_IMAP", "imap.gmail.com")
EMAIL_REMETENTES = [x.strip().lower() for x in os.environ.get("EMAIL_REMETENTES", "").split(",") if x.strip()]
EMAIL_INTERVALO = int(os.environ.get("EMAIL_INTERVALO", "60"))


def _cor_variacao(it):
    """Variacao entre parenteses: '(Rosa Claro, Personalizado com Nome)' ou so '(Roxo)'. Kit/Padrao nao e cor."""
    par = [x for x in re.findall(r"\(([^()]*)\)", it.split(" / ")[0]) if x.strip()]
    if not par:
        par = [x for x in re.findall(r"\(([^()]*)\)", it) if "," in x]
    par = [x for x in par if "," in x or (len(x.strip()) <= 20 and not re.search(r"KIT|PADR|UNID|\bUND\b|\d", x.upper()))]
    return par


def ler_etiqueta_txt(t):
    """Le uma pagina de etiqueta do UpSeller (Shopee com DANFE, TikTok, etiqueta da folha de gravacao)."""
    T = (t or "").upper()
    # nº do pedido Shopee (14 caracteres, ex.: 2609286KQNSNAY); o PDF as vezes quebra com espaco no meio
    junto = re.sub(r"(?<=[0-9A-Z]) (?=[0-9A-Z])", "", T)
    sn = next((m for m in re.findall(r"(?<![0-9A-Z])(2\d{5}[0-9A-Z]{8})", T) + re.findall(r"(?<![0-9A-Z])(2\d{5}[0-9A-Z]{8})", junto)
               if re.search(r"[A-Z]", m)), "")
    # TikTok: nº do pedido com 18 digitos comecando com 5
    m = re.search(r"(?<!\d)(5\d{17})(?!\d)", T)
    tid = m.group(1) if m else ""
    # Shopee Entrega Rapida: etiqueta sem DANFE com "Pedido: 999882..." (15 digitos) - e esse o codigo de barras
    m = re.search(r"PEDIDO:\s*(9\d{13,15})(?!\d)", T)
    erid = m.group(1) if m else ""
    m = re.search(r"BR\d{12,14}[0-9A-Z]?", T)
    ras = m.group(0) if m else ""
    m = re.search(r"UPPUS\d+", T)
    ups = m.group(0) if m else ""
    ped = sn or tid or erid or ras or ups
    if not ped:
        return None
    envio = ""
    if tid or erid.startswith("9998") or "TIKTOK" in T or "TIK TOK" in T:
        canal = "TIKTOK"  # o arquivo "_tiktok" da automacao traz as etiquetas TikTok com "Pedido: 9998..."
    elif erid or re.search(r"ENTREGA\s+(DIRETA|R[AÁ]PIDA)", T):
        canal, envio = "SHOPEE", "ENTREGA DIRETA"
    else:
        canal, envio = "SHOPEE", "SHOPEE XPRESS"
    # itens do pedido (rodape do UpSeller): "1. 18726I-Personalizado (Rosa Claro, Personalizado com Nome) / ..."
    rod = re.split(r"#UPPUS\d+[^\n]*\n", t, maxsplit=1)
    skus, cores, pers, pecas = [], [], [], []
    if len(rod) == 2:
        for it in re.split(r"(?m)^\s*\d+\.\s*", rod[1])[1:]:
            mm = re.match(r"\s*([A-Za-z0-9]+)", it)
            mq = re.search(r"\*\s*(\d+)\s*\)?\s*$", it.strip())
            par0 = _cor_variacao(it)
            if mm:
                sku_it = mm.group(1).upper()
                var = it.split(" / ")[0].upper()  # so a variacao (o titulo do anuncio pode ter numeros)
                if sku_it == "18949":
                    sku_it = "18949P" if re.search(r"350\s*ML", it.upper()) else "18949M" if re.search(r"550\s*ML", it.upper()) else sku_it
                # kit: "AZUL - 100 UND", "KIT 50", "30 UNIDADES" -> unidades fisicas
                mk = re.search(r"(\d{2,4})\s*(?:UND|UNID|UNIDS|UNIDADES|UN)\b", var) or re.search(r"\bKIT\s*(?:C/|COM|DE)?\s*(\d{2,4})\b", var)
                kit = int(mk.group(1)) if mk else 1
                skus.append(sku_it)
                pecas.append({"sku": sku_it, "cor": par0[-1].split(",")[0].strip() if par0 else "",
                              "qtd": (int(mq.group(1)) if mq else 1) * kit, **({"kit": kit} if kit > 1 else {})})
            par = _cor_variacao(it)
            if par:
                cores.append(par[-1].split(",")[0].strip())
            I = it.upper()
            pers.append("PERSONALIZ" in I and "SEM PERSONALIZ" not in I)
    if not skus:
        skus = [x.upper() for x in re.findall(r"SKU\s*[:#-]?\s*([A-Za-z][A-Za-z0-9._\-/]{1,40})", t, re.I)]
    cli = re.search(r"Customer:\s*(.+)", t)
    # loja (remetente) na etiqueta Shopee: linha logo depois do rastreio BR...
    loja = ""
    if ras:
        ml = re.search(re.escape(ras) + r"\s*\n\s*([A-Za-z][A-Za-z0-9 &.\-]{1,30})\s*\n", t)
        if ml:
            loja = ml.group(1).strip().upper()
    fl = next((ln for ln in t.splitlines() if re.match(r"\s*Fonte\s*[:\-]", ln, re.I)), "")
    # so a linha "ETIQUETA 12" da folha de gravacao; nao confundir com "DANFE SIMPLIFICADO - ETIQUETA" + "1 - Saida"
    etq = re.search(r"(?mi)^[ \t]*ETIQUETA[ \t]+N?[ºo°.]?[ \t]*(\d+)[ \t]*$", t)
    if etq or _RX_PEDE_NOME.search(t):
        personalizado = True  # etiqueta numerada da folha de gravacao / "BUSCAR NOME NO CHAT" = vai para a gravacao
    elif pers:
        personalizado = any(pers)
    else:
        personalizado = not ("SEM PERSONALIZ" in T and not etq)
    return {"pedido": ped, "rastreio": ras, "canal": canal, "envio": envio,
            "sku": " + ".join(dict.fromkeys(skus)), "cor": " + ".join(dict.fromkeys(cores)),
            "fonte": re.sub(r"^\s*Fonte\s*[:\-]\s*", "", fl, flags=re.I).strip(),
            "obs": ("Cliente: " + cli.group(1).strip()) if cli else "",
            "etiqueta": int(etq.group(1)) if etq else None, "personalizado": personalizado,
            "pecas": pecas, "loja": loja, "impresso": _impresso_em(t),
            "codigos": [x for x in dict.fromkeys((sn, tid, erid, ras, ups)) if x]}


def dica_arquivo(nome):
    """A automacao separa os arquivos por tipo: etiquetas_<data>_<hora>_<n>_<tipo>.pdf - o nome manda no canal."""
    n = (nome or "").lower().replace(" ", "_")
    if "tiktok" in n:
        return {"canal": "TIKTOK", "envio": ""}
    if "entrega_rapida" in n or "entrega_direta" in n or "turbo" in n:
        return {"canal": "SHOPEE", "envio": "ENTREGA DIRETA"}
    if "shopee" in n or "xpress" in n:
        return {"canal": "SHOPEE", "envio": "SHOPEE XPRESS"}
    return {}


def _impresso_em(t):
    """Data de impressao do UpSeller no rodape: 'UPPUS210840 01/10/2026 19:03:02' -> 2026-10-01."""
    m = re.search(r"UPPUS\d+\s+(\d{2})/(\d{2})/(\d{4})", t or "")
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else ""


def itens_do_pdf(dados, nome=""):
    from pypdf import PdfReader
    dica = dica_arquivo(nome)
    itens, ign = [], 0
    for pg in PdfReader(io.BytesIO(dados)).pages:
        it = ler_etiqueta_txt(pg.extract_text() or "")
        if not it:
            ign += 1
            continue
        ant = itens[-1] if itens else None
        if ant and it["codigos"] == [it["pedido"]] and it["pedido"].startswith("UPPUS") and it["pedido"] in ant["codigos"]:
            # continuacao da lista de itens do mesmo pedido (pedido com muitos produtos ocupa 2 paginas)
            for k in ("sku", "cor"):
                ant[k] = " + ".join(dict.fromkeys([x for x in (ant[k] + " + " + it[k]).split(" + ") if x]))
            ant["personalizado"] = ant["personalizado"] or it["personalizado"]
            ant["pecas"] = ant.get("pecas", []) + it.get("pecas", [])
            continue
        itens.append(it)
    for it in itens:
        it.update(dica)
    return itens, ign


def checar_email():
    import imaplib, email
    from email.header import decode_header, make_header
    from email.utils import parseaddr
    im = imaplib.IMAP4_SSL(EMAIL_IMAP)
    try:
        im.login(EMAIL_USUARIO, EMAIL_SENHA)
        im.select("INBOX")
        # todos os e-mails dos ultimos 2 dias que ainda nao foram processados (mesmo se ja abertos no Gmail)
        desde = (datetime.now(BR) - timedelta(days=1)).strftime("%d-%b-%Y")
        _, ids = im.uid("SEARCH", None, f'(SINCE "{desde}")')
        with conn() as c:
            feitos = {r[0] for r in c.execute("SELECT uid FROM emails WHERE uid IS NOT NULL")}
        permitidos = set(EMAIL_REMETENTES) | {x for x in (EMAIL_USUARIO.lower(), XBZ_EMAIL_USUARIO.lower(),
                                                          "brindesdoboni@gmail.com", "bexluuh@gmail.com") if x}
        for mid in ids[0].split():
            uid = mid.decode()
            if uid in feitos:
                continue
            _, dd = im.uid("FETCH", mid, "(BODY.PEEK[])")
            if not dd or not isinstance(dd[0], tuple):
                continue
            msg = email.message_from_bytes(dd[0][1])
            rem = parseaddr(msg.get("From", ""))[1].lower()
            assunto = str(make_header(decode_header(msg.get("Subject", ""))))[:200]
            reg = dict(uid=uid, em=agora(), remetente=rem, assunto=assunto, arquivos="", etiquetas=0, novos=0, atualizados=0, erro="")
            xmls = [pt for pt in msg.walk() if (pt.get_filename() or "").lower().endswith(".xml")
                    or pt.get_content_type() in ("text/xml", "application/xml")]
            nfe = [pt for pt in xmls if b"infNFe" in (pt.get_payload(decode=True) or b"")]
            if nfe and (rem.endswith("@xbzbrindes.com.br") or rem in permitidos):
                # nota fiscal (ex.: XBZ): so o XML interessa; o PDF (DANFE) nao e etiqueta
                res = [importar_nfe(pt.get_payload(decode=True)) for pt in nfe]
                reg["arquivos"] = ", ".join(str(make_header(decode_header(pt.get_filename() or "nfe.xml"))) for pt in nfe)
                reg["erro"] = "; ".join(f"NF {r.get('nf')} {r.get('conta','')}: R$ {r.get('total')}" if r.get("ok")
                                        else r.get("erro", "") for r in res)
            elif rem not in permitidos:
                # registra uma vez so; se o remetente for liberado depois, o e-mail e lido normalmente
                if "neg:" + uid not in feitos:
                    reg.update(uid="neg:" + uid, erro="remetente nao autorizado (ignorado)")
                    with _lock, conn() as c:
                        c.execute(f"INSERT INTO emails({','.join(reg)}) VALUES({','.join('?' * len(reg))})", list(reg.values()))
                continue
            else:
                nomes = []
                for parte in msg.walk():
                    nome = parte.get_filename()
                    nome = str(make_header(decode_header(nome))) if nome else ""
                    if parte.get_content_type() != "application/pdf" and not nome.lower().endswith(".pdf"):
                        continue
                    nomes.append(nome or "anexo.pdf")
                    try:
                        itens, _ = itens_do_pdf(parte.get_payload(decode=True) or b"", nome)
                        reg["etiquetas"] += len(itens)
                        if itens:
                            r = importar_lote({"lote": "EMAIL " + datetime.now(BR).strftime("%d/%m %H:%M"), "itens": itens})
                            reg["novos"] += r["novos"]
                            reg["atualizados"] += r["atualizados"]
                    except Exception as e:
                        reg["erro"] = f"{nome}: {e}"[:300]
                reg["arquivos"] = ", ".join(nomes)
                if not nomes:
                    reg["erro"] = "e-mail sem PDF anexado"
                elif not reg["etiquetas"] and not reg["erro"]:
                    reg["erro"] = "nenhuma etiqueta reconhecida no PDF"
            with _lock, conn() as c:
                c.execute(f"INSERT INTO emails({','.join(reg)}) VALUES({','.join('?' * len(reg))})", list(reg.values()))
            im.uid("STORE", mid, "+FLAGS", "\\Seen")
    finally:
        try:
            im.logout()
        except Exception:
            pass


def reprocessar_emails(dias=2):
    """Le de novo os PDFs de etiquetas dos ultimos dias e corrige personalizado / numero da etiqueta / cor
    das etiquetas que ja estao na Central (nao muda etapa, nao cria nada novo, nao mexe em estoque)."""
    import imaplib, email
    im = imaplib.IMAP4_SSL(EMAIL_IMAP)
    corrigidos, lidos = 0, 0
    try:
        im.login(EMAIL_USUARIO, EMAIL_SENHA)
        im.select("INBOX")
        desde = (datetime.now(BR) - timedelta(days=dias)).strftime("%d-%b-%Y")
        _, ids = im.uid("SEARCH", None, f'(SINCE "{desde}")')
        for mid in ids[0].split():
            _, dd = im.uid("FETCH", mid, "(BODY.PEEK[])")
            if not dd or not isinstance(dd[0], tuple):
                continue
            msg = email.message_from_bytes(dd[0][1])
            for parte in msg.walk():
                nome = parte.get_filename() or ""
                if parte.get_content_type() != "application/pdf" and not nome.lower().endswith(".pdf"):
                    continue
                try:
                    itens, _ = itens_do_pdf(parte.get_payload(decode=True) or b"", nome)
                except Exception:
                    continue
                with _lock, conn() as c:
                    for it in itens:
                        lidos += 1
                        cods = [norm(x) for x in it.get("codigos") or [] if x]
                        if not cods:
                            continue
                        r = c.execute(f"SELECT i.id, i.personalizado, i.etiqueta, i.cor, i.tipo FROM itens i JOIN codigos k ON k.item_id=i.id "
                                      f"WHERE k.codigo IN ({','.join('?' * len(cods))}) LIMIT 1", cods).fetchone()
                        if not r or r["tipo"]:  # tipo veio da folha de gravacao (mais confiavel): nao mexe
                            continue
                        pers = 1 if it["personalizado"] else 0
                        cor = r["cor"] or it.get("cor") or ""
                        if (r["personalizado"], r["etiqueta"], r["cor"]) != (pers, it.get("etiqueta"), cor):
                            c.execute("UPDATE itens SET personalizado=?, etiqueta=?, cor=?, atualizado_em=? WHERE id=?",
                                      (pers, it.get("etiqueta"), cor, agora(), r["id"]))
                            corrigidos += 1
    finally:
        try:
            im.logout()
        except Exception:
            pass
    return {"ok": True, "lidos": lidos, "corrigidos": corrigidos}


XBZ_EMAIL_USUARIO = os.environ.get("XBZ_EMAIL_USUARIO", "").strip()
XBZ_EMAIL_SENHA = os.environ.get("XBZ_EMAIL_SENHA", "").replace(" ", "")


def checar_notas_xbz():
    """Le SO os e-mails da XBZ (notas fiscais) direto na caixa do brindesdoboni. Nao marca como lido, nao apaga,
    nao mexe em nenhum outro e-mail."""
    import imaplib, email
    from email.header import decode_header, make_header
    im = imaplib.IMAP4_SSL(EMAIL_IMAP)
    novas = 0
    try:
        im.login(XBZ_EMAIL_USUARIO, XBZ_EMAIL_SENHA)
        im.select("INBOX", readonly=True)
        desde = (datetime.now(BR) - timedelta(days=40)).strftime("%d-%b-%Y")
        _, ids = im.uid("SEARCH", None, f'(FROM "xbzbrindes.com.br" SINCE "{desde}")')
        with conn() as c:
            feitos = {r[0] for r in c.execute("SELECT uid FROM emails WHERE uid LIKE 'xbz:%'")}
        for mid in ids[0].split():
            uid = "xbz:" + mid.decode()
            if uid in feitos:
                continue
            _, dd = im.uid("FETCH", mid, "(BODY.PEEK[])")
            if not dd or not isinstance(dd[0], tuple):
                continue
            msg = email.message_from_bytes(dd[0][1])
            nfe = [pt for pt in msg.walk() if b"infNFe" in (pt.get_payload(decode=True) or b"")]
            res = [importar_nfe(pt.get_payload(decode=True)) for pt in nfe]
            reg = dict(uid=uid, em=agora(), remetente="XBZ (direto do " + XBZ_EMAIL_USUARIO + ")",
                       assunto=str(make_header(decode_header(msg.get("Subject", ""))))[:200],
                       arquivos=", ".join(str(make_header(decode_header(pt.get_filename() or "nfe.xml"))) for pt in nfe),
                       etiquetas=0, novos=0, atualizados=0,
                       erro="; ".join(f"NF {r.get('nf')} {r.get('conta','')}" if r.get("ok") else r.get("erro", "")
                                      for r in res) or "sem XML de nota")
            with _lock, conn() as c:
                c.execute(f"INSERT INTO emails({','.join(reg)}) VALUES({','.join('?' * len(reg))})", list(reg.values()))
            novas += 1
    finally:
        try:
            im.logout()
        except Exception:
            pass
    return novas


def _email_loop():
    import time
    while True:
        if EMAIL_USUARIO and EMAIL_SENHA:
            try:
                checar_email()
                _email_status.update(ok=True, erro="", ultima=agora())
            except Exception as e:
                _email_status.update(ok=False, erro=str(e)[:300], ultima=agora())
                print("E-mail:", e, flush=True)
        if XBZ_EMAIL_USUARIO and XBZ_EMAIL_SENHA:
            try:
                checar_notas_xbz()
                _email_status.update(xbz_ok=True, xbz_erro="", xbz_ultima=agora())
            except Exception as e:
                _email_status.update(xbz_ok=False, xbz_erro=str(e)[:300], xbz_ultima=agora())
                print("Notas XBZ:", e, flush=True)
        time.sleep(max(EMAIL_INTERVALO, 20))


_email_status = {"ativo": bool(EMAIL_USUARIO and EMAIL_SENHA), "conta": EMAIL_USUARIO, "ok": None, "erro": "", "ultima": "",
                 "xbz_conta": XBZ_EMAIL_USUARIO, "xbz_ok": None, "xbz_erro": "", "xbz_ultima": ""}



# ---------------- Shopee Open Platform (somente leitura por enquanto) ----------------
# Variaveis no Railway (nunca no codigo): SHOPEE_PARTNER_ID, SHOPEE_PARTNER_KEY.
# Opcional: SHOPEE_HOST (padrao = ao vivo), SHOPEE_RETORNO (padrao = https://operacaodoboni.up.railway.app/shopee/retorno).
SHOPEE_HOST = os.environ.get("SHOPEE_HOST", "https://partner.shopeemobile.com").rstrip("/")
SHOPEE_RETORNO = os.environ.get("SHOPEE_RETORNO", "https://operacaodoboni.up.railway.app/shopee/retorno")
_shopee_status = {"ultimo_erro": "", "ultima_renovacao": ""}
_shopee_convites = {}


def shopee_convite_ok(vale, usar=False):
    import time
    ok = _shopee_convites.get(vale or "", 0) > time.time()
    if ok and usar:
        _shopee_convites.pop(vale, None)
    return ok


def _shopee_cred():
    pid, key = os.environ.get("SHOPEE_PARTNER_ID", "").strip(), os.environ.get("SHOPEE_PARTNER_KEY", "").strip()
    return (int(pid), key) if pid.isdigit() and key else (None, None)


def _shopee_assina(key, *partes):
    return hmac.new(key.encode(), "".join(str(x) for x in partes).encode(), hashlib.sha256).hexdigest()


def _shopee_http(metodo, path, params=None, corpo=None, loja=None, aceitar_erro=False):
    """Chamada assinada. Nunca registra URL/assinatura/token em log. aceitar_erro: devolve a resposta mesmo com erro
    (o update_stock diz o motivo de cada variacao em failure_list)."""
    import urllib.request, urllib.parse, time
    pid, key = _shopee_cred()
    if not pid:
        raise RuntimeError("SHOPEE_PARTNER_ID/SHOPEE_PARTNER_KEY nao configurados no Railway")
    ts = int(time.time())
    q = {"partner_id": pid, "timestamp": ts}
    if loja:
        q.update(access_token=loja["access_token"], shop_id=loja["shop_id"])
        q["sign"] = _shopee_assina(key, pid, path, ts, loja["access_token"], loja["shop_id"])
    else:
        q["sign"] = _shopee_assina(key, pid, path, ts)
    q.update(params or {})
    url = SHOPEE_HOST + path + "?" + urllib.parse.urlencode(q, doseq=True)
    dados = json.dumps(corpo).encode() if corpo is not None else None
    req = urllib.request.Request(url, data=dados, method=metodo, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            res = json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            res = json.loads(e.read() or b"{}")
        except Exception:
            res = {}
        if not res.get("error"):
            res = {"error": f"http {e.code}", "message": res.get("message", "")}
    if res.get("error") and not aceitar_erro:
        raise RuntimeError(f"{res.get('error')}: {res.get('message', '')}"[:200])
    return res


def shopee_link_autorizacao():
    import urllib.parse, time
    pid, key = _shopee_cred()
    if not pid:
        return {"ok": False, "erro": "Coloque SHOPEE_PARTNER_ID e SHOPEE_PARTNER_KEY nas Variables do Railway"}
    path, ts = "/api/v2/shop/auth_partner", int(time.time())
    # codigo de uso unico (30 min) no caminho do retorno: a loja pode ser autorizada em outro navegador/perfil
    # sem login no painel, e ninguem de fora consegue ligar uma loja sem um link gerado pelo painel
    vale = secrets.token_urlsafe(16)
    _shopee_convites[vale] = ts + 1800
    q = {"partner_id": pid, "timestamp": ts, "sign": _shopee_assina(key, pid, path, ts),
         "redirect": SHOPEE_RETORNO.rstrip("/") + "/" + vale}
    return {"ok": True, "url": SHOPEE_HOST + path + "?" + urllib.parse.urlencode(q)}


def _shopee_salvar_token(c, shop_id, res):
    from time import time as _t
    agora_s = int(_t())
    c.execute("""INSERT INTO shopee_lojas(shop_id, nome, access_token, refresh_token, expira, autorizada_em, atualizado_em)
                 VALUES(?,?,?,?,?,?,?) ON CONFLICT(shop_id) DO UPDATE SET access_token=excluded.access_token,
                 refresh_token=excluded.refresh_token, expira=excluded.expira, atualizado_em=excluded.atualizado_em""",
              (int(shop_id), "", res["access_token"], res["refresh_token"], agora_s + int(res.get("expire_in") or 14400),
               agora(), agora()))


def shopee_retorno(code, shop_id, main_account_id=""):
    """Shopee volta aqui depois que a loja autoriza. Troca o code pelo token e guarda."""
    pid, _ = _shopee_cred()
    if not code or not (shop_id or main_account_id):
        return {"ok": False, "erro": "retorno sem code/shop_id"}
    corpo = {"code": code, "partner_id": pid}
    if shop_id:
        corpo["shop_id"] = int(shop_id)
    else:
        corpo["main_account_id"] = int(main_account_id)
    res = _shopee_http("POST", "/api/v2/auth/token/get", corpo=corpo)
    ids = [int(shop_id)] if shop_id else [int(x) for x in (res.get("shop_id_list") or [])]
    with _lock, conn() as c:
        for sid in ids:
            _shopee_salvar_token(c, sid, res)
    nomes = []
    for sid in ids:
        nomes.append(shopee_atualizar_nome(sid))
    return {"ok": True, "lojas": nomes}


def _shopee_loja(shop_id):
    with conn() as c:
        r = c.execute("SELECT * FROM shopee_lojas WHERE shop_id=?", (int(shop_id),)).fetchone()
    return dict(r) if r else None


def shopee_atualizar_nome(shop_id):
    loja = _shopee_loja(shop_id)
    try:
        info = _shopee_http("GET", "/api/v2/shop/get_shop_info", loja=loja)
        nome = info.get("shop_name") or str(shop_id)
        with _lock, conn() as c:
            c.execute("UPDATE shopee_lojas SET nome=?, status_loja=?, expira_autorizacao=? WHERE shop_id=?",
                      (nome, info.get("status", ""), int(info.get("expire_time") or 0), int(shop_id)))
        return nome
    except Exception as e:
        _shopee_status["ultimo_erro"] = f"{shop_id}: {e}"
        return str(shop_id)


def shopee_renovar(shop_id):
    loja = _shopee_loja(shop_id)
    pid, _ = _shopee_cred()
    res = _shopee_http("POST", "/api/v2/auth/access_token/get",
                       corpo={"refresh_token": loja["refresh_token"], "shop_id": int(shop_id), "partner_id": pid})
    with _lock, conn() as c:
        _shopee_salvar_token(c, shop_id, res)
        c.execute("UPDATE shopee_lojas SET erro='' WHERE shop_id=?", (int(shop_id),))


def _shopee_loop():
    """Renova o token de cada loja antes de vencer (vale 4h; renova a cada ~3h). Assim a autorizacao nao cai."""
    import time
    while True:
        try:
            with conn() as c:
                lojas = [dict(r) for r in c.execute("SELECT shop_id, expira FROM shopee_lojas")]
            for l in lojas:
                if l["expira"] - time.time() < 3600:
                    try:
                        shopee_renovar(l["shop_id"])
                        _shopee_status["ultima_renovacao"] = agora()
                    except Exception as e:
                        with _lock, conn() as c:
                            c.execute("UPDATE shopee_lojas SET erro=? WHERE shop_id=?", (str(e)[:200], l["shop_id"]))
        except Exception as e:
            _shopee_status["ultimo_erro"] = str(e)[:200]
        try:
            shopee_sincronizar_todas()
        except Exception as e:
            _shopee_status["ultimo_erro"] = str(e)[:200]
        try:
            shopee_devolucoes_sincronizar()
        except Exception as e:
            _shopee_dev_status["erro"] = str(e)[:200]
        time.sleep(600)


def shopee_testar(shop_id):
    """Teste so de leitura: quantos pedidos a loja teve nas ultimas 24h."""
    import time
    loja = _shopee_loja(shop_id)
    if not loja:
        return {"ok": False, "erro": "loja nao autorizada"}
    if loja["expira"] - time.time() < 120:
        shopee_renovar(shop_id)
        loja = _shopee_loja(shop_id)
    fim = int(time.time())
    res = _shopee_http("GET", "/api/v2/order/get_order_list", loja=loja,
                       params={"time_range_field": "create_time", "time_from": fim - 86400, "time_to": fim, "page_size": 100})
    r = res.get("response") or {}
    n = len(r.get("order_list") or [])
    return {"ok": True, "loja": loja["nome"], "pedidos_24h": f"{n}{'+' if r.get('more') else ''}"}


# ---------------- pedidos da Shopee (SO LEITURA: nada e alterado na loja)
SHOPEE_CANCEL = ("CANCELLED", "IN_CANCEL")
SHOPEE_STATUS_PT = {"UNPAID": "Aguardando pagamento", "READY_TO_SHIP": "A enviar", "PROCESSED": "Etiqueta gerada",
                    "SHIPPED": "Enviado", "TO_CONFIRM_RECEIVE": "Enviado", "COMPLETED": "Concluído",
                    "IN_CANCEL": "EM CANCELAMENTO", "CANCELLED": "CANCELADO", "TO_RETURN": "Devolução",
                    "INVOICE_PENDING": "Aguardando NF"}
_shopee_sync = {}


def _shopee_cancelado(c, item_ids):
    """Se alguma etiqueta bipada for de pedido cancelado (ou em cancelamento) na Shopee, devolve o pedido."""
    if not item_ids:
        return None
    q = ",".join("?" * len(item_ids))
    r = c.execute(f"""SELECT order_sn, status FROM shopee_pedidos WHERE status IN ('CANCELLED','IN_CANCEL') AND order_sn IN
                      (SELECT codigo FROM codigos WHERE item_id IN ({q}) UNION SELECT pedido FROM itens WHERE id IN ({q}))""",
                  list(item_ids) * 2).fetchone()
    return dict(r) if r else None


def _shopee_token_ok(shop_id):
    import time
    loja = _shopee_loja(shop_id)
    if loja and loja["expira"] - time.time() < 300:
        shopee_renovar(shop_id)
        loja = _shopee_loja(shop_id)
    return loja


def _shopee_salvar_pedidos(shop_id, nome, detalhes):
    with _lock, conn() as c:
        for o in detalhes:
            sn = norm(o.get("order_sn"))
            if not sn:
                continue
            itens = [{"sku": (i.get("model_sku") or i.get("item_sku") or "").strip(), "nome": (i.get("item_name") or "")[:80],
                      "var": (i.get("model_name") or "")[:60], "qtd": int(i.get("model_quantity_purchased") or 1)}
                     for i in (o.get("item_list") or [])]
            st = o.get("order_status") or ""
            desp = int(o.get("pickup_done_time") or 0) or (int(o.get("update_time") or 0) if st in SHOPEE_SAIU else 0) or None
            c.execute("""INSERT INTO shopee_pedidos(order_sn, shop_id, loja, status, criado, atualizado, prazo, envio, msg, itens, motivo, visto_em, despachado)
                         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(order_sn) DO UPDATE SET status=excluded.status,
                         despachado=COALESCE(shopee_pedidos.despachado, excluded.despachado),
                         atualizado=excluded.atualizado, prazo=excluded.prazo, envio=excluded.envio, msg=excluded.msg,
                         itens=excluded.itens, motivo=excluded.motivo, loja=excluded.loja, visto_em=excluded.visto_em""",
                      (sn, int(shop_id), nome, o.get("order_status") or "", int(o.get("create_time") or 0),
                       int(o.get("update_time") or 0), int(o.get("ship_by_date") or 0), o.get("shipping_carrier") or "",
                       (o.get("message_to_seller") or "").strip()[:500], json.dumps(itens, ensure_ascii=False),
                       (o.get("cancel_reason") or "")[:120], agora(), desp))


SHOPEE_SAIU = ("SHIPPED", "TO_CONFIRM_RECEIVE", "COMPLETED")


def _shopee_estoque(detalhes):
    """Rede de seguranca do estoque com o que a Shopee informa:
    - pedido ENVIADO cuja etiqueta nunca foi bipada: da baixa (o material saiu e ninguem bipou);
    - pedido CANCELADO que ja tinha dado baixa e ainda nao foi gravado nem despachado: o produto volta para a prateleira.
    Gravado (personalizado) nao volta: a peca ja foi gravada. Cada etiqueta so mexe uma vez (ref unica)."""
    import time
    feitos = {"baixa_enviado": 0, "volta_cancelado": 0}
    with _lock, conn() as c:
        for o in detalhes:
            sn, st = norm(o.get("order_sn")), o.get("order_status") or ""
            if st not in SHOPEE_SAIU and st != "CANCELLED":
                continue
            ids = [r[0] for r in c.execute("SELECT DISTINCT item_id FROM codigos WHERE codigo=?", (sn,))]
            for iid in ids:
                it = c.execute("SELECT * FROM itens WHERE id=?", (iid,)).fetchone()
                if not it or it["lote"] == "DEVOLUCAO":
                    continue
                if st in SHOPEE_SAIU:
                    if not it["despachado_em"] and it["status"] != "DEVOLVIDO":
                        # a transportadora retirou: conta como DESPACHADO (hora da coleta, se a Shopee informar)
                        ts = int(o.get("pickup_done_time") or o.get("update_time") or 0) or None
                        quando = datetime.fromtimestamp(min(ts, time.time()), timezone.utc).isoformat() if ts else agora()
                        c.execute("UPDATE itens SET despachado_em=?, despachado_por='SHOPEE' WHERE id=?", (quando, iid))
                        feitos["despachados"] = feitos.get("despachados", 0) + 1
                    if not _ja_baixado(c, iid) and it["status"] != "DEVOLVIDO":
                        baixar_estoque(c, iid)
                        feitos["baixa_enviado"] += _ja_baixado(c, iid)
                    continue
                if it["status"] in ("EXPEDIDO", "DEVOLVIDO", "EM_GRAVACAO", "GRAVADO"):
                    continue
                for m in c.execute("SELECT * FROM estoque_mov WHERE ref LIKE ? AND tipo='ETIQUETA'", (f"ETQ|{iid}|%",)).fetchall():
                    cur = c.execute("INSERT OR IGNORE INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                                    (agora(), m["sku"], m["cor"], -m["qtd"], "CANCELADO", "CANC|" + m["ref"],
                                     f"pedido {it['pedido']} cancelado na Shopee: volta para a prateleira"))
                    feitos["volta_cancelado"] += cur.rowcount
    return feitos


def shopee_sincronizar(shop_id, dias_iniciais=3):
    """Le os pedidos alterados desde a ultima leitura (status, prazo de postagem, envio, mensagem do comprador).
    So usa get_order_list / get_order_detail: nenhuma alteracao e feita na Shopee."""
    import time
    loja = _shopee_token_ok(shop_id)
    if not loja:
        return {"ok": False, "erro": "loja nao autorizada"}
    chave = f"shopee_sync_{int(shop_id)}"
    with conn() as c:
        r = c.execute("SELECT valor FROM meta WHERE chave=?", (chave,)).fetchone()
    agora_s = int(time.time())
    ini = int(r[0]) - 900 if r and str(r[0]).isdigit() else agora_s - dias_iniciais * 86400
    sns = []
    t0 = ini
    while t0 < agora_s:  # a Shopee aceita no maximo 15 dias por consulta
        t1 = min(t0 + 15 * 86400 - 60, agora_s)
        cursor = ""
        for _ in range(100):
            res = _shopee_http("GET", "/api/v2/order/get_order_list", loja=loja,
                               params={"time_range_field": "update_time", "time_from": t0, "time_to": t1,
                                       "page_size": 100, "cursor": cursor})
            rr = res.get("response") or {}
            sns += [o["order_sn"] for o in (rr.get("order_list") or []) if o.get("order_sn")]
            if not rr.get("more"):
                break
            cursor = rr.get("next_cursor") or ""
        t0 = t1
    sns = list(dict.fromkeys(sns))
    det = []
    for k in range(0, len(sns), 50):
        res = _shopee_http("GET", "/api/v2/order/get_order_detail", loja=loja,
                           params={"order_sn_list": ",".join(sns[k:k + 50]),
                                   "response_optional_fields": "item_list,shipping_carrier,cancel_reason,pickup_done_time"})
        det += (res.get("response") or {}).get("order_list") or []
    _shopee_salvar_pedidos(shop_id, loja.get("nome") or str(shop_id), det)
    est = _shopee_estoque(det)
    with _lock, conn() as c:
        c.execute("INSERT INTO meta(chave, valor) VALUES(?,?) ON CONFLICT(chave) DO UPDATE SET valor=excluded.valor",
                  (chave, str(agora_s)))
    _shopee_sync[int(shop_id)] = {"em": datetime.now(BR).strftime("%d/%m %H:%M"), "pedidos": len(det), "erro": "", **est}
    return {"ok": True, "loja": loja.get("nome"), "pedidos": len(det), **est}


def shopee_sincronizar_todas():
    with conn() as c:
        ids = [r[0] for r in c.execute("SELECT shop_id FROM shopee_lojas")]
    out = []
    for sid in ids:
        try:
            out.append(shopee_sincronizar(sid))
        except Exception as e:
            _shopee_sync[int(sid)] = {"em": datetime.now(BR).strftime("%d/%m %H:%M"), "pedidos": 0, "erro": str(e)[:200]}
            out.append({"ok": False, "shop_id": sid, "erro": str(e)[:200]})
    try:
        corrigir_personalizados_todos()
    except Exception as e:
        print("corrigir personalizados:", e, flush=True)
    try:
        limpar_etiquetas()
    except Exception as e:
        print("limpeza:", e, flush=True)
    try:   # etiqueta incluida no bipe que agora tem pedido na Shopee: ganha SKU e da a baixa que faltou
        garantir_baixas()
    except Exception as e:
        print("garantir baixas (shopee):", e, flush=True)
    return out


def shopee_pedidos_resumo():
    """Para a pagina /shopee/pedidos: cancelados que estao na Central, prazos de postagem e mensagens dos compradores."""
    import time
    hoje_fim = int(datetime.now(BR).replace(hour=23, minute=59, second=59).timestamp())
    with conn() as c:
        peds = [dict(r) for r in c.execute("""SELECT * FROM shopee_pedidos WHERE criado > ? OR status IN
                 ('READY_TO_SHIP','PROCESSED','IN_CANCEL','UNPAID','INVOICE_PENDING') ORDER BY prazo""",
                 (int(time.time()) - 15 * 86400,))]
        cent = {}
        if peds:
            for r in c.execute(f"""SELECT k.codigo, i.status, i.etiqueta FROM codigos k JOIN itens i ON i.id=k.item_id
                                   WHERE k.codigo IN ({",".join("?" * len(peds))})""", [p["order_sn"] for p in peds]):
                cent.setdefault(r[0], []).append(r[1])
    out = {"cancelados": [], "prazo": [], "mensagens": [], "sem_etiqueta": 0, "lojas": {}, "sync": _shopee_sync}
    for p in peds:
        p["itens"] = json.loads(p["itens"] or "[]")
        p["status_pt"] = SHOPEE_STATUS_PT.get(p["status"], p["status"])
        p["central"] = cent.get(p["order_sn"], [])
        p["prazo_txt"] = datetime.fromtimestamp(p["prazo"], BR).strftime("%d/%m %H:%M") if p["prazo"] else ""
        p["atrasado"] = bool(p["prazo"]) and p["prazo"] < time.time()
        p["hoje"] = bool(p["prazo"]) and not p["atrasado"] and p["prazo"] <= hoje_fim
        l = out["lojas"].setdefault(p["loja"], {"a_enviar": 0, "cancelados": 0, "atrasados": 0})
        if p["status"] in SHOPEE_CANCEL:
            l["cancelados"] += 1
            if p["central"] and not all(s in ("EXPEDIDO", "DEVOLVIDO") for s in p["central"]):
                out["cancelados"].append(p)
        elif p["status"] in ("READY_TO_SHIP", "PROCESSED"):
            l["a_enviar"] += 1
            l["atrasados"] += p["atrasado"]
            out["prazo"].append(p)
            out["sem_etiqueta"] += not p["central"]
        if p["msg"] and p["status"] not in SHOPEE_CANCEL:
            out["mensagens"].append(p)
    return out


# ---------------- anuncios da Shopee: ESTOQUE (nosso + XBZ) e FOTOS de instrucao do nome
# Autorizado pelo Lucas em 05/10/2026: estoque do anuncio = nosso disponivel + todo o estoque da XBZ, igual nas lojas.
# Protecoes: so mexe em variacao com SKU e cor reconhecidos; nao escreve com XBZ desatualizada.
ANUNCIOS_INTERVALO = int(os.environ.get("ANUNCIOS_INTERVALO", "1800"))
ANUNCIOS_XBZ_MAX_HORAS = float(os.environ.get("ANUNCIOS_XBZ_MAX_HORAS", "16"))
# Regra do Lucas (06/10/2026): XBZ com 100 ou menos de um produto/cor = conta como ZERO no anuncio
# (estoque baixo na XBZ some rapido; a Shopee fica so com o que temos aqui, e zera se nao tivermos).
ANUNCIOS_XBZ_MIN = int(os.environ.get("ANUNCIOS_XBZ_MIN", "100"))
_anuncios_status = {"rodando": False, "ultima": "", "lojas": {}, "erro": ""}


def _shopee_upload_imagem(caminho):
    """Sobe uma imagem para o banco de imagens da Shopee (do app). Devolve o image_id."""
    import urllib.request, urllib.parse, time
    pid, key = _shopee_cred()
    if not pid:
        raise RuntimeError("SHOPEE_PARTNER_ID/SHOPEE_PARTNER_KEY nao configurados no Railway")
    path, ts = "/api/v2/media_space/upload_image", int(time.time())
    q = {"partner_id": pid, "timestamp": ts, "sign": _shopee_assina(key, pid, path, ts)}
    lim = "----boni" + secrets.token_hex(8)
    with open(caminho, "rb") as f:
        dados = f.read()
    corpo = (f"--{lim}\r\nContent-Disposition: form-data; name=\"scene\"\r\n\r\nnormal\r\n"
             f"--{lim}\r\nContent-Disposition: form-data; name=\"ratio\"\r\n\r\n1:1\r\n"
             f"--{lim}\r\nContent-Disposition: form-data; name=\"image\"; filename=\"{os.path.basename(caminho)}\"\r\n"
             f"Content-Type: image/jpeg\r\n\r\n").encode() + dados + f"\r\n--{lim}--\r\n".encode()
    req = urllib.request.Request(SHOPEE_HOST + path + "?" + urllib.parse.urlencode(q), data=corpo, method="POST",
                                 headers={"Content-Type": f"multipart/form-data; boundary={lim}"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            res = json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        res = json.loads(e.read() or b"{}") if e.fp else {"error": f"http {e.code}"}
    if res.get("error"):
        raise RuntimeError(f"{res.get('error')}: {res.get('message', '')}"[:200])
    rr = res.get("response") or {}
    info = rr.get("image_info") or ((rr.get("image_info_list") or [{}])[0].get("image_info") or {})
    if not info.get("image_id"):
        raise RuntimeError("upload sem image_id")
    return info["image_id"]


def _shopee_itens_ativos(loja):
    ids, off = [], 0
    for _ in range(100):
        res = _shopee_http("GET", "/api/v2/product/get_item_list", loja=loja,
                           params={"offset": off, "page_size": 100, "item_status": "NORMAL"})
        rr = res.get("response") or {}
        ids += [int(i["item_id"]) for i in (rr.get("item") or [])]
        if not rr.get("has_next_page"):
            break
        off = rr.get("next_offset") or off + 100
    return ids


def _shopee_base_info(loja, ids):
    out = {}
    for k in range(0, len(ids), 50):
        res = _shopee_http("GET", "/api/v2/product/get_item_base_info", loja=loja,
                           params={"item_id_list": ",".join(str(x) for x in ids[k:k + 50])})
        for it in (res.get("response") or {}).get("item_list") or []:
            out[int(it["item_id"])] = it
    return out


def _estoque_do_vendedor(info):
    """(estoque atual, location_id) de stock_info_v2.seller_stock."""
    ss = ((info or {}).get("stock_info_v2") or {}).get("seller_stock") or []
    if not ss:
        return None, None
    return sum(int(x.get("stock") or 0) for x in ss), ss[0].get("location_id")


def _kit_mult(*textos):
    for t in textos:
        t = (t or "").upper()
        m = (re.search(r"(?<![0-9])(\d{1,4})\s*X\s*(?=\d)", t) or re.search(r"(\d{2,4})\s*(?:UND|UNID|UNIDS|UNIDADES|UN)\b", t)
             or re.search(r"\bKIT\s*(?:C/|COM|DE)?\s*(\d{1,4})\b", t) or re.search(r"(?<![0-9])(\d)\s*(?:UN|UND)\b", t))
        if m and int(m.group(1)) > 1:
            return int(m.group(1))
    return 1


def _sku_do_texto(t, conhecidos=None, extra=""):
    """SKU do anuncio -> SKU do estoque. Usa o texto todo (18949 + '550ml' = 18949M) e, se o estoque conhece
    o produto com outro numero de zeros (018637 x 18637, 1093 x 01093), usa o do estoque."""
    T = (t or "").upper()
    m = re.search(r"(?<![0-9A-Z])(?:\d{1,4}X)?(\d{4,6}[A-Z]?)(?![0-9A-Z])", T)
    if not m:
        return ""
    sku = estoque_chave(m.group(1), "", f"{T} {extra}")[0]
    if conhecidos is not None and sku not in conhecidos:
        for v in sorted(_skus_parecidos(sku)):
            v2 = estoque_chave(v, "", f"{T} {extra}")[0]
            if v2 in conhecidos:
                return v2
    return sku


def _base_estoque_anuncios(c):
    """Nosso disponivel e o estoque da XBZ por (sku, cor), com as cores conhecidas de cada sku."""
    nosso, cores = {}, {}
    for i in estoque()["itens"]:
        if i.get("sem_cor"):
            continue
        cor = "" if i["cor"] == "PADRAO" else i["cor"]
        nosso[(i["sku"], cor)] = i["saldo"] if i.get("conhecido") else max(0, i["saldo"])
        cores.setdefault(i["sku"], set()).add(cor)
    xbz = {}
    for r in c.execute("SELECT codigo, cor, estoque FROM xbz"):
        k = (estoque_chave(r[0])[0], r[1] or "")
        xbz[k] = xbz.get(k, 0) + int(r[2] or 0)
        cores.setdefault(k[0], set()).add(k[1])
    return nosso, xbz, cores


def _cor_do_modelo(sku, opcoes, cores):
    """Acha nas opcoes da variacao (ex.: 'Rosa Claro', 'Personalizado') a cor que o estoque/XBZ conhece."""
    cs = cores.get(sku, set())
    if cs <= {""}:
        return ""
    for op in opcoes:
        for parte in re.split(r"[,/|;]| - ", op or ""):
            parte = re.sub(r"^\s*\d+\s*(?:UN|UND|UNID|UNIDADES|X)\b\.?\s*", "", parte, flags=re.I)
            parte = re.sub(r"\(?\d+\s*ML\)?", "", parte, flags=re.I)
            for cand in (estoque_chave(sku, parte)[1], cor_norm(parte)):
                if cand and cand in cs:
                    return cand
    return None


def _xbz_idade_horas(c):
    r = c.execute("SELECT MAX(atualizado) FROM xbz").fetchone()[0]
    if not r:
        return None
    return (datetime.now(timezone.utc) - datetime.fromisoformat(r)).total_seconds() / 3600


def anuncios_estoque(aplicar=False, so_loja=None):
    """Calcula (e, se aplicar=True, grava) o estoque de cada variacao dos anuncios ativos de cada loja autorizada."""
    import time
    with conn() as c:
        idade = _xbz_idade_horas(c)
        nosso, xbz, cores = _base_estoque_anuncios(c)
        lojas = [r[0] for r in c.execute("SELECT shop_id FROM shopee_lojas ORDER BY shop_id")]
    if idade is None or idade > ANUNCIOS_XBZ_MAX_HORAS:
        return {"ok": False, "erro": f"estoque da XBZ desatualizado ({'sem dados' if idade is None else f'{idade:.0f} h'}): nada foi alterado"}
    rel = {"ok": True, "aplicado": aplicar, "em": datetime.now(BR).strftime("%d/%m %H:%M"), "lojas": []}
    for sid in lojas:
        if so_loja and int(so_loja) != sid:
            continue
        L = {"shop_id": sid, "nome": "", "mudar": [], "igual": 0, "sem_par": [], "erros": [], "gravados": 0}
        try:
            loja = _shopee_token_ok(sid)
            L["nome"] = loja.get("nome") or str(sid)
            ids = _shopee_itens_ativos(loja)
            base = _shopee_base_info(loja, ids)
            for iid in ids:
                it = base.get(iid) or {}
                nome = it.get("item_name") or ""
                modelos = []
                if it.get("has_model"):
                    res = _shopee_http("GET", "/api/v2/product/get_model_list", loja=loja, params={"item_id": iid})
                    rr = res.get("response") or {}
                    tiers = rr.get("tier_variation") or []
                    for m in rr.get("model") or []:
                        ops = []
                        for t, ix in zip(tiers, m.get("tier_index") or []):
                            ol = t.get("option_list") or []
                            if ix < len(ol):
                                ops.append(ol[ix].get("option") or "")
                        atual, loc = _estoque_do_vendedor(m)
                        modelos.append({"model_id": int(m["model_id"]), "sku_txt": m.get("model_sku") or it.get("item_sku") or "",
                                        "ops": ops, "atual": atual, "loc": loc})
                    time.sleep(0.15)
                else:
                    atual, loc = _estoque_do_vendedor(it)
                    modelos.append({"model_id": 0, "sku_txt": it.get("item_sku") or "", "ops": [], "atual": atual, "loc": loc})
                for m in modelos:
                    rot = f"{nome[:45]} | {' / '.join(m['ops'])}".strip(" |")
                    extra = f"{nome} {' '.join(m['ops'])}"
                    sku = _sku_do_texto(m["sku_txt"], cores, extra) or _sku_do_texto(it.get("item_sku"), cores, extra)
                    if not sku or sku not in cores:
                        L["sem_par"].append({"item_id": iid, "anuncio": rot, "sku": m["sku_txt"], "motivo": "SKU nao reconhecido"})
                        continue
                    cor = _cor_do_modelo(sku, m["ops"] + [m["sku_txt"].split("-", 1)[1] if "-" in m["sku_txt"] else ""], cores)
                    if cor is None:
                        L["sem_par"].append({"item_id": iid, "anuncio": rot, "sku": sku, "motivo": "cor nao reconhecida"})
                        continue
                    k = (sku, cor)
                    if k not in nosso and k not in xbz:
                        L["sem_par"].append({"item_id": iid, "anuncio": rot, "sku": sku, "motivo": f"{sku} {cor or 'PADRAO'} sem estoque cadastrado"})
                        continue
                    mult = _kit_mult(m["sku_txt"], " ".join(m["ops"]))
                    xbz_k = xbz.get(k, 0)
                    xbz_conta = xbz_k if xbz_k > ANUNCIOS_XBZ_MIN else 0
                    alvo = int((max(0, nosso.get(k, 0)) + xbz_conta) // mult)
                    if m["atual"] is not None and alvo == m["atual"]:
                        L["igual"] += 1
                        continue
                    L["mudar"].append({"item_id": iid, "model_id": m["model_id"], "anuncio": rot, "sku": sku,
                                       "cor": cor or "PADRAO", "kit": mult, "nosso": nosso.get(k, 0), "xbz": xbz_k,
                                       "xbz_ignorado": bool(xbz_k and not xbz_conta),
                                       "de": m["atual"], "para": alvo, "loc": m["loc"]})
            if aplicar:
                por_item = {}
                for x in L["mudar"]:
                    por_item.setdefault(x["item_id"], []).append(x)
                for iid, xs in por_item.items():
                    for k in range(0, len(xs), 50):
                        lista = [{"model_id": x["model_id"],
                                  "seller_stock": [{**({"location_id": x["loc"]} if x["loc"] else {}), "stock": x["para"]}]}
                                 for x in xs[k:k + 50]]
                        try:
                            res = _shopee_http("POST", "/api/v2/product/update_stock", loja=loja,
                                               corpo={"item_id": iid, "stock_list": lista}, aceitar_erro=True)
                            rr = res.get("response") or {}
                            L["gravados"] += len(rr.get("success_list") or [])
                            falhas = rr.get("failure_list") or []
                            nomes = {x["model_id"]: x["anuncio"] for x in xs}
                            for f in falhas:
                                L["erros"].append(f"{nomes.get(f.get('model_id'), iid)}: {f.get('failed_reason', '')}"[:200])
                            if res.get("error") and not falhas:
                                L["erros"].append(f"{nomes.get(xs[0]['model_id'], iid)}: {res.get('error')} {res.get('message', '')}"[:200])
                        except Exception as e:
                            L["erros"].append(f"{iid}: {e}"[:160])
                        time.sleep(0.2)
        except Exception as e:
            L["erros"].append(str(e)[:200])
        for x in L["mudar"]:
            x.pop("loc", None)
        rel["lojas"].append(L)
    if aplicar:
        _anuncios_status.update(ultima=rel["em"], lojas={l["nome"] or l["shop_id"]: {
            "gravados": l["gravados"], "mudar": len(l["mudar"]), "igual": l["igual"], "sem_par": len(l["sem_par"]),
            "erros": l["erros"][:5]} for l in rel["lojas"]}, erro="")
        with _lock, conn() as c:
            c.execute("INSERT OR REPLACE INTO meta VALUES('anuncios_ultimo', ?)", (json.dumps(rel, ensure_ascii=False)[:900000],))
    return rel


def anuncios_auto():
    with conn() as c:
        r = c.execute("SELECT valor FROM meta WHERE chave='anuncios_auto'").fetchone()
    return (r[0] if r else "1") == "1"


def _anuncios_loop():
    import time
    time.sleep(180)
    while True:
        try:
            if anuncios_auto():
                _anuncios_status["rodando"] = True
                r = anuncios_estoque(aplicar=True)
                if not r.get("ok"):
                    _anuncios_status["erro"] = r.get("erro", "")
        except Exception as e:
            _anuncios_status["erro"] = str(e)[:200]
        _anuncios_status["rodando"] = False
        time.sleep(ANUNCIOS_INTERVALO)


# ---- fotos: troca as imagens antigas de instrucao pelas novas (plano revisado: web/fotos_plano.json)
FOTOS_NOVAS = {"NOVO_A": "foto_nome_1_como_pedir.jpg", "NOVO_B": "foto_nome_2_fontes.jpg", "NOVO_C": "foto_nome_3_esqueceu.jpg"}


def _arquivo_web(nome):
    """Arquivo da pasta web (ou da raiz do repositorio, se subiu solto)."""
    c = os.path.join(AQUI, "web", nome)
    return c if os.path.exists(c) else os.path.join(AQUI, nome)


def _fotos_plano():
    with open(_arquivo_web("fotos_plano.json"), encoding="utf-8") as f:
        return json.load(f)


def fotos_aplicar(aplicar=False):
    """Simula (ou aplica) a troca das fotos de instrucao. So mexe se as fotos do anuncio ainda forem as mesmas do plano."""
    plano = _fotos_plano()
    sid = int(plano["shop_id"])
    loja = _shopee_token_ok(sid)
    if not loja:
        return {"ok": False, "erro": "loja do plano nao autorizada"}
    base = _shopee_base_info(loja, [int(i) for i in plano["itens"]])
    novos = {}
    if aplicar:
        with conn() as c:
            r = c.execute("SELECT valor FROM meta WHERE chave='fotos_novas_ids'").fetchone()
        novos = json.loads(r[0]) if r else {}
        for k, arq in FOTOS_NOVAS.items():
            if not novos.get(k):
                novos[k] = _shopee_upload_imagem(_arquivo_web(arq))
        with _lock, conn() as c:
            c.execute("INSERT OR REPLACE INTO meta VALUES('fotos_novas_ids', ?)", (json.dumps(novos),))
    out = {"ok": True, "aplicado": aplicar, "loja": loja.get("nome"), "itens": []}
    for iid, p in plano["itens"].items():
        it = base.get(int(iid)) or {}
        atual = ((it.get("image") or {}).get("image_id_list")) or []
        linha = {"item_id": int(iid), "anuncio": (it.get("item_name") or "")[:60], "tirou": p.get("tirou", [])}
        if not it:
            linha["status"] = "anuncio nao encontrado / inativo"
        elif atual == p["depois"] or (novos and atual == [novos.get(x, x) for x in p["depois"]]):
            linha["status"] = "ja estava com as fotos novas"
        elif atual != p["antes"]:
            linha["status"] = "PULADO: as fotos mudaram desde a revisao (nada alterado)"
        elif not aplicar:
            linha["status"] = "vai trocar"
        else:
            lista = [novos.get(x, x) for x in p["depois"]]
            try:
                _shopee_http("POST", "/api/v2/product/update_item", loja=loja,
                             corpo={"item_id": int(iid), "image": {"image_id_list": lista}})
                linha["status"] = "TROCADO"
            except Exception as e:
                linha["status"] = "ERRO: " + str(e)[:150]
        out["itens"].append(linha)
    if aplicar:
        with _lock, conn() as c:
            c.execute("INSERT OR REPLACE INTO meta VALUES('fotos_ultimo', ?)", (json.dumps(out, ensure_ascii=False),))
    return out


def fotos_desfazer():
    """Volta as fotos antigas (lista 'antes' do plano) nos anuncios que foram trocados."""
    plano = _fotos_plano()
    loja = _shopee_token_ok(int(plano["shop_id"]))
    with conn() as c:
        r = c.execute("SELECT valor FROM meta WHERE chave='fotos_novas_ids'").fetchone()
    novos = json.loads(r[0]) if r else {}
    base = _shopee_base_info(loja, [int(i) for i in plano["itens"]])
    out = []
    for iid, p in plano["itens"].items():
        atual = (((base.get(int(iid)) or {}).get("image") or {}).get("image_id_list")) or []
        if atual == [novos.get(x, x) for x in p["depois"]]:
            try:
                _shopee_http("POST", "/api/v2/product/update_item", loja=loja,
                             corpo={"item_id": int(iid), "image": {"image_id_list": p["antes"]}})
                out.append({"item_id": int(iid), "status": "voltou"})
            except Exception as e:
                out.append({"item_id": int(iid), "status": "ERRO: " + str(e)[:150]})
    return {"ok": True, "itens": out}


def shopee_lojas():
    from time import time as _t
    pid, _ = _shopee_cred()
    with conn() as c:
        rows = [dict(r) for r in c.execute("SELECT shop_id, nome, status_loja, expira, expira_autorizacao, autorizada_em, atualizado_em, erro FROM shopee_lojas ORDER BY nome")]
    for r in rows:
        r["token_ok"] = r["expira"] > _t()
        r["autorizacao_ate"] = datetime.fromtimestamp(r["expira_autorizacao"], BR).strftime("%d/%m/%Y") if r.get("expira_autorizacao") else ""
        del r["expira"]
    return {"configurado": bool(pid), "lojas": rows, "status": _shopee_status, "retorno": SHOPEE_RETORNO}


if __name__ == "__main__":
    iniciar_db()
    try:
        n = carregar_abertura()
        if n:
            print(f"Estoque: saldo inicial carregado ({n} produtos/cores)", flush=True)
    except Exception as e:
        print("Estoque: erro no saldo inicial:", e, flush=True)
    try:
        corrigir_personalizados_todos()
    except Exception as e:
        print("corrigir personalizados:", e, flush=True)
    _tenta_limpar()
    try:
        corrigir_nf_antes_da_contagem()
    except Exception as e:
        print("corte NF:", e, flush=True)
    try:
        corrigir_baixas_erradas()
    except Exception as e:
        print("baixas erradas:", e, flush=True)
    try:
        garantir_baixas()
    except Exception as e:
        print("garantir baixas:", e, flush=True)
    try:
        corrigir_auto_sem_prova()
    except Exception as e:
        print("auto sem prova:", e, flush=True)
    try:
        zerar_faltas_bipadas()
    except Exception as e:
        print("zerar faltas:", e, flush=True)
    if _email_status["ativo"] or (XBZ_EMAIL_USUARIO and XBZ_EMAIL_SENHA):
        threading.Thread(target=_email_loop, daemon=True).start()
    if os.environ.get("XBZ_TOKEN"):
        threading.Thread(target=_xbz_loop, daemon=True).start()
    threading.Thread(target=_shopee_loop, daemon=True).start()
    threading.Thread(target=_vendas_loop, daemon=True).start()   # historico de vendas (previsao por dia da semana)
    if xbz_api_configurada():
        threading.Thread(target=_xbz_compras_loop, daemon=True).start()   # API oficial (substitui a leitura do site)
    elif xbz_site_configurado():
        threading.Thread(target=_xbz_retiradas_loop, daemon=True).start()
    threading.Thread(target=_anuncios_loop, daemon=True).start()
    porta = int(os.environ.get("PORT", "8000"))
    print(f"Central Boni rodando na porta {porta} (banco: {DB})", flush=True)
    ThreadingHTTPServer(("0.0.0.0", porta), H).serve_forever()
