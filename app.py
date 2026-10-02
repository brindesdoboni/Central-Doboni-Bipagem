# -*- coding: utf-8 -*-
"""Central Boni - Bipagem da producao. Python puro (sem dependencias). SQLite em volume."""
import csv, hashlib, hmac, io, json, os, re, secrets, sqlite3, threading
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

ETAPAS = ["AGUARDANDO", "SEPARADO", "EM_GRAVACAO", "EXPEDIDO", "DEVOLVIDO"]
ORDEM = {e: i for i, e in enumerate(ETAPAS)}


def agora():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def conn():
    c = sqlite3.connect(DB, timeout=15)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON")
    return c


def iniciar_db():
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
        if "envio" not in [r[1] for r in c.execute("PRAGMA table_info(itens)")]:
            c.execute("ALTER TABLE itens ADD COLUMN envio TEXT DEFAULT ''")
        cols_it = [r[1] for r in c.execute("PRAGMA table_info(itens)")]
        if "qtd" not in cols_it:
            c.execute("ALTER TABLE itens ADD COLUMN qtd INTEGER DEFAULT 1")
        if "impresso" not in cols_it:
            c.execute("ALTER TABLE itens ADD COLUMN impresso TEXT DEFAULT ''")
        if "pecas" not in cols_it:
            c.execute("ALTER TABLE itens ADD COLUMN pecas TEXT DEFAULT ''")
        c.execute("DELETE FROM custos WHERE length(COALESCE(atualizado_em,''))=10")  # custos vindos de nota (valor nao real)
        c.execute("""CREATE TABLE IF NOT EXISTS estoque_mov(id INTEGER PRIMARY KEY, em TEXT, sku TEXT, cor TEXT,
            qtd REAL, tipo TEXT, ref TEXT UNIQUE, obs TEXT)""")
        c.execute("CREATE INDEX IF NOT EXISTS ix_mov_sku ON estoque_mov(sku, cor)")
        c.execute("""CREATE TABLE IF NOT EXISTS compra_aprendizado(sku TEXT PRIMARY KEY, fator REAL, pedidos INTEGER,
            atualizado TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS pedidos_xbz(id INTEGER PRIMARY KEY, em TEXT, itens TEXT, total REAL)""")
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
        c.execute("UPDATE itens SET status='EM_GRAVACAO' WHERE status='GRAVADO'")
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


GRUPOS = ["SHOPEE ENTREGA RÁPIDA", "TIKTOK", "SHOPEE EXPRESS", "OUTROS"]


def grupo_envio(i):
    """Plataforma/forma de envio para a contagem do dia (lojas juntas)."""
    t = " ".join([i.get("canal") or "", i.get("envio") or "", _envio_obs(i.get("obs"))]).upper()
    if "DIRETA" in t or "RAPIDA" in t or "RÁPIDA" in t:
        return "SHOPEE ENTREGA RÁPIDA"
    if "TIKTOK" in t or "TIK TOK" in t:
        return "TIKTOK"
    if "SHOPEE" in t or "SPX" in t or "XPRESS" in t:
        return "SHOPEE EXPRESS"  # Shopee que nao e entrega direta = Express
    return "OUTROS"


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
        p = pedidos.setdefault(k, {"grupo": g, "pendente": False})
        if g != "OUTROS":
            p["grupo"] = g
        if i["status"] != "EXPEDIDO":
            p["pendente"] = True
    res = {g: {"total": 0, "enviados": 0, "faltam": 0} for g in GRUPOS}
    for p in pedidos.values():
        r = res[p["grupo"]]
        r["total"] += 1
        r["faltam" if p["pendente"] else "enviados"] += 1
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
        sem = sum(checar_falta(c, iid) for iid in dict.fromkeys(tocados))
    return {"ok": True, "lote": lote, "novos": n_novo, "atualizados": n_atual, "sem_estoque": sem}


# ------------------------------------------------------------------ bipe
def bipar(posto, codigo, operador, modo):
    posto = (posto or "").upper()
    cod = norm(codigo)
    with _lock, conn() as c:
        col = c.execute("SELECT * FROM colaboradores WHERE codigo=? AND ativo=1", (cod,)).fetchone()
        if col:
            return {"tipo": "operador", "operador": {"codigo": col["codigo"], "nome": col["nome"]},
                    "msg": f"Ola, {col['nome']}!"}
        op = c.execute("SELECT * FROM colaboradores WHERE codigo=? AND ativo=1", (norm(operador),)).fetchone()
        if not op:
            return {"tipo": "erro", "msg": "Bipe o seu CRACHA primeiro."}
        if cod == "CMDDESFAZER":
            ev = c.execute("""SELECT e.*, i.pedido FROM eventos e JOIN itens i ON i.id=e.item_id
                   WHERE colaborador_id=? AND posto=? AND desfeito=0 ORDER BY e.id DESC LIMIT 1""",
                           (op["id"], posto)).fetchone()
            if not ev:
                return {"tipo": "aviso", "msg": "Nada para desfazer."}
            c.execute("UPDATE eventos SET desfeito=1 WHERE id=?", (ev["id"],))
            recalcular(c, ev["item_id"])
            if c.execute("SELECT status FROM itens WHERE id=?", (ev["item_id"],)).fetchone()[0] == "AGUARDANDO":
                c.execute("DELETE FROM estoque_mov WHERE ref LIKE ?", (f"ETQ|{ev['item_id']}|%",))  # volta para o estoque
            return {"tipo": "ok", "msg": f"Desfeito: {ev['etapa']} do pedido {ev['pedido']}"}
        itens = c.execute("SELECT i.* FROM itens i JOIN codigos k ON k.item_id=i.id WHERE k.codigo=? "
                          "ORDER BY i.etiqueta, i.id", (cod,)).fetchall()
        if not itens and posto == "DEVOLUCAO":
            iid = c.execute("""INSERT INTO itens(chave,lote,pedido,sku,personalizado,status,criado_em,atualizado_em)
                               VALUES(?,?,?,?,0,'AGUARDANDO',?,?)""",
                            (f"DEV|{cod}|{agora()}", "DEVOLUCAO", codigo.strip(), "", agora(), agora())).lastrowid
            c.execute("INSERT OR IGNORE INTO codigos VALUES(?,?)", (cod, iid))
            itens = c.execute("SELECT * FROM itens WHERE id=?", (iid,)).fetchall()
        if not itens:
            return {"tipo": "erro", "msg": f"Codigo {codigo} nao encontrado em nenhum lote."}

        def ev(it, etapa, alerta=""):
            c.execute("INSERT INTO eventos(item_id,etapa,colaborador_id,posto,em,alerta) VALUES(?,?,?,?,?,?)",
                      (it["id"], etapa, op["id"], posto, agora(), alerta))
            recalcular(c, it["id"])
            if etapa in ("SEPARADO", "GRAVACAO_INICIO", "EXPEDIDO"):
                baixar_estoque(c, it["id"])  # baixa no estoque quando o material sai para a separacao
            return dict(c.execute("SELECT * FROM itens WHERE id=?", (it["id"],)).fetchone())

        if modo == "FALTA":
            it = itens[0]
            if it["falta_material"]:
                return {"tipo": "aviso", "msg": f"Ja estava marcado como NAO TEM - {it['sku']}", "item": dict(it)}
            return {"tipo": "aviso", "msg": f"FALTA DE MATERIAL registrada - {it['sku']}",
                    "item": ev(it, "FALTA_MATERIAL", "falta de material")}

        if posto == "SEPARACAO":
            alvo = next((i for i in itens if ORDEM[i["status"]] < ORDEM["SEPARADO"]), None)
            if not alvo:
                return {"tipo": "aviso", "msg": "Ja separado.", "item": dict(itens[0])}
            return {"tipo": "ok", "msg": "Separado", "item": ev(alvo, "SEPARADO")}

        if posto == "GRAVACAO":
            pers = [i for i in itens if i["personalizado"]]
            if not pers:
                return {"tipo": "erro", "msg": "Este pedido NAO tem gravacao."}
            alvo = next((i for i in pers if ORDEM[i["status"]] < ORDEM["EM_GRAVACAO"]), None)
            if alvo:
                alerta = "" if alvo["status"] == "SEPARADO" else "pulou separacao"
                r = ev(alvo, "GRAVACAO_INICIO", alerta)
                return {"tipo": "aviso" if alerta else "ok",
                        "msg": "Gravacao registrada" + (" (atencao: nao foi separado)" if alerta else ""), "item": r}
            return {"tipo": "aviso", "msg": "Ja foi para gravacao.", "item": dict(pers[0])}

        if posto == "EXPEDICAO":
            falta = [i for i in itens if i["personalizado"] and ORDEM[i["status"]] < ORDEM["EM_GRAVACAO"]]
            if falta:
                return {"tipo": "erro", "msg": f"NAO DESPACHAR: {len(falta)} item(ns) ainda nao gravado(s)!",
                        "item": dict(falta[0])}
            pend = [i for i in itens if i["status"] != "EXPEDIDO"]
            if not pend:
                return {"tipo": "aviso", "msg": "Ja expedido.", "item": dict(itens[0])}
            r = None
            for i in pend:
                r = ev(i, "EXPEDIDO")
            return {"tipo": "ok", "msg": f"Expedido ({len(pend)} item(ns))", "item": r}
        if posto == "DEVOLUCAO":
            pend = [i for i in itens if i["status"] != "DEVOLVIDO"]
            if not pend:
                return {"tipo": "aviso", "msg": "Devolucao ja registrada.", "item": dict(itens[0])}
            r, total = None, 0.0
            for i in pend:
                r = ev(i, "DEVOLVIDO")
                total += custo_de(c, i["sku"]) or 0
            sem_sku = any(not i["sku"] for i in pend)
            msg = f"Devolucao registrada ({len(pend)} item(ns))"
            msg += " - SKU desconhecido: completar no painel" if sem_sku else (f" - custo R$ {total:.2f}".replace(".", ",") if total else "")
            return {"tipo": "aviso" if sem_sku else "ok", "msg": msg, "item": r}
        return {"tipo": "erro", "msg": "Posto invalido."}


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
        elif et == "EXPEDIDO":
            st = "EXPEDIDO"
        elif et == "DEVOLVIDO":
            st = "DEVOLVIDO"
    c.execute("UPDATE itens SET status=?, falta_material=?, atualizado_em=? WHERE id=?", (st, falta, agora(), iid))


# ------------------------------------------------------------------ painel
def dia_utc(data):
    d = datetime.strptime(data, "%Y-%m-%d").replace(tzinfo=BR)
    return d.astimezone(timezone.utc).isoformat(), (d + timedelta(days=1)).astimezone(timezone.utc).isoformat()


def painel(data):
    ini, fim = dia_utc(data)
    with conn() as c:
        # itens do dia = criados no dia OU com movimento no dia OU ainda nao expedidos
        itens = [dict(r) for r in c.execute("""SELECT * FROM itens WHERE (criado_em>=? AND criado_em<?)
            OR id IN (SELECT item_id FROM eventos WHERE em>=? AND em<? AND desfeito=0)
            OR (status NOT IN ('EXPEDIDO','DEVOLVIDO') AND criado_em<?) ORDER BY canal, etiqueta, id""", (ini, fim, ini, fim, fim))]
        cont = {e: 0 for e in ETAPAS}
        for i in itens:
            cont[i["status"]] += 1
        por_canal = por_grupo_status(itens, ETAPAS)
        evs = [dict(r) for r in c.execute("""SELECT e.*, k.nome FROM eventos e LEFT JOIN colaboradores k
             ON k.id=e.colaborador_id WHERE em>=? AND em<? AND desfeito=0 ORDER BY e.id""", (ini, fim))]
        equipe = {}
        ultimo_grav = {}
        for e in evs:
            if e["colaborador_id"] is None:
                continue  # marcacao automatica (estoque), nao e de ninguem da equipe
            p = equipe.setdefault(e["nome"] or "?", {"separados": 0, "gravados": 0, "expedidos": 0,
                                                     "min_gravacao": [], "faltas": 0, "primeiro": e["em"], "ultimo": e["em"]})
            p["ultimo"] = e["em"]
            if e["etapa"] == "SEPARADO": p["separados"] += 1
            if e["etapa"] == "EXPEDIDO": p["expedidos"] += 1
            if e["etapa"] == "FALTA_MATERIAL": p["faltas"] += 1
            if e["etapa"] == "GRAVACAO_INICIO":
                # tempo por peca = intervalo entre bipes seguidos do mesmo gravador (ignora pausas > 30 min)
                p["gravados"] += 1
                ant = ultimo_grav.get(e["nome"])
                if ant:
                    m = (datetime.fromisoformat(e["em"]) - datetime.fromisoformat(ant)).total_seconds() / 60
                    if 0 < m <= 30:
                        p["min_gravacao"].append(m)
                ultimo_grav[e["nome"]] = e["em"]
        for p in equipe.values():
            m = p.pop("min_gravacao")
            p["media_gravacao_min"] = round(sum(m) / len(m), 1) if m else None
        alertas = []
        for i in itens:
            if i["falta_material"]:
                alertas.append({"tipo": "FALTA DE MATERIAL", "item": i})
        for e in evs:
            if e["alerta"] and e["alerta"] != "falta de material":
                alertas.append({"tipo": e["alerta"].upper(), "item": next((i for i in itens if i["id"] == e["item_id"]), {"pedido": "?"})})
        return {"data": data, "contagem": cont, "total": len(itens), "por_canal": por_canal,
                "plataformas": por_plataforma(itens), "equipe": equipe, "alertas": alertas, "itens": itens}


def operacao():
    """Visao geral SEM dados por funcionario (para a TV da operacao)."""
    hoje = datetime.now(BR).strftime("%Y-%m-%d")
    d = painel(hoje)
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
            "plataformas": d["plataformas"],
            "previsao": prev, "falta_material": sum(1 for i in itens if i["falta_material"])}


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
    """Lista (colaborador, sku, minutos) - tempo de cada peca = ate o proximo bipe do mesmo gravador (<= 30 min)."""
    q = """SELECT e.colaborador_id, k.nome, e.em, UPPER(COALESCE(i.sku,'')) sku FROM eventos e JOIN itens i ON i.id=e.item_id
           LEFT JOIN colaboradores k ON k.id=e.colaborador_id
           WHERE e.etapa='GRAVACAO_INICIO' AND e.desfeito=0 AND e.em>=?""" + (" AND e.em<?" if fim else "") + \
        " ORDER BY e.colaborador_id, e.em"
    rows = c.execute(q, (ini, fim) if fim else (ini,)).fetchall()
    out = []
    for a, b in zip(rows, rows[1:]):
        if a[0] != b[0]:
            continue
        m = (datetime.fromisoformat(b[2]) - datetime.fromisoformat(a[2])).total_seconds() / 60
        if 0 < m <= 30:
            out.append((a[1] or "?", a[3] or "(sem SKU)", m))
    return out


def tempos_por_material(c, dias):
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
        if p in ("/operacao", "/tv"):
            return self._pagina("operacao.html")
        if p == "/api/operacao":
            if not (self._admin() or hmac.compare_digest(self.headers.get("X-Chave", ""), STATION_KEY)):
                return self._envia(403, {"erro": "sem acesso"})
            return self._envia(200, operacao())
        if p == "/sair":
            return self._envia(302, "", extra={"Location": "/painel",
                               "Set-Cookie": "cb_admin=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"})
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
        if p == "/api/compra/sugestao":
            return self._envia(200, sugestao_compra(int(q["dias"]) if (q.get("dias") or "").isdigit() else None))
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
                return self._envia(200, [dict(r) for r in c.execute("SELECT * FROM colaboradores WHERE COALESCE(excluido,0)=0 ORDER BY ativo DESC, nome")])
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
        try:
            d = self._json()
        except Exception:
            return self._envia(400, {"erro": "json invalido"})
        if p == "/login":
            if hmac.compare_digest(str(d.get("senha", "")), ADMIN_PASSWORD):
                return self._envia(200, {"ok": True}, extra={
                    "Set-Cookie": f"cb_admin={assinatura()}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000"})
            return self._envia(403, {"erro": "senha incorreta"})
        if p == "/api/bipe":
            if not hmac.compare_digest(self.headers.get("X-Chave", ""), STATION_KEY):
                return self._envia(403, {"tipo": "erro", "msg": "Chave do posto invalida."})
            return self._envia(200, bipar(d.get("posto"), d.get("codigo"), d.get("operador"), d.get("modo")))
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
            it["nomes"] = [n.strip() for n in str(d.get("nomes", "")).split("|") if n.strip()]
            it["personalizado"] = bool(d.get("personalizado", True))
            it["seq"] = "M" + agora()
            return self._envia(200, importar_lote({"lote": "MANUAL", "itens": [it]}))
        if p == "/api/itens/editar":
            with _lock, conn() as c:
                c.execute("UPDATE itens SET sku=?, atualizado_em=? WHERE id=?",
                          (str(d.get("sku", "")).strip().upper(), agora(), int(d["id"])))
            return self._envia(200, {"ok": True})
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
    return s_, cn


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


def baixar_estoque(c, iid):
    """Baixa do estoque as pecas da etiqueta quando o material sai para a separacao (uma vez so por etiqueta)."""
    r = c.execute("SELECT * FROM itens WHERE id=?", (iid,)).fetchone()
    if not r or r["lote"] == "DEVOLUCAO" or _ja_no_snapshot(c, dict(r)):
        return
    for sku, cor, qtd in _pecas_do_item(dict(r)):
        sku, cn = estoque_chave(sku, cor)
        if not sku or sku.startswith("("):
            continue
        c.execute("INSERT OR IGNORE INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                  (agora(), sku, cn, -qtd, "ETIQUETA", f"ETQ|{iid}|{sku}|{cn}", f"pedido {r['pedido']}"))


def _reservado(c, sku, cn, nivel, antes_de):
    """Unidades de etiquetas que entraram antes desta e ainda nao foram separadas (vao sair da prateleira)."""
    tot = 0
    for i in c.execute("SELECT * FROM itens WHERE status='AGUARDANDO' AND id<? AND COALESCE(lote,'')<>'DEVOLUCAO'", (antes_de,)):
        if _ja_no_snapshot(c, dict(i)):
            continue
        for s_, c_, q in _pecas_do_item(dict(i)):
            k = estoque_chave(s_, c_)
            if k[0] == sku and (nivel == "sku" or k[1] == cn):
                tot += q
    return tot


def checar_falta(c, iid):
    """Etiqueta nova: se o que tem na prateleira (menos o que ja esta reservado) nao cobre, marca NAO TEM sozinho."""
    r = c.execute("SELECT * FROM itens WHERE id=?", (iid,)).fetchone()
    if not r or r["lote"] == "DEVOLUCAO" or r["status"] != "AGUARDANDO" or r["falta_material"] or _ja_no_snapshot(c, dict(r)):
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
    feitos, erros = 0, []
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
            c.execute("INSERT INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                      (agora(), sku, cor, qtd - atual, "CONTAGEM", f"CONT|{sku}|{cor}|{agora()}|{secrets.token_hex(3)}",
                       tipo_obs or f"contagem: {qtd:g}"))
            feitos += 1
    return {"ok": True, "feitos": feitos, "erros": erros}


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
    url = base + "?" + urlencode({"cnpj": cnpj, "token": tok})
    ini = datetime.now()
    try:
        with urllib.request.urlopen(url, timeout=90) as r:
            dados = json.loads(r.read())
    except Exception as e:
        msg = re.sub(r"token=[^&\s]+", "token=***", str(e))
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


def xbz_de(c, sku, cor=""):
    """Preco de custo e estoque da XBZ para o produto (e a cor, se achar)."""
    sku = sku_base(sku)
    rows = c.execute("SELECT * FROM xbz WHERE codigo=?", (sku,)).fetchall()
    if not rows:
        return None
    cn = cor_norm(cor)
    sel = [r for r in rows if cn and (r["cor"] == cn or cn.startswith(r["cor"] + " ") or r["cor"].startswith(cn))] or rows
    precos = [r["preco"] for r in sel if r["preco"]]
    return {"preco": round(sum(precos) / len(precos), 2) if precos else None, "estoque": sum(r["estoque"] for r in sel),
            "nome": rows[0]["nome"], "cor_xbz": ", ".join(sorted({r["cor"] for r in sel})),
            "reposicao": min((r["reposicao"] for r in sel if r["reposicao"] and not r["reposicao"].startswith("0001")), default="")}


def estoque():
    """Uma pagina so: saldo, saidas, cobertura, quanto comprar, preco e estoque da XBZ."""
    hoje = datetime.now(timezone.utc)
    d7, d15 = (hoje - timedelta(days=7)).isoformat(), (hoje - timedelta(days=15)).isoformat()
    with conn() as c:
        linhas = {}
        for r in c.execute("""SELECT sku, cor, SUM(qtd) saldo,
                 -SUM(CASE WHEN tipo='ETIQUETA' AND em>=? THEN qtd ELSE 0 END) s7,
                 -SUM(CASE WHEN tipo='ETIQUETA' AND em>=? THEN qtd ELSE 0 END) s15,
                 MAX(CASE WHEN tipo='CONTAGEM' THEN em END) contado,
                 SUM(CASE WHEN tipo IN ('CONTAGEM','DISTRIBUI','ENTRADA_NF','AJUSTE') THEN 1 ELSE 0 END) conhecido,
                 SUM(CASE WHEN tipo='ENTRADA_NF' AND em>=? THEN qtd ELSE 0 END) e15
                 FROM estoque_mov GROUP BY sku, cor""", (d7, d15, d15)):
            linhas[(r["sku"], r["cor"])] = dict(r)
        pend = {}
        # reservado = etiquetas que ainda nao foram para a separacao (o material ainda esta na prateleira)
        for i in c.execute("SELECT * FROM itens WHERE status='AGUARDANDO' AND COALESCE(lote,'')<>'DEVOLUCAO'"):
            if _ja_no_snapshot(c, dict(i)):
                continue
            for sku, cor, q in _pecas_do_item(dict(i)):
                k = estoque_chave(sku, cor)
                if k[0] and not k[0].startswith("("):
                    pend[k] = pend.get(k, 0) + q
        for k in pend:
            linhas.setdefault(k, {"sku": k[0], "cor": k[1], "saldo": 0, "s7": 0, "s15": 0, "contado": None,
                                  "conhecido": 0, "e15": 0})
        com_cor = {k[0] for k in linhas if k[1]}
        out = []
        for (sku, cor), r in linhas.items():
            sem_cor = cor == "" and sku in com_cor  # produto com cores: entrada sem cor ainda precisa ser distribuida
            media = (r["s15"] or 0) / 15
            fisico = r["saldo"] or 0
            saldo = fisico - pend.get((sku, cor), 0)  # disponivel
            x = xbz_de(c, sku, cor) or {}
            cu = preco_xbz(c, sku, cor)
            conhecido = bool(r["conhecido"])
            comprar = max(0, round(media * ESTOQUE_DIAS_COMPRA - saldo)) if not sem_cor and conhecido else 0
            out.append({"sku": sku, "cor": cor or ("(cor a definir)" if sem_cor else "PADRAO"), "sem_cor": sem_cor,
                        "saldo": round(saldo, 2),
                        "fisico": round(fisico, 2),
                        "pendente_hoje": pend.get((sku, cor), 0), "saidas_7d": r["s7"] or 0, "saidas_15d": r["s15"] or 0,
                        "media_dia": round(media, 1), "dias": round(saldo / media, 1) if media > 0 and saldo > 0 else (0 if saldo <= 0 else None),
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
    return {"itens": out, "dias_compra": ESTOQUE_DIAS_COMPRA, "xbz": _xbz_status, "alertas_xbz": alertas_xbz,
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
        habitos = {}
        chaves = set(vendas) | {k for k, v in est.items() if v["pendente_hoje"]} | \
            {k for k, v in est.items() if k[0] in ESTRATEGICOS and v.get("conhecido")}
        for sku, cor in sorted(chaves):
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
            alvo = media * horizonte * fatores.get(sku, 1.0)
            precisa = alvo + reservado - fisico  # estoque negativo aumenta a compra (falta fisica)
            x = xbz_de(c, sku, cor) or {}
            preco = preco_xbz(c, sku, cor)
            linha = {"sku": sku, "cor": cor, "nome": x.get("nome", e.get("nome", "")), "media_dia": round(media, 1),
                     "na_prateleira": fisico, "reservado": reservado, "ciclo_dias": h["ciclo"], "multiplo": h["multiplo"],
                     "projecao": round(alvo), "fator": round(fatores.get(sku, 1.0), 2), "preco": preco,
                     "xbz_estoque": x.get("estoque"), "estrategico": sku in ESTRATEGICOS}
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
    out.sort(key=lambda l: (not l.get("critico"), l["sku"], l["cor"]))
    total = round(sum(l.get("total", 0) for l in out), 2)
    return {"itens": out, "sem_contagem": sem_contagem, "fora": fora, "total": total,
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
            if it["cprod"] and it["qtd"] and not any(x and x in (n["conta"] or "").upper() for x in ESTOQUE_IGNORAR):
                # entrada no estoque proprio (a nota nao traz a cor: entra como "cor a definir")
                c.execute("INSERT OR IGNORE INTO estoque_mov(em,sku,cor,qtd,tipo,ref,obs) VALUES(?,?,?,?,?,?,?)",
                          (agora(), estoque_chave(codigo_xbz_nf(it["cprod"]))[0], "", it["qtd"], "ENTRADA_NF",
                           f"NF|{n['chave'] or n['nf']}|{it['item']}", f"NF {n['nf']} {n['conta']}"))
    return {"ok": True, "nf": n["nf"], "data": n["data"], "conta": n["conta"], "itens": len(n["itens"]), "novos": novos,
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
        q = "SELECT * FROM itens WHERE ((criado_em>=? AND criado_em<?)"
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
    if tid or "TIKTOK" in T or "TIK TOK" in T:
        canal = "TIKTOK"
    elif erid or re.search(r"ENTREGA\s+(DIRETA|R[AÁ]PIDA)", T):
        canal, envio = "SHOPEE", "ENTREGA DIRETA"
    elif sn or ras or "DANFE" in T or "SHOPEE" in T or "SPX" in T:
        canal, envio = "SHOPEE", "SHOPEE XPRESS"
    else:
        canal = "OUTROS"
    # itens do pedido (rodape do UpSeller): "1. 18726I-Personalizado (Rosa Claro, Personalizado com Nome) / ..."
    rod = re.split(r"#UPPUS\d+[^\n]*\n", t, maxsplit=1)
    skus, cores, pers, pecas = [], [], [], []
    if len(rod) == 2:
        for it in re.split(r"(?m)^\s*\d+\.\s*", rod[1])[1:]:
            mm = re.match(r"\s*([A-Za-z0-9]+)", it)
            mq = re.search(r"\*\s*(\d+)\s*\)?\s*$", it.strip())
            par0 = [x for x in re.findall(r"\(([^()]*)\)", it) if "," in x]
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
            par = [x for x in re.findall(r"\(([^()]*)\)", it) if "," in x]
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
    etq = re.search(r"ETIQUETA\s*N?[ºo°.]?\s*(\d+)", t, re.I)
    if etq:
        personalizado = True  # etiqueta numerada da folha de gravacao = vai para a gravacao
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


if __name__ == "__main__":
    iniciar_db()
    try:
        n = carregar_abertura()
        if n:
            print(f"Estoque: saldo inicial carregado ({n} produtos/cores)", flush=True)
    except Exception as e:
        print("Estoque: erro no saldo inicial:", e, flush=True)
    if _email_status["ativo"] or (XBZ_EMAIL_USUARIO and XBZ_EMAIL_SENHA):
        threading.Thread(target=_email_loop, daemon=True).start()
    if os.environ.get("XBZ_TOKEN"):
        threading.Thread(target=_xbz_loop, daemon=True).start()
    porta = int(os.environ.get("PORT", "8000"))
    print(f"Central Boni rodando na porta {porta} (banco: {DB})", flush=True)
    ThreadingHTTPServer(("0.0.0.0", porta), H).serve_forever()
