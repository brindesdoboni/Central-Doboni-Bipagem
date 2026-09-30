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
        """)
        c.execute("CREATE TABLE IF NOT EXISTS meta(chave TEXT PRIMARY KEY, valor TEXT)")
        if "excluido" not in [r[1] for r in c.execute("PRAGMA table_info(colaboradores)")]:
            c.execute("ALTER TABLE colaboradores ADD COLUMN excluido INTEGER DEFAULT 0")
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
def importar_lote(dados):
    lote = dados.get("lote") or datetime.now(BR).strftime("%Y%m%d-%H%M")
    n_novo = n_atual = 0
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
                          loja=it.get("loja") or "", sku=it.get("sku") or "", cor=it.get("cor") or "",
                          nomes=nomes, fonte=it.get("fonte") or "", tipo=tipo, obs=it.get("obs") or "",
                          personalizado=pers, atualizado_em=agora())
            ant = c.execute("SELECT id FROM itens WHERE chave=?", (chave,)).fetchone()
            if ant:
                iid = ant["id"]
                sets = ", ".join(f"{k}=?" for k in campos if k != "rastreio" or campos["rastreio"])
                vals = [v for k, v in campos.items() if k != "rastreio" or campos["rastreio"]]
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
    return {"ok": True, "lote": lote, "novos": n_novo, "atualizados": n_atual}


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
            return dict(c.execute("SELECT * FROM itens WHERE id=?", (it["id"],)).fetchone())

        if modo == "FALTA":
            it = itens[0]
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
        por_canal = {}
        for i in itens:
            d = por_canal.setdefault(i["canal"] or "-", {e: 0 for e in ETAPAS})
            d[i["status"]] += 1
        evs = [dict(r) for r in c.execute("""SELECT e.*, k.nome FROM eventos e LEFT JOIN colaboradores k
             ON k.id=e.colaborador_id WHERE em>=? AND em<? AND desfeito=0 ORDER BY e.id""", (ini, fim))]
        equipe = {}
        ultimo_grav = {}
        for e in evs:
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
                "equipe": equipe, "alertas": alertas, "itens": itens}


def operacao():
    """Visao geral SEM dados por funcionario (para a TV da operacao)."""
    hoje = datetime.now(BR).strftime("%Y-%m-%d")
    d = painel(hoje)
    itens = [i for i in d["itens"] if i["status"] != "DEVOLVIDO"]
    cont = {e: 0 for e in ETAPAS if e != "DEVOLVIDO"}
    por_canal = {}
    for i in itens:
        cont[i["status"]] += 1
        por_canal.setdefault(i["canal"] or "-", {e: 0 for e in cont})[i["status"]] += 1
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
            "previsao": prev, "falta_material": sum(1 for i in itens if i["falta_material"])}


def _num(v):
    t = str(v or "").strip().replace("R$", "").replace(" ", "")
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    return float(t)


def custo_de(c, sku):
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
                GROUP BY k.nome, e.etapa""", (ini, fim)):
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
        if p == "/api/lotes":
            if not hmac.compare_digest(self.headers.get("X-Token", ""), API_TOKEN):
                return self._envia(403, {"erro": "token invalido"})
            return self._envia(200, importar_lote(d))
        if not self._admin():
            return self._envia(401, {"erro": "login necessario"})
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
            return self._envia(200, {"ok": True, "excluidos": len(ids)})
        if p == "/api/itens/adicionar":
            if not str(d.get("pedido", "")).strip():
                return self._envia(400, {"erro": "informe o pedido"})
            it = {k: d.get(k, "") for k in ("pedido", "rastreio", "canal", "loja", "sku", "cor", "fonte", "obs")}
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
                if d.get("excluir"):
                    c.execute("DELETE FROM pausas WHERE id=?", (d["id"],))
                elif re.fullmatch(r"\d{1,2}:\d{2}", d.get("hora", "")):
                    h, m = d["hora"].split(":")
                    c.execute("INSERT INTO pausas(nome,hora,pessoas) VALUES(?,?,?)",
                              (d.get("nome") or "Pausa", f"{int(h):02d}:{m}", d.get("pessoas", "")))
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


if __name__ == "__main__":
    iniciar_db()
    porta = int(os.environ.get("PORT", "8000"))
    print(f"Central Boni rodando na porta {porta} (banco: {DB})", flush=True)
    ThreadingHTTPServer(("0.0.0.0", porta), H).serve_forever()
