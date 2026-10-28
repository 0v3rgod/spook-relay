#!/usr/bin/env python3
"""PAD — notas do Spook (relay).

Mesmo desenho das tasks (um arquivo por nota em gdrive:SPOOK/pad/<id>.json,
outbox de pendentes, espelho local), mas com as ideias do Sync do Obsidian:

  * hash sha256 do texto (dedupe: mesmo conteudo = nada a fazer)
  * base_hash: quem edita manda o hash da versao em que se baseou; se o
    servidor ja estiver em outro hash -> conflito (o cliente faz merge 3 vias)
  * historico de versoes dentro do proprio arquivo (ultimas 20) + restaurar
  * lapide (removido/removido_em): apagar = marcar, o arquivo FICA no Drive
  * em (epoch ms do cliente) para LWW e `versao` (contador do servidor) para
    desempate/historico

Leitura e' sempre do espelho local (instantanea). Upload/sync ficam numa
thread de fundo; nada aqui bloqueia o socket.
"""
import fcntl
import hashlib
import json
import os
import threading
import time
import uuid
from pathlib import Path

import hub_store

PAD = "pad"
_PAD_DIR = Path(os.environ.get("SPOOK_PAD_DIR", "/home/ubuntu/spook-relay/data/pad"))
_PEND_FILE = Path(os.environ.get("SPOOK_PAD_PENDING", "/tmp/spook_pad_pending.json"))
_PEND_LOCK_PATH = "/tmp/spook_pad_pending.lock"
_LOCK = threading.RLock()
_MAX_VERSOES = 20
_MAX_TEXTO = 256 * 1024


# ---------------------------------------------------------------- utilidades
def _agora() -> int:
    return int(time.time() * 1000)


def _hash(texto: str) -> str:
    return hashlib.sha256((texto or "").encode("utf-8")).hexdigest()


def _caminho(nid: str) -> Path:
    return _PAD_DIR / f"{nid}.json"


def _arquivo_remoto(nid: str) -> str:
    return f"{PAD}/{nid}.json"


def _p_lock():
    f = open(_PEND_LOCK_PATH, "a+")
    fcntl.flock(f, fcntl.LOCK_EX)
    return f


def _p_carregar() -> dict:
    try:
        if _PEND_FILE.exists():
            d = json.loads(_PEND_FILE.read_text())
            if isinstance(d, dict):
                return d
    except Exception:
        pass
    return {}


def _p_salvar(d: dict):
    try:
        tmp = _PEND_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(d, ensure_ascii=False))
        tmp.replace(_PEND_FILE)
    except Exception:
        pass


def _pin(rec: dict):
    """Marca a nota como pendente de upload (create/update/tombstone)."""
    fd = _p_lock()
    try:
        d = _p_carregar()
        d[rec["id"]] = rec
        _p_salvar(d)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()


def _despin(nid: str):
    fd = _p_lock()
    try:
        d = _p_carregar()
        if d.pop(nid, None) is not None:
            _p_salvar(d)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()


# --------------------------------------------------------------- persistencia
def _gravar_espelho(rec: dict):
    try:
        _PAD_DIR.mkdir(parents=True, exist_ok=True)
        tmp = _caminho(rec["id"]).with_suffix(".tmp")
        tmp.write_text(json.dumps(rec, ensure_ascii=False))
        tmp.replace(_caminho(rec["id"]))
    except Exception:
        pass


def _ler_espelho(nid: str):
    try:
        return json.loads(_caminho(nid).read_text())
    except Exception:
        return None


def _upload(rec: dict) -> bool:
    try:
        corpo = json.dumps(rec, ensure_ascii=False).encode("utf-8")
        hub_store._run(["rcat", hub_store._base(_arquivo_remoto(rec["id"]))], input_bytes=corpo)
        return True
    except Exception:
        return False


def _flush():
    """Sobe tudo que esta pendente. So remove do outbox quando o Drive lista o
    arquivo (a listagem atrasa alguns segundos; sem isso o sync seguinte poda o
    espelho e a nota 'some')."""
    pend = _p_carregar()
    for nid in list(pend):
        rec = pend.get(nid)
        if not rec:
            continue
        if _upload(rec) and hub_store._remoto_tem(_arquivo_remoto(nid)):
            _despin(nid)


def _sync():
    fd = hub_store._acquire_sync_lock()
    if fd is None:
        return False
    try:
        _flush()
        _PAD_DIR.mkdir(parents=True, exist_ok=True)
        hub_store._run(
            ["sync", "--fast-list", "--update", hub_store._base(PAD), str(_PAD_DIR)],
            check=False,
            timeout=120,
        )
        # re-garante no espelho o que ainda nao subiu (o sync pruna)
        for rec in _p_carregar().values():
            _gravar_espelho(rec)
        return True
    except Exception:
        return False
    finally:
        hub_store._release_sync_lock(fd)


def _loop():
    time.sleep(1.0)
    while True:
        try:
            _flush()
            _sync()
        except Exception:
            pass
        time.sleep(6)


# ------------------------------------------------------------------- leitura
def listar() -> list:
    """Todas as notas (inclusive lapides), mais novas primeiro."""
    notas = {}
    try:
        if _PAD_DIR.exists():
            for f in _PAD_DIR.glob("*.json"):
                try:
                    r = json.loads(f.read_text())
                    if r.get("id"):
                        notas[r["id"]] = r
                except Exception:
                    continue
    except Exception:
        pass
    for nid, rec in _p_carregar().items():
        if rec.get("id"):
            notas[nid] = rec
    lista = []
    for r in notas.values():
        item = dict(r)
        item.pop("historico", None)
        item["n_versoes"] = len(r.get("historico") or [])
        lista.append(item)
    lista.sort(key=lambda x: (x.get("em", 0), x.get("versao", 0)), reverse=True)
    return lista


def obter(nid: str):
    rec = _ler_espelho(nid) or _p_carregar().get(nid)
    return rec


def _proxima_versao(atual) -> int:
    base = int((atual or {}).get("versao", 0)) + 1
    return max(base, 1)


def _empilha_historico(rec: dict, anterior: dict):
    if not anterior:
        return
    hist = list(rec.get("historico") or [])
    hist.append(
        {
            "em": anterior.get("em", 0),
            "device": anterior.get("device", ""),
            "hash": anterior.get("hash", ""),
            "texto": anterior.get("texto", ""),
            "versao": anterior.get("versao", 0),
        }
    )
    rec["historico"] = hist[-_MAX_VERSOES:]


# ------------------------------------------------------------------ escrita
def criar(titulo: str, texto: str = "", device: str = "android", nid: str = None) -> dict:
    agora = _agora()
    nid = nid or str(uuid.uuid4())
    rec = {
        "id": nid,
        "titulo": (titulo or "sem titulo").strip()[:120] or "sem titulo",
        "texto": (texto or "")[:_MAX_TEXTO],
        "hash": _hash(texto),
        "em": agora,
        "device": device or "android",
        "criado_em": agora,
        "versao": 1,
        "removido": False,
        "removido_em": 0,
        "historico": [],
    }
    with _LOCK:
        _gravar_espelho(rec)
        _pin(rec)
    return rec


def salvar(nid: str, titulo: str, texto: str, em: int, base_hash: str, device: str) -> dict:
    """Grava (LWW por em) e devolve {nota, conflito}.

    conflito=True  -> o servidor tem outra versao desde o base_hash do cliente;
                      devolve a nota atual para o cliente fazer merge 3 vias.
    igual=True     -> o texto e' identico ao que ja estava; nada mudou.
    antiga=True    -> o `em` enviado e' mais velho que o atual; ignorado.
    """
    texto = (texto or "")[:_MAX_TEXTO]
    h = _hash(texto)
    with _LOCK:
        atual = obter(nid)
        if atual is None:
            rec = criar(titulo, texto, device, nid)
            rec["em"] = em or rec["em"]
            _gravar_espelho(rec)
            _pin(rec)
            return {"nota": rec, "conflito": False}
        if atual.get("hash") == h and not atual.get("removido"):
            return {"nota": atual, "conflito": False, "igual": True}
        if em and em < int(atual.get("em", 0)) - 500:
            return {"nota": atual, "conflito": False, "antiga": True}
        if base_hash and base_hash != atual.get("hash"):
            return {"nota": atual, "conflito": True}

        rec = dict(atual)
        _empilha_historico(rec, atual)
        rec.update(
            {
                "titulo": (titulo or atual.get("titulo") or "sem titulo").strip()[:120],
                "texto": texto,
                "hash": h,
                "em": max(int(em or 0), int(atual.get("em", 0)) + 1),
                "device": device or "android",
                "versao": _proxima_versao(atual),
                "removido": False,
                "removido_em": 0,
            }
        )
        _gravar_espelho(rec)
        _pin(rec)
        return {"nota": rec, "conflito": False}


def apagar(nid: str, device: str = "android") -> dict:
    with _LOCK:
        atual = obter(nid)
        if not atual:
            return {"id": nid, "removido": True}
        rec = dict(atual)
        rec.update(
            {
                "removido": True,
                "removido_em": _agora(),
                "em": max(_agora(), int(atual.get("em", 0)) + 1),
                "device": device or "android",
                "versao": _proxima_versao(atual),
            }
        )
        _gravar_espelho(rec)
        _pin(rec)
        return rec


def historico(nid: str) -> list:
    rec = obter(nid)
    if not rec:
        return []
    hist = list(rec.get("historico") or [])
    hist.append(
        {
            "em": rec.get("em", 0),
            "device": rec.get("device", ""),
            "hash": rec.get("hash", ""),
            "texto": rec.get("texto", ""),
            "versao": rec.get("versao", 0),
            "atual": True,
        }
    )
    hist.reverse()
    return hist


def restaurar(nid: str, em: int, device: str = "android") -> dict:
    """Restaura a versao cujo `em` bate com a do historico, como NOVA versao."""
    with _LOCK:
        atual = obter(nid)
        if not atual:
            return {"ok": False, "erro": "nota nao encontrada"}
        alvo = None
        for v in atual.get("historico") or []:
            if int(v.get("em", 0)) == int(em):
                alvo = v
                break
        if alvo is None and int(atual.get("em", 0)) == int(em):
            return {"ok": True, "nota": atual}
        if alvo is None:
            return {"ok": False, "erro": "versao nao encontrada"}
        rec = dict(atual)
        _empilha_historico(rec, atual)
        rec.update(
            {
                "texto": alvo.get("texto", ""),
                "hash": _hash(alvo.get("texto", "")),
                "em": max(_agora(), int(atual.get("em", 0)) + 1),
                "device": device or "android",
                "versao": _proxima_versao(atual),
            }
        )
        _gravar_espelho(rec)
        _pin(rec)
        return {"ok": True, "nota": rec}


threading.Thread(target=_loop, daemon=True).start()
