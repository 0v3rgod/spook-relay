#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""HORAS — registro de horas trabalhadas do Spook (relay).

Mesmo desenho de PAD/TASKS: UM arquivo por dia em
`gdrive:SPOOK/horas/<YYYY-MM-DD>.json`, outbox de pendentes e espelho local
(`data/horas/`). A leitura e' instantanea do espelho; upload/sync numa thread
de fundo.

Um dia = um registro:
    {
      "id": "2026-11-15", "data": "2026-11-15",
      "entrada": <epoch ms>, "saida": <epoch ms>,
      "intervalos": [{"inicio": ms, "fim": ms}, ...],
      "descricao": "...", "tipo": "normal" | "fim_semana" | "feriado",
      "local": {"lat": .., "lon": .., "precisao": .., "texto": "..."},
      "em": <epoch ms do cliente>, "device": "poco-x7" | "arch-desktop",
      "removido": false, "removido_em": 0
    }

LWW por `em`; apagar = lapide (o arquivo fica no Drive).
"""
import fcntl
import json
import os
import threading
import time
from pathlib import Path

import hub_store

HORAS = "horas"
_HORAS_DIR = Path(os.environ.get("SPOOK_HORAS_DIR", "/home/ubuntu/spook-relay/data/horas"))
_PEND_FILE = Path(os.environ.get("SPOOK_HORAS_PENDING", "/tmp/spook_horas_pending.json"))
_PEND_LOCK_PATH = "/tmp/spook_horas_pending.lock"
_LOCK = threading.RLock()
_MAX_DESC = 4000
_MAX_INTERVALOS = 12
_TIPOS = {"normal", "fim_semana", "feriado"}


# ---------------------------------------------------------------- utilidades
def _agora() -> int:
    return int(time.time() * 1000)


def _caminho(hid: str) -> Path:
    return _HORAS_DIR / f"{hid}.json"


def _arquivo_remoto(hid: str) -> str:
    return f"{HORAS}/{hid}.json"


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
    fd = _p_lock()
    try:
        d = _p_carregar()
        d[rec["id"]] = rec
        _p_salvar(d)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()


def _despin(hid: str):
    fd = _p_lock()
    try:
        d = _p_carregar()
        if d.pop(hid, None) is not None:
            _p_salvar(d)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()


# --------------------------------------------------------------- persistencia
def _gravar_espelho(rec: dict):
    try:
        _HORAS_DIR.mkdir(parents=True, exist_ok=True)
        tmp = _caminho(rec["id"]).with_suffix(".tmp")
        tmp.write_text(json.dumps(rec, ensure_ascii=False))
        tmp.replace(_caminho(rec["id"]))
    except Exception:
        pass


def _ler_espelho(hid: str):
    try:
        return json.loads(_caminho(hid).read_text())
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
    """Sobe pendentes; so desafoga quando a LISTA do Drive refletir o upload."""
    pend = _p_carregar()
    for hid in list(pend):
        rec = pend.get(hid)
        if not rec:
            continue
        if _upload(rec) and hub_store._remoto_tem(_arquivo_remoto(hid)):
            _despin(hid)


def _sync():
    fd = hub_store._acquire_sync_lock()
    if fd is None:
        return False
    try:
        _flush()
        _HORAS_DIR.mkdir(parents=True, exist_ok=True)
        hub_store._run(
            ["sync", "--fast-list", "--update", hub_store._base(HORAS), str(_HORAS_DIR)],
            check=False,
            timeout=120,
        )
        # o sync poda o que ainda nao subiu: re-garante no espelho
        for rec in _p_carregar().values():
            _gravar_espelho(rec)
        return True
    except Exception:
        return False
    finally:
        hub_store._release_sync_lock(fd)


def _loop():
    time.sleep(2.0)
    try:
        _HORAS_DIR.mkdir(parents=True, exist_ok=True)
        hub_store._run(["mkdir", hub_store._base(HORAS)], check=False, timeout=60)
    except Exception:
        pass
    while True:
        try:
            _sync()
        except Exception:
            pass
        time.sleep(15)


def iniciar():
    """Chamado no import do relay (uma vez)."""
    threading.Thread(target=_loop, daemon=True, name="horas-sync").start()


# ------------------------------------------------------------------- modelo
def _int(v, padrao=0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return padrao


def _normaliza(rec: dict) -> dict:
    agora = _agora()
    data = str(rec.get("data") or rec.get("id") or "").strip()[:10]
    hid = str(rec.get("id") or data).strip()[:64]
    intervalos = []
    for it in (rec.get("intervalos") or [])[:_MAX_INTERVALOS]:
        if not isinstance(it, dict):
            continue
        ini = _int(it.get("inicio"))
        fim = _int(it.get("fim"))
        if ini <= 0:
            continue
        intervalos.append({"inicio": ini, "fim": max(fim, 0)})
    local = rec.get("local")
    if not isinstance(local, dict):
        local = {}
    tipo = str(rec.get("tipo") or "normal").strip().lower()
    if tipo not in _TIPOS:
        tipo = "normal"
    return {
        "id": hid,
        "data": data or hid,
        "entrada": _int(rec.get("entrada")),
        "saida": _int(rec.get("saida")),
        "intervalos": intervalos,
        "descricao": str(rec.get("descricao") or "")[:_MAX_DESC],
        "tipo": tipo,
        "local": {
            "lat": float(local.get("lat") or 0.0),
            "lon": float(local.get("lon") or 0.0),
            "precisao": float(local.get("precisao") or 0.0),
            "texto": str(local.get("texto") or "")[:200],
        },
        "em": _int(rec.get("em")) or agora,
        "device": str(rec.get("device") or "android")[:64],
        "removido": bool(rec.get("removido")),
        "removido_em": _int(rec.get("removido_em") or rec.get("removidoEm") or 0),
        "atualizado_em": agora,
    }


def listar() -> list:
    """Todos os dias (inclusive lapides), mais recentes primeiro."""
    dias = {}
    try:
        if _HORAS_DIR.exists():
            for f in _HORAS_DIR.glob("*.json"):
                try:
                    r = json.loads(f.read_text())
                    if r.get("id"):
                        dias[r["id"]] = r
                except Exception:
                    continue
    except Exception:
        pass
    for hid, rec in _p_carregar().items():
        if rec.get("id"):
            dias[hid] = rec
    lista = list(dias.values())
    lista.sort(key=lambda x: (str(x.get("data") or ""), x.get("em", 0)), reverse=True)
    return lista


def obter(hid: str):
    return _ler_espelho(hid) or _p_carregar().get(hid)


def salvar(rec: dict) -> dict:
    """Grava um dia (LWW por `em`). Devolve o registro efetivamente gravado."""
    novo = _normaliza(rec)
    if not novo.get("id"):
        return {"ok": False, "erro": "id_obrigatorio"}
    with _LOCK:
        atual = obter(novo["id"])
        if atual is not None:
            if novo["em"] <= _int(atual.get("em")):
                return {"ok": True, "dia": atual, "antiga": True}
            if novo["removido"] and not atual.get("removido"):
                novo["removido_em"] = novo["removido_em"] or _agora()
        novo["em"] = max(novo["em"], _int((atual or {}).get("em")) + 1)
        _gravar_espelho(novo)
        _pin(novo)
    return {"ok": True, "dia": novo}


def apagar(hid: str, device: str = "android") -> dict:
    with _LOCK:
        atual = obter(hid)
        if not atual:
            return {"ok": True, "dia": {"id": hid, "removido": True}}
        rec = dict(atual)
        rec.update(
            {
                "removido": True,
                "removido_em": _agora(),
                "em": max(_agora(), _int(atual.get("em")) + 1),
                "device": device or "android",
            }
        )
        _gravar_espelho(rec)
        _pin(rec)
        return {"ok": True, "dia": rec}


threading.Thread(target=_loop, daemon=True).start()
