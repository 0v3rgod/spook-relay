#!/usr/bin/env python3
import json
import os
import subprocess
import threading
import time
import uuid
from pathlib import Path

REMOTE = os.environ.get("SPOOK_REMOTE", "gdrive:SPOOK")
CHAT = "chat"
MEDIA = "media"
TASKS = "tasks"
TASKS_FILE = "tasks.json"
ORIGEM = "arch"

# Espelhos locais: rclone sync (1 chamada) + leitura de disco = muito mais rapido
# que o antigo lsjson + 1 rclone cat por mensagem.
_DADOS = Path(os.environ.get("SPOOK_DADOS", "/tmp"))
_CHAT_DIR = _DADOS / Path(os.environ.get("SPOOK_CHAT_DIR", "spook_chat_local"))
_TASKS_DIR = _DADOS / Path(os.environ.get("SPOOK_TASKS_DIR", "spook_tasks_local"))
_TASKS_CACHE = _DADOS / Path(os.environ.get("SPOOK_TASKS_CACHE", "spook_tasks_cache.json"))

# Um unico sync por vez em toda a MAQUINA, nao por processo: servidor, overlay e
# qualquer processo importando hub_store rodam refreshers concorrentes no MESMO
# espelho; 2 rclone sync simultaneos no mesmo dir clobram um ao outro (cada um
# poda o que o outro baixou, com cache de listagem do Drive). O flock serializa.
_CHAT_LOCK = threading.Lock()
_TASKS_LOCK = threading.Lock()
_REFRESHER_STARTED = False
_REFRESHER_INTERVAL = 5
_CHAT_VERSION = [0]
_ULTIMO_SYNC = [0.0]
_SYNC_LOCK_PATH = "/tmp/spook_sync.lock"

import fcntl


def _acquire_sync_lock():
    """Tenta exclusao mutua entre processos (nao-bloqueante). Retorna fd ou None."""
    fd = open(_SYNC_LOCK_PATH, "a+")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except OSError:
        fd.close()
        return None


def _release_sync_lock(fd):
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
    except (OSError, ValueError):
        pass


# Pendentes: mensagens escritas localmente (local-first) cujo upload ao Drive
# ainda nao confirmou. O rclone sync prunaria esses .json do espelho local (que
# e COMPARTILHADO entre servidor/overlay), entao sao "pinados" de volta apos cada
# sync ate o upload confirmar. Como varios processos escrevem no mesmo espelho,
# a fila fica em ARQUIVO com flock BLOQUEANTE (a fila e curta e o lock so vive
# microssegundos; quem sincroniza re-aplica TODOS, nao so os do proprio processo —
# era isso que fazia a msg sumir/voltar). Se um processo morrer segurando o lock,
# o kernel libera sozinho ao fechar o fd.
_PENDING_FILE = Path("/tmp/spook_pending.json")
_PENDING_LOCK_PATH = "/tmp/spook_pending.lock"
_PENDING = {}


def _pend_lock():
    fd = open(_PENDING_LOCK_PATH, "a+")
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


# Serializa as GRAVACOES NO DRIVE entre processos do mesmo host. O lock do
# pending (acima) so protege o arquivo de fila; sem este, servidor E overlay
# faziam copyto/rcat do mesmo nome em paralelo e o Drive guardava 2 documentos
# identicos (nome repetido) -> o chat ficava com mensagem ja gravada duas vezes.
_UPLOAD_LOCK_PATH = "/tmp/spook_flush.lock"


def _upload_lock():
    fd = open(_UPLOAD_LOCK_PATH, "a+")
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


def _carregar_pending():
    try:
        arr = json.loads(_PENDING_FILE.read_text())
        return {m["id"]: m for m in arr if isinstance(m, dict) and m.get("id")} if isinstance(arr, list) else {}
    except Exception:
        return {}


def _salvar_pending():
    try:
        tmp = _PENDING_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(list(_PENDING.values())))
        tmp.replace(_PENDING_FILE)
    except Exception:
        pass


def _pin(msg, _tipo_arquivo=None, _src=None):
    """Registra a msg como pendente no arquivo compartilhado (sobrescreve se ja
    existir; mesmo id enviado de 2 process diferentes e idempotente)."""
    item = dict(msg)
    item["_tipo_arquivo"] = _tipo_arquivo or ""
    item["_src"] = _src or ""
    fd = _pend_lock()
    try:
        _PENDING.update(_carregar_pending())
        _PENDING[msg["id"]] = item
        _salvar_pending()
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()


def _flush_pending():
    """Tenta subir TODOS os pendentes do arquivo compartilhado (de qualquer
    processo). Após cada sync, quem pegar o flock re-aplica os que sobraram.
    A gravacao no Drive e serializada com _upload_lock e so sobe o que ainda
    NAO existe (o Drive permite nomes duplicados, e 2 processos do host
    gravando o mesmo id criavam doc duplicado no chat/midia)."""
    fd = _pend_lock()
    try:
        _PENDING.update(_carregar_pending())
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
    for mid in [m for m in _PENDING.copy()]:
        fdu = _upload_lock()
        try:
            _PENDING.update(_carregar_pending())
            item = _PENDING.get(mid)
            if not item:
                continue
            msg = {k: v for k, v in item.items() if not k.startswith("_")}
            if item.get("_tipo_arquivo") and item.get("_src") and not _remoto_tem(item["arquivo"]):
                _run(["copyto", item["_src"], _base(item["arquivo"])], timeout=900)
            if not _remoto_tem(f"{CHAT}/{mid}.json"):
                _escrever_json(f"{CHAT}/{mid}.json", msg)
            if not _remoto_tem(f"{CHAT}/{mid}.json"):
                continue  # listagem ainda nao refletiu; mantem pendente (pin/merge)
            fd = _pend_lock()
            try:
                _PENDING.update(_carregar_pending())
                _PENDING.pop(mid, None)
                _salvar_pending()
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                fd.close()
        except Exception:
            continue
        finally:
            fcntl.flock(fdu, fcntl.LOCK_UN)
            fdu.close()


def pendentes_ids():
    fd = _pend_lock()
    try:
        _PENDING.update(_carregar_pending())
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
    return set(_PENDING)


def _gravar_local(msg):
    os.makedirs(_CHAT_DIR, exist_ok=True)
    try:
        (_CHAT_DIR / f"{msg['id']}.json").write_text(json.dumps(msg))
    except Exception:
        pass


def _aaplica_pendentes_locais():
    """Re-escreve no espelho local os .json que ainda constam como pendentes no
    arquivo compartilhado (protege contra o prune do rclone sync de OUTRO
    processo que tenha vencido o flock antes do upload confirmar)."""
    fd = _pend_lock()
    try:
        _PENDING.update(_carregar_pending())
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
    for item in _PENDING.values():
        msg = {k: v for k, v in item.items() if not k.startswith("_")}
        _gravar_local(msg)


def _sync_chat():
    with _CHAT_LOCK:
        fd = _acquire_sync_lock()
        if fd is None:
            return False  # outro processo ja esta sincronizando
        try:
            _flush_pending()
            _run(["sync", "--fast-list", "--update", _base(CHAT), str(_CHAT_DIR)], check=False, timeout=300)
            # o sync poda local-exclusivos; pinamos os nossos de volta p/ nao sumir
            _aaplica_pendentes_locais()
            _CHAT_VERSION[0] += 1
            _ULTIMO_SYNC[0] = time.time()
            return True
        except Exception:
            return False
        finally:
            _release_sync_lock(fd)


def _sync_tasks():
    with _TASKS_LOCK:
        fd = _acquire_sync_lock()
        if fd is None:
            return False
        try:
            _flush_tpending()
            os.makedirs(_TASKS_DIR, exist_ok=True)
            _run(["sync", "--fast-list", "--update", _base(TASKS), str(_TASKS_DIR)], check=False, timeout=120)
            _aaplica_tpend_locais()  # re-garante os nossos contra o prune
            _migra_tasks_json()      # converte a lista unica antiga, uma vez
            return True
        except Exception:
            return False
        finally:
            _release_sync_lock(fd)


def _start_refresher():
    global _REFRESHER_STARTED
    if _REFRESHER_STARTED:
        return
    _REFRESHER_STARTED = True
    _sync_chat()
    _sync_tasks()
    threading.Thread(target=_loop, daemon=True).start()


def _loop():
    while True:
        _flush_pending()  # sobe os nossos; nao depende do sync
        _sync_chat()      # so o processo que pegar o flock sincroniza
        time.sleep(1)
        _sync_tasks()
        _aaplica_pendentes_locais()  # garante que nada nosso sumiu do espelho
        time.sleep(max(2, _REFRESHER_INTERVAL - 1))


_start_refresher()


def _ler_cache_tasks():
    try:
        d = json.loads(_TASKS_CACHE.read_text())
        if isinstance(d, dict) and "tasks" in d:
            return {t["id"]: t for t in d["tasks"]}
    except Exception:
        pass
    return {}


def _salvar_cache_tasks(tasks_dict):
    tmp = _TASKS_CACHE.with_suffix(".tmp")
    tmp.write_text(json.dumps({"tasks": list(tasks_dict.values())}, ensure_ascii=False))
    tmp.replace(_TASKS_CACHE)


# Tasks segunda o mesmo padrao do chat: UM ARQUIVO POR TASK em gdrive:SPOOK/tasks/<id>.json.
# O arquivo unico tasks.json (antigo) causava read-modify-write com perda entre
# android(SAF) x servidor/relay x overlay. Pendentes em arquivo compartilhado com
# flock (mesma ideia do chat) — idempotente por id.
_TASKS_PEND_FILE = Path("/tmp/spook_tasks_pending.json")
_TASKS_PEND_LOCK = "/tmp/spook_tasks_pending.lock"
_TPEND = {}


def _tpend_lock():
    fd = open(_TASKS_PEND_LOCK, "a+")
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


def _tcarregar():
    try:
        arr = json.loads(_TASKS_PEND_FILE.read_text())
        return {t["id"]: t for t in arr if isinstance(t, dict) and t.get("id")} if isinstance(arr, list) else {}
    except Exception:
        return {}


def _tsalvar():
    try:
        tmp = _TASKS_PEND_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(list(_TPEND.values()), ensure_ascii=False))
        tmp.replace(_TASKS_PEND_FILE)
    except Exception:
        pass


def _tpin(record):
    fd = _tpend_lock()
    try:
        _TPEND.update(_tcarregar())
        _TPEND[record["id"]] = record
        _tsalvar()
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()


def _flush_tpending():
    """Sobe TODAS as operacoes de task pendentes (create/update/delete) de
    qualquer processo. Idempotente por id."""
    fd = _tpend_lock()
    try:
        _TPEND.update(_tcarregar())
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
    for tid in list(_TPEND):
        rec = _TPEND.get(tid)
        if not rec:
            continue
        fdu = _upload_lock()
        try:
            _TPEND.update(_tcarregar())
            rec = _TPEND.get(tid)
            if not rec:
                continue
            if rec.get("__del"):
                if _remoto_tem(f"{TASKS}/{tid}.json"):
                    _run(["deletefile", _base(f"{TASKS}/{tid}.json")], check=False)
                alvo = False
            else:
                _gravar_task_local(rec)
                if not _remoto_tem(f"{TASKS}/{tid}.json"):
                    _run(["copyto", str(_TASKS_DIR / f"{tid}.json"), _base(f"{TASKS}/{tid}.json")], check=False)
                alvo = True
            # so da como confirmado quando a LISTA do Drive refletir a mudanca;
            # enquanto isso o pendente continua vivo (pin + merge no listar)
            if _remoto_tem(f"{TASKS}/{tid}.json") != alvo:
                continue
            fd = _tpend_lock()
            try:
                _TPEND.update(_tcarregar())
                _TPEND.pop(tid, None)
                _tsalvar()
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                fd.close()
        except Exception:
            continue
        finally:
            fcntl.flock(fdu, fcntl.LOCK_UN)
            fdu.close()


def _gravar_task_local(task):
    if not task or not task.get("id"):
        return
    _TASKS_DIR.mkdir(parents=True, exist_ok=True)
    f = _TASKS_DIR / f"{task['id']}.json"
    tmp = f.with_suffix(".json.tmp")
    try:
        tmp.write_text(json.dumps(task, ensure_ascii=False))
        tmp.replace(f)
    except Exception:
        pass


def _aaplica_tpend_locais():
    """Re-escreve no espelho local os .json pendentes (protege contra o prune do
    rclone sync de outro processo antes do upload confirmar)."""
    fd = _tpend_lock()
    try:
        _TPEND.update(_tcarregar())
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
    for rec in _TPEND.values():
        if not rec.get("__del"):
            _gravar_task_local(rec)


def _migra_tasks_json():
    """Converte a antiga lista unica tasks/tasks.json em N arquivos por-task
    (roda uma unica vez, quando o espelho ainda tem o arquivo antigo)."""
    f = _TASKS_DIR / TASKS_FILE
    if not f.exists():
        return
    try:
        arr = json.loads(f.read_text()).get("tasks", [])
    except Exception:
        return
    ok = False
    for t in arr if isinstance(arr, list) else []:
        if not t.get("id"):
            continue
        _gravar_task_local(t)
        try:
            _run(["copyto", str(_TASKS_DIR / f"{t['id']}.json"), _base(f"{TASKS}/{t['id']}.json")], check=False)
            ok = True
        except Exception:
            pass
    if ok:
        _run(["deletefile", _base(f"{TASKS}/{TASKS_FILE}")], check=False)
        try:
            f.unlink(missing_ok=True)
        except Exception:
            pass


def _merge_tasks(local: dict, remote: dict) -> dict:
    merged = {**local}
    for tid, t in remote.items():
        if tid and (tid not in merged or t.get("ts", 0) > merged[tid].get("ts", 0)):
            merged[tid] = t
    return merged


def _sorted_tasks(merged: dict):
    return sorted(merged.values(), key=lambda t: (t.get("feito", False), -t.get("ts", 0)))


def _base(rel):
    return f"{REMOTE}/{rel}"


def _run(args, input_bytes=None, check=True, timeout=120):
    p = subprocess.run(
        ["rclone", *args],
        input=input_bytes,
        capture_output=True,
        timeout=timeout,
    )
    if check and p.returncode != 0:
        raise RuntimeError(p.stderr.decode(errors="replace").strip() or f"rclone {' '.join(args)}")
    return p


def _remoto_tem(rel, timeout=40):
    """Confirma se um caminho existe no Drive (lsf de arquivo unico e barato).

    A listagem do Google Drive so reflete um upload alguns SEGUNDOS depois de o
    objeto existir (eventual consistency). Usado no flush: so damos o upload como
    confirmado quando a LISTA do Drive o ve — senao o rclone sync pull seguinte
    (que ve listagem velha) podaria o espelho local e a task sumia da view.
    Retorna True/False determinístico; em erro retorna False (tenta de novo)."""
    try:
        p = _run(["lsf", "--format", "p", _base(rel)], check=False, timeout=timeout)
        return p.returncode == 0 and bool(p.stdout.decode(errors="replace").strip())
    except Exception:
        return False


def _ler_chat_local():
    """Lê todas as msgs de disco (espelho local), em memória. Inventário do rclone fica de fora."""
    msgs = []
    if _CHAT_DIR.exists():
        for f in _CHAT_DIR.iterdir():
            if not f.name.endswith(".json"):
                continue
            try:
                msg = json.loads(f.read_text())
                if isinstance(msg, dict) and msg.get("id"):
                    msg["_arquivo"] = str(f)
                    msgs.append(msg)
            except Exception:
                continue
    return sorted(msgs, key=lambda m: m.get("ts", 0))


def _escrever_json(caminho, msg):
    _run(["rcat", _base(caminho)], input_bytes=json.dumps(msg).encode())


def _bg(fn):
    threading.Thread(target=fn, daemon=True).start()


def _upload_texto(msg):
    try:
        _escrever_json(f"{CHAT}/{msg['id']}.json", msg)
    except Exception:
        pass


def _upload_arquivo(p, destino, msg):
    try:
        _run(["copyto", str(p), _base(destino)], timeout=900)
        _escrever_json(f"{CHAT}/{msg['id']}.json", msg)
    except Exception:
        pass


def listar_mensagens():
    # Leitura instantanea do espelho local. O sync fica a cargo da thread de
    # background; o sincronismo inicial aconteceu no _start_refresher().
    return _ler_chat_local()


def apagar_mensagem(msg_id):
    # local-first: remove do espelho ja; deletefile roda em background
    _delete_local(msg_id)
    fd = _pend_lock()
    try:
        _PENDING.update(_carregar_pending())
        _PENDING.pop(msg_id, None)
        _salvar_pending()
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
    _bg(lambda: _delete_remoto(f"{msg_id}.json", _ler_msg_local(msg_id)))
    return True


def _ler_msg_local(msg_id):
    try:
        f = _CHAT_DIR / f"{msg_id}.json"
        return json.loads(f.read_text())
    except Exception:
        return {}


def _delete_local(msg_id):
    try:
        (_CHAT_DIR / f"{msg_id}.json").unlink(missing_ok=True)
    except Exception:
        pass


def _delete_remoto(nome, msg):
    try:
        _run(["deletefile", f"{_base(CHAT)}/{nome}"], check=False)
    except Exception:
        pass
    arq = msg.get("arquivo", "")
    if arq:
        try:
            _run(["deletefile", f"{_base(MEDIA)}/{arq.rsplit('/', 1)[-1]}"], check=False)
        except Exception:
            pass


def enviar_texto(texto, origem=ORIGEM):
    msg = {
        "id": str(uuid.uuid4()),
        "ts": int(time.time() * 1000),
        "from": origem,
        "tipo": "texto",
        "texto": texto.strip(),
        "arquivo": "",
    }
    _pin(msg)
    _gravar_local(msg)
    _bg(_flush_pending)
    return msg


def enviar_texto_id(texto, origem=ORIGEM, msg_id=None, tipo="texto", arquivo=""):
    msg = {
        "id": msg_id or str(uuid.uuid4()),
        "ts": int(time.time() * 1000),
        "from": origem,
        "tipo": tipo,
        "texto": texto.strip(),
        "arquivo": arquivo or "",
    }
    _pin(msg)
    _gravar_local(msg)
    _bg(_flush_pending)
    return msg


def enviar_arquivo(caminho, tipo="arquivo", caption="", msg_id=None):
    p = Path(caminho)
    destino = f"{MEDIA}/{int(time.time()*1000)}-{p.name}"
    msg = {
        "id": msg_id or str(uuid.uuid4()),
        "ts": int(time.time() * 1000),
        "from": ORIGEM,
        "tipo": tipo,
        "texto": caption,
        "arquivo": destino,
    }
    _pin(msg, _tipo_arquivo=tipo, _src=str(p))
    _gravar_local(msg)
    # Garante disponibilidade imediata no espelho de midia local para a rota /media/
    os.makedirs(_MEDIA_DIR, exist_ok=True)
    try:
        ( _MEDIA_DIR / destino.split("/")[-1] ).write_bytes(p.read_bytes())
    except Exception:
        pass
    return msg


def enviar_midia_server(msg_id, texto, origem, tipo, arquivo_nome, caminho_bytes):
    """POST /upload do relay: persiste o json do chat e agenda o upload do
    arquivo p/ Drive (mesmo fluxo de persistencia das msgs, sem SAF). O arquivo
    ja esta salvo no espelho local de midia (/tmp/spook_media_local)."""
    msg = {
        "id": msg_id or str(uuid.uuid4()),
        "ts": int(time.time() * 1000),
        "from": origem,
        "tipo": tipo,
        "texto": texto.strip(),
        "arquivo": f"{MEDIA}/{arquivo_nome}",
    }
    _pin(msg, _tipo_arquivo=tipo, _src=caminho_bytes)
    _gravar_local(msg)
    _bg(_flush_pending)
    return msg


def id_arquivo(nome):
    p = _run(["lsf", "--files-only", "--format", "pi", _base(MEDIA)], check=False, timeout=40)
    if p.returncode != 0:
        return None
    for linha in p.stdout.decode().splitlines():
        caminho, sep, arquivo_id = linha.partition(";")
        if sep and caminho == nome and arquivo_id:
            return arquivo_id
    return None


def baixar_bytes(rel):
    p = _run(["cat", _base(rel)], check=False)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.decode(errors="replace").strip())
    return p.stdout


_MEDIA_DIR = Path(os.environ.get("SPOOK_MEDIA_DIR", "/tmp/spook_media_local"))


# Modo local: quando SPOOK_DADOS é explicitado (relay/overlay), o remote rclone
# é um type=local e precisa dos subdirs chat/tasks/media existindo — o rcat do
# rclone NÃO cria o diretório de destino automaticamente.
if os.environ.get("SPOOK_DADOS"):
    for sub in (CHAT, TASKS, MEDIA):
        try:
            (_DADOS / sub).mkdir(parents=True, exist_ok=True)
        except Exception:
            pass


def baixar_midia(nome):
    """Baixa/obtem midia (media/<nome>) p/ cache local e retorna o path.
    Idempotente. Prioridade: cache local -> origem local de pendente compartilhado
    (local-first ainda nao publicado) -> remoto."""
    seguro = Path(nome.rsplit("/", 1)[-1]).name
    destino = _MEDIA_DIR / seguro
    if destino.exists() and destino.stat().st_size > 0:
        return str(destino)
    os.makedirs(_MEDIA_DIR, exist_ok=True)
    # 1) origem local de um pendente (msg criada aqui, upload ainda nao subiu)
    fd = _pend_lock()
    try:
        _PENDING.update(_carregar_pending())
        for item in _PENDING.values():
            src = item.get("_src")
            arq = item.get("arquivo") or ""
            if src and Path(arq).name == seguro and Path(src).exists():
                destino.write_bytes(Path(src).read_bytes())
                return str(destino)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
    # 2) remoto
    tmp = destino.with_suffix(destino.suffix + ".part")
    try:
        _run(["copyto", _base(f"{MEDIA}/{seguro}"), str(tmp)], timeout=900)
        tmp.replace(destino)
        return str(destino) if destino.exists() else None
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        return None


def listar_tasks():
    # Leitura instantanea do espelho (mesmo principio do chat): o sync fica por
    # conta da thread de background (_loop). Forcar sync aqui fazia cada poll
    # (a cada 2s) rodar um rclone sync completo + sofrer prune da listagem velha.
    locais = {}
    if _TASKS_DIR.exists():
        for f in _TASKS_DIR.glob("*.json"):
            if f.name == TASKS_FILE:
                continue
            try:
                t = json.loads(f.read_text())
                if t.get("id"):
                    locais[t["id"]] = t
            except Exception:
                continue
    # cobre o que ainda esta pendente (upload nao confirmado): nao deve sumir da view
    fd = _tpend_lock()
    try:
        pend = _tcarregar()
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
    for tid, rec in pend.items():
        if not rec.get("__del") and rec.get("id"):
            locais[tid] = rec
    # cache so espelha o presente (nao deixa task apagada ressuscitar)
    _salvar_cache_tasks(locais)
    return _sorted_tasks(locais)


def salvar_tasks(tasks):
    with _TASKS_LOCK:
        for t in tasks:
            if not t.get("id"):
                continue
            _gravar_task_local(t)
            _tpin(t)


def criar_task(titulo):
    nova = {
        "id": str(uuid.uuid4()),
        "titulo": titulo.strip(),
        "feito": False,
        "criado_por": ORIGEM,
        "ts": int(time.time() * 1000),
    }
    with _TASKS_LOCK:
        merged = _ler_cache_tasks()
        merged[nova["id"]] = nova
        _salvar_cache_tasks(merged)
        _gravar_task_local(nova)
        _tpin(nova)
    return nova, _sorted_tasks(merged)


def marcar_task(task_id, feito):
    agora = int(time.time() * 1000)
    with _TASKS_LOCK:
        merged = _ler_cache_tasks()
        if task_id in merged:
            merged[task_id] = {**merged[task_id], "feito": feito, "ts": agora}
        else:
            f = _TASKS_DIR / f"{task_id}.json"
            try:
                t = json.loads(f.read_text())
                merged[task_id] = {**t, "feito": feito, "ts": agora}
            except Exception:
                return _sorted_tasks(merged)
        _salvar_cache_tasks(merged)
        _gravar_task_local(merged[task_id])
        _tpin(merged[task_id])
    return _sorted_tasks(merged)


def apagar_task(task_id):
    with _TASKS_LOCK:
        merged = _ler_cache_tasks()
        merged.pop(task_id, None)
        _salvar_cache_tasks(merged)
        try:
            (_TASKS_DIR / f"{task_id}.json").unlink(missing_ok=True)
        except Exception:
            pass
        _tpin({"id": task_id, "__del": True})
    return _sorted_tasks(merged)


def listar_spook():
    p = _run(["lsjson", "--files-only", _base("")], check=False)
    if p.returncode != 0 or not p.stdout.strip():
        return []
    return [i.get("Name") for i in json.loads(p.stdout) if not i.get("IsDir")]


def enviar_spook(caminho):
    p = Path(caminho)
    _run(["copyto", str(p), _base(p.name)])
    return p.name

def listar_media():
    """Nomes de arquivos disponiveis em media/ (espelho local + pendentes).
    Barato: apenas leitura de disco, sem chamar rclone."""
    nomes = set()
    try:
        if _MEDIA_DIR.exists():
            for f in _MEDIA_DIR.iterdir():
                if f.is_file() and f.stat().st_size > 0:
                    nomes.add(f.name)
    except Exception:
        pass
    try:
        fd = _pend_lock()
        try:
            _PENDING.update(_carregar_pending())
            for item in _PENDING.values():
                arq = item.get("arquivo") or ""
                if arq.startswith(f"{MEDIA}/"):
                    nomes.add(arq.split("/", 1)[1])
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()
    except Exception:
        pass
    return sorted(nomes)





# ============================ LIVROS (prateleira + posicoes) ============================
# Uma FICHA por livro (bookId = sha256 do conteudo do arquivo). Substitui o par
# antigo spook_reading_local/<bookId>.json (posicao) + livros_local/<deviceId>.json
# (lista inteira por dispositivo). Aquele desenho obrigava reenviar a biblioteca
# toda a cada "feito"/"excluir" e nao sabia representar remocao: o livro apagado
# num dispositivo voltava pelo outro no proximo envio.
#
# Convergencia (last-write-wins por timestamp do CLIENTE, para funcionar offline):
#   done     -> doneEm / donePor        (desmarcar tambem vence)
#   removido -> removidoEm / removidoPor (tombstone: some da lista, arquivo fica)
#   retomar  -> so escrita de leitura real (origem != "restaurado"), nunca o eco
#               de uma posicao que o cliente acabou de restaurar
# Arquivos sao content-addressed: reading/arquivos/<bookId>.<ext> (nome = hash),
# entao renomear nao quebra nada e o cliente valida o sha256 depois do download.
import hashlib
import shutil

LIVROS_DIR = _DADOS / "livros"
LIVROS_DIR.mkdir(parents=True, exist_ok=True)
LIVROS_ARQ_DIR = _DADOS / "livros_arquivos"
LIVROS_ARQ_DIR.mkdir(parents=True, exist_ok=True)
DRIVE_LIVROS_REL = "reading/livros"
DRIVE_ARQ_REL = "reading/arquivos"

_LIV_WRITE_LOCK = threading.Lock()
_MIRROR_LOCK = threading.Lock()
_MIRROR_PEDIDOS = {}                 # bookId -> {"forcar": bool, "em": float}
_MIRROR_INTERVALO_POS = 45.0         # posicao: no maximo 1 upload por livro/45s
_MIRROR_INICIADO = False
_PULL_LIVROS_INICIADO = False


def _book_id_ok(book_id):
    b = str(book_id or "").strip().lower()
    if len(b) != 64:
        return None
    if any(c not in "0123456789abcdef" for c in b):
        return None
    return b


def _mime_de(ext):
    return {
        "pdf": "application/pdf",
        "epub": "application/epub+zip",
    }.get((ext or "").lower(), "application/octet-stream")


def _sha256_arquivo(caminho):
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _path_livro(book_id):
    return LIVROS_DIR / f"{book_id}.json"


def _registro_novo(book_id):
    return {
        "schema": 2,
        "bookId": book_id,
        "titulo": "",
        "ext": "",
        "mimeType": "",
        "tamanho": 0,
        "criadoEm": 0,
        "criadoPor": "",
        "atualizadoEm": 0,
        "done": False,
        "doneEm": 0,
        "donePor": "",
        "removido": False,
        "removidoEm": 0,
        "removidoPor": "",
        "arquivo": {"noDrive": False, "nome": None, "tamanho": 0, "enviadoEm": 0},
        "posicoes": {},
        "retomar": {"page": 0, "offsetY": 0.0, "percent": 0.0, "device": "", "em": 0},
    }


def obter_livro(book_id):
    b = _book_id_ok(book_id)
    if not b:
        return None
    try:
        p = _path_livro(b)
        if not p.exists():
            return None
        r = json.loads(p.read_text())
        base = _registro_novo(b)
        base.update(r or {})
        base["arquivo"] = {**_registro_novo(b)["arquivo"], **(r.get("arquivo") or {})}
        base["retomar"] = {**_registro_novo(b)["retomar"], **(r.get("retomar") or {})}
        base["posicoes"] = r.get("posicoes") or {}
        base["bookId"] = b
        return base
    except Exception:
        return None


def _pedir_espelho(book_id, forcar=False):
    with _MIRROR_LOCK:
        atual = _MIRROR_PEDIDOS.get(book_id)
        if atual and atual.get("forcar") and not forcar:
            return
        _MIRROR_PEDIDOS[book_id] = {"forcar": bool(forcar), "em": time.time()}


def _gravar_livro(rec, forcar_espelho=False):
    b = rec["bookId"]
    p = _path_livro(b)
    with _LIV_WRITE_LOCK:
        tmp = p.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(rec, ensure_ascii=False, indent=2))
        os.replace(tmp, p)
    _pedir_espelho(b, forcar_espelho)
    _iniciar_espelho()
    return rec


def _espelhar_livro(book_id):
    """Sobe a ficha para o Drive. Devolve False p/ o chamador re-tentar."""
    if not _path_livro(book_id).exists():
        return True
    try:
        fd = _upload_lock()
        try:
            _run(["copyto", str(_path_livro(book_id)),
                  _base(f"{DRIVE_LIVROS_REL}/{book_id}.json")], timeout=90)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()
        return True
    except Exception:
        return False


def _loop_espelho():
    while True:
        try:
            agora = time.time()
            prontos = []
            with _MIRROR_LOCK:
                for bid, ped in list(_MIRROR_PEDIDOS.items()):
                    if ped["forcar"] or (agora - ped["em"]) >= _MIRROR_INTERVALO_POS:
                        prontos.append((bid, ped["forcar"]))
                        _MIRROR_PEDIDOS.pop(bid, None)
            for bid, forcar in prontos:
                if not _espelhar_livro(bid):
                    with _MIRROR_LOCK:
                        atual = _MIRROR_PEDIDOS.get(bid) or {}
                        _MIRROR_PEDIDOS[bid] = {"forcar": forcar or atual.get("forcar", False),
                                                "em": min(agora, atual.get("em", agora))}
        except Exception:
            pass
        time.sleep(3)


def _iniciar_espelho():
    global _MIRROR_INICIADO
    if _MIRROR_INICIADO:
        return
    _MIRROR_INICIADO = True
    threading.Thread(target=_loop_espelho, daemon=True).start()


def _pull_livros():
    """Puxa fichas do Drive (nunca apaga local: copy, nao sync). No boot e de vez
    em quando. --update garante que um arquivo local mais novo nao seja sobrescrito."""
    fd = _acquire_sync_lock()
    if fd is None:
        return False
    try:
        _run(["copy", "--update", "--fast-list", _base(DRIVE_LIVROS_REL), str(LIVROS_DIR)],
             check=False, timeout=180)
        return True
    except Exception:
        return False
    finally:
        _release_sync_lock(fd)


def _loop_pull_livros():
    while True:
        try:
            _pull_livros()
        except Exception:
            pass
        time.sleep(120)


def _iniciar_pull_livros():
    global _PULL_LIVROS_INICIADO
    if _PULL_LIVROS_INICIADO:
        return
    _PULL_LIVROS_INICIADO = True
    threading.Thread(target=_loop_pull_livros, daemon=True).start()


def listar_livros(incluir_removidos=False):
    """Prateleira. Ordena por ultima LEITURA (retomar.em), depois por atualizacao."""
    out = []
    try:
        for p in LIVROS_DIR.glob("*.json"):
            r = obter_livro(p.stem)
            if not r:
                continue
            if not incluir_removidos and r.get("removido"):
                continue
            r["disponivel"] = bool(caminho_arquivo_livro(r["bookId"]))
            r["noDrive"] = bool((r.get("arquivo") or {}).get("noDrive"))
            out.append(r)
    except Exception:
        return []
    out.sort(key=lambda x: (x.get("retomar") or {}).get("em") or x.get("atualizadoEm") or 0, reverse=True)
    return out


def _posicao_ou_zero(d):
    return {
        "page": int(d.get("page") or 0),
        "offsetY": float(d.get("offsetY") or 0.0),
        "percent": float(d.get("percent") or 0.0),
        "em": int(d.get("em") or 0),
    }


def atualizar_livro(book_id, patch):
    """Aplica um patch parcial numa ficha com LWW por timestamp do cliente."""
    b = _book_id_ok(book_id)
    if not b:
        return None
    d = patch or {}
    rec = obter_livro(b) or _registro_novo(b)
    agora = int(time.time() * 1000)
    device = str(d.get("deviceId") or d.get("dispositivo") or "").strip()[:48]
    mudou_meta = False

    for campo in ("titulo", "ext", "mimeType"):
        if d.get(campo) and not rec.get(campo):
            rec[campo] = str(d[campo])[:400]
            mudou_meta = True
    if d.get("tamanho") and not rec.get("tamanho"):
        try:
            rec["tamanho"] = int(d["tamanho"])
            mudou_meta = True
        except Exception:
            pass
    if not rec.get("mimeType") and rec.get("ext"):
        rec["mimeType"] = _mime_de(rec["ext"])
    if not rec.get("criadoEm"):
        rec["criadoEm"] = agora
        rec["criadoPor"] = device

    if "done" in d:
        done_em = int(d.get("doneEm") or agora)
        if done_em >= int(rec.get("doneEm") or 0):
            rec["done"] = bool(d.get("done"))
            rec["doneEm"] = done_em
            rec["donePor"] = str(d.get("donePor") or device)
            mudou_meta = True

    if "removido" in d:
        rem_em = int(d.get("removidoEm") or agora)
        if rem_em >= int(rec.get("removidoEm") or 0):
            rec["removido"] = bool(d.get("removido"))
            rec["removidoEm"] = rem_em
            rec["removidoPor"] = str(d.get("removidoPor") or device)
            mudou_meta = True

    arq = d.get("arquivo")
    if isinstance(arq, dict) and arq.get("nome"):
        base_arq = rec.get("arquivo") or {}
        if (not base_arq.get("noDrive")) or arq.get("noDrive"):
            rec["arquivo"] = {**base_arq, **arq}
            if not rec.get("ext"):
                nome_arq = str(arq.get("nome"))
                if "." in nome_arq:
                    rec["ext"] = nome_arq.rsplit(".", 1)[-1].lower()
                    rec["mimeType"] = _mime_de(rec["ext"])
            mudou_meta = True

    if device and d.get("page") is not None:
        em = int(d.get("em") or agora)
        if em <= 0:
            em = agora
        if em > agora + 300_000:      # relogio do cliente muito no futuro
            em = agora
        ant = (rec.get("posicoes") or {}).get(device) or {}
        if em >= int(ant.get("em") or 0):
            rec.setdefault("posicoes", {})[device] = _posicao_ou_zero({**d, "em": em})
            origem = str(d.get("origem") or "leitura")
            ret = rec.get("retomar") or {}
            if origem != "restaurado" and em > int(ret.get("em") or 0):
                rec["retomar"] = {**_posicao_ou_zero({**d, "em": em}), "device": device}
                mudou_meta = True
            _pedir_espelho(b, False)

    rec["atualizadoEm"] = agora
    return _gravar_livro(rec, forcar_espelho=mudou_meta)


def publicar_livro(origem, book_id=None, titulo="", ext="", device="", forcar_upload=True):
    """Registra um arquivo (PDF/EPUB) na prateleira e sobe pro Drive.
    O nome no Drive e o sha256 do conteudo: identidade independente de nome."""
    try:
        p = Path(origem)
        if not p.exists() or p.stat().st_size == 0:
            return None
        ext = (ext or p.suffix.lstrip(".")).lower()
        if ext not in ("pdf", "epub"):
            return None
        calc = _sha256_arquivo(p)
        b = calc
        if book_id and _book_id_ok(book_id) and _book_id_ok(book_id) != calc:
            # arquivo nao corresponde ao id que o cliente mandou: o conteudo manda
            b = calc
        dest = LIVROS_ARQ_DIR / f"{b}.{ext}"
        if p.resolve() != dest.resolve():
            shutil.copyfile(p, dest)

        no_drive = False
        if forcar_upload:
            try:
                fd = _upload_lock()
                try:
                    _run(["copyto", str(dest), _base(f"{DRIVE_ARQ_REL}/{b}.{ext}")], timeout=900)
                finally:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    fd.close()
                no_drive = True
            except Exception:
                no_drive = False

        agora = int(time.time() * 1000)
        rec = obter_livro(b) or _registro_novo(b)
        rec["titulo"] = rec.get("titulo") or titulo or p.stem
        rec["ext"] = ext
        rec["mimeType"] = _mime_de(ext)
        rec["tamanho"] = dest.stat().st_size
        rec["arquivo"] = {
            "noDrive": no_drive,
            "nome": f"{b}.{ext}",
            "tamanho": dest.stat().st_size,
            "enviadoEm": agora if no_drive else 0,
        }
        # reimportar limpa a tombstone (LWW: agora > remocao antiga)
        rec["removido"] = False
        rec["removidoEm"] = agora
        rec["removidoPor"] = device
        if not rec.get("criadoEm"):
            rec["criadoEm"] = agora
            rec["criadoPor"] = device
        rec["atualizadoEm"] = agora
        _gravar_livro(rec, forcar_espelho=True)
        return rec
    except Exception:
        return None


def caminho_arquivo_livro(book_id):
    b = _book_id_ok(book_id)
    if not b:
        return None
    try:
        for f in LIVROS_ARQ_DIR.glob(f"{b}.*"):
            if f.is_file() and f.stat().st_size > 0:
                return f
    except Exception:
        pass
    return None


def baixar_arquivo_livro(book_id):
    """Devolve o Path local do arquivo, baixando do Drive se preciso."""
    b = _book_id_ok(book_id)
    if not b:
        return None
    local = caminho_arquivo_livro(b)
    if local:
        return local
    rec = obter_livro(b) or {}
    ext = (rec.get("ext") or "").lower()
    nome = (rec.get("arquivo") or {}).get("nome") or (f"{b}.{ext}" if ext else "")
    if not nome:
        return None
    dest = LIVROS_ARQ_DIR / nome
    fd = _acquire_sync_lock()
    try:
        _run(["copyto", _base(f"{DRIVE_ARQ_REL}/{nome}"), str(dest)], timeout=900)
    except Exception:
        return None
    finally:
        if fd is not None:
            _release_sync_lock(fd)
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    return None


def apagar_livro(book_id, device="", apagar_arquivo=False):
    """Remove da lista = tombstone. Por padrao NAO apaga o arquivo do Drive."""
    rec = obter_livro(book_id)
    if not rec:
        return None
    agora = int(time.time() * 1000)
    rec["removido"] = True
    rec["removidoEm"] = agora
    rec["removidoPor"] = str(device)
    rec["atualizadoEm"] = agora
    if apagar_arquivo:
        try:
            nome = (rec.get("arquivo") or {}).get("nome")
            if nome:
                _run(["deletefile", _base(f"{DRIVE_ARQ_REL}/{nome}")], check=False, timeout=120)
            local = caminho_arquivo_livro(book_id)
            if local:
                local.unlink(missing_ok=True)
            rec["arquivo"] = {"noDrive": False, "nome": nome, "tamanho": 0, "enviadoEm": 0}
        except Exception:
            pass
    return _gravar_livro(rec, forcar_espelho=True)


# ---------- compatibilidade com o protocolo antigo (leitura_* / livros_*) ----------

def obter_leitura(book_id):
    """Shape antigo (spook_reading_local) derivado da ficha nova."""
    rec = obter_livro(book_id)
    if not rec:
        return None
    r = rec.get("retomar") or {}
    return {
        "bookId": rec["bookId"],
        "title": rec.get("titulo", ""),
        "mimeType": rec.get("mimeType", ""),
        "devices": {
            k: {"page": v.get("page"), "offsetY": v.get("offsetY"),
                "percent": v.get("percent"), "updatedAt": v.get("em")}
            for k, v in (rec.get("posicoes") or {}).items()
        },
        "lastPage": r.get("page", 0),
        "lastOffsetY": r.get("offsetY", 0.0),
        "lastPercent": r.get("percent", 0.0),
        "lastDevice": r.get("device", ""),
        "createdAt": rec.get("criadoEm", 0),
        "updatedAt": rec.get("atualizadoEm", 0),
        "done": rec.get("done", False),
        "removido": rec.get("removido", False),
    }


def atualizar_leitura(book_id, dados):
    rec = atualizar_livro(book_id, dados or {})
    return obter_leitura(rec["bookId"]) if rec else None


def obter_todos_livros(excluir_deviceId=None):
    """Compat: devolve a prateleira no shape antigo (lista de livros)."""
    out = []
    for r in listar_livros():
        out.append({
            "bookId": r["bookId"],
            "titulo": r.get("titulo", ""),
            "mimeType": r.get("mimeType", ""),
            "tamanho": r.get("tamanho", 0),
            "ext": r.get("ext", ""),
            "done": r.get("done", False),
            "caminho": None,
            "noDrive": bool((r.get("arquivo") or {}).get("noDrive")),
        })
    return out


def salvar_livros(device_id, livros):
    """Compat com o APK antigo, que manda a lista INTEIRA do dispositivo.
    Upserta o que veio e vira tombstone o que sumiu (era o bug da ressurreicao)."""
    try:
        safe = "".join(c for c in str(device_id) if c.isalnum() or c in "-_")
        snap_dir = _DADOS / "livros_local"
        snap_dir.mkdir(parents=True, exist_ok=True)
        snap = snap_dir / (safe + ".json")
        anteriores = set()
        estado_anterior = {}
        try:
            if snap.exists():
                arr = json.loads(snap.read_text())
                anteriores = {l.get("bookId") for l in arr if isinstance(l, dict)}
                estado_anterior = {l.get("bookId"): bool(l.get("done")) for l in arr if isinstance(l, dict)}
        except Exception:
            anteriores = set()
            estado_anterior = {}
        agora = int(time.time() * 1000)
        vistos = set()
        for l in (livros or []):
            if not isinstance(l, dict):
                continue
            bid = _book_id_ok(l.get("bookId"))
            if not bid:
                continue
            vistos.add(bid)
            patch = {
                "titulo": l.get("titulo", ""),
                "ext": l.get("ext", ""),
                "tamanho": l.get("tamanho", 0),
                "deviceId": device_id,
            }
            # so manda done quando MUDOU em relacao ao snapshot: o APK antigo
            # reenvia a lista inteira e, sem isso, carimbaria doneEm=agora em
            # todo livro e venceria um "feito" marcado no outro aparelho.
            if bid not in estado_anterior or estado_anterior[bid] != bool(l.get("done")):
                patch["done"] = bool(l.get("done"))
                patch["doneEm"] = agora
            atualizar_livro(bid, patch)
        for antigo in anteriores - vistos:
            apagar_livro(antigo, device=device_id)
        snap.write_text(json.dumps(livros, ensure_ascii=False))
        return True
    except Exception:
        return False


def listar_livros_drive():
    """Compat com o botao GOOGLE DRIVE do Arch antigo."""
    out = []
    for r in listar_livros():
        nome = (r.get("arquivo") or {}).get("nome") or f"{r['bookId']}.{r.get('ext', 'pdf')}"
        out.append({
            "bookId": r["bookId"],
            "titulo": r.get("titulo", ""),
            "nome_arquivo": nome,
            "tamanho": r.get("tamanho", 0),
            "ext": r.get("ext", "pdf"),
            "modificado": "",
            "done": r.get("done", False),
            "noDrive": bool((r.get("arquivo") or {}).get("noDrive")),
        })
    return out


def _migra_livros_v1():
    """Converte os dois arquivos antigos em fichas por livro. Roda uma vez."""
    marca = _DADOS / ".livros_v2_migrado"
    if marca.exists():
        return
    tocados = 0
    try:
        antigos = _DADOS / "spook_reading_local"
        if antigos.exists():
            for p in antigos.glob("*.json"):
                if not _book_id_ok(p.stem):
                    continue                      # test-book-*: lixo de teste
                try:
                    d = json.loads(p.read_text())
                except Exception:
                    continue
                rec = obter_livro(p.stem) or _registro_novo(p.stem)
                rec["titulo"] = rec.get("titulo") or d.get("title", "")
                rec["mimeType"] = rec.get("mimeType") or d.get("mimeType", "")
                pos = {}
                for dev, v in (d.get("devices") or {}).items():
                    if v.get("page") is None:
                        continue
                    pos[dev] = {
                        "page": int(v.get("page") or 0),
                        "offsetY": float(v.get("offsetY") or 0),
                        "percent": float(v.get("percent") or 0),
                        "em": int(v.get("updatedAt") or 0),
                    }
                rec["posicoes"] = pos or rec.get("posicoes") or {}
                if d.get("lastDevice") and d.get("lastPage") is not None:
                    rec["retomar"] = {
                        "page": int(d.get("lastPage") or 0),
                        "offsetY": float(d.get("lastOffsetY") or 0),
                        "percent": float(d.get("lastPercent") or 0),
                        "device": d.get("lastDevice"),
                        "em": int(d.get("updatedAt") or 0),
                    }
                rec["criadoEm"] = rec.get("criadoEm") or int(d.get("createdAt") or 0)
                rec["atualizadoEm"] = int(d.get("updatedAt") or 0)
                _gravar_livro(rec, forcar_espelho=True)
                tocados += 1

        lista_antiga = _DADOS / "livros_local"
        if lista_antiga.exists():
            for p in lista_antiga.glob("*.json"):
                try:
                    arr = json.loads(p.read_text())
                except Exception:
                    continue
                for l in (arr or []):
                    bid = _book_id_ok(l.get("bookId"))
                    if not bid:
                        continue
                    rec = obter_livro(bid) or _registro_novo(bid)
                    rec["titulo"] = rec.get("titulo") or l.get("titulo", "")
                    rec["ext"] = rec.get("ext") or (l.get("ext") or "").lower()
                    rec["mimeType"] = rec.get("mimeType") or l.get("mimeType") or _mime_de(rec.get("ext"))
                    rec["tamanho"] = rec.get("tamanho") or int(l.get("tamanho") or 0)
                    if l.get("done"):
                        rec["done"] = True
                        rec["doneEm"] = rec.get("doneEm") or int(time.time() * 1000)
                        rec["donePor"] = rec.get("donePor") or p.stem
                    rec["criadoEm"] = rec.get("criadoEm") or int(time.time() * 1000)
                    rec["criadoPor"] = rec.get("criadoPor") or p.stem
                    _gravar_livro(rec, forcar_espelho=True)
                    tocados += 1
        marca.write_text(json.dumps({"em": int(time.time() * 1000), "fichas": tocados}))
    except Exception:
        pass


_migra_livros_v1()
_iniciar_pull_livros()
_iniciar_espelho()


# ============================ RELATORIOS (Drive gdrive:SPOOK/relatorios) ============================
_RELATORIOS_DIR = Path(os.environ.get("SPOOK_RELATORIOS_DIR", str(_DADOS / "relatorios")))
_RELATORIOS_DIR.mkdir(parents=True, exist_ok=True)

def _rclone_lsjson_relatorios():
    """Lista arquivos em gdrive:SPOOK/relatorios via rclone lsjson."""
    try:
        r = subprocess.run(
            ["rclone", "lsjson", "gdrive:SPOOK/relatorios", "--fast-list"],
            capture_output=True, text=True, timeout=30
        )
        if r.returncode != 0:
            return []
        return json.loads(r.stdout)
    except Exception:
        return []

def listar_relatorios() -> list[dict]:
    """Lista relatórios do Drive, agrupados por base (sem extensão)."""
    files = _rclone_lsjson_relatorios()
    by_base = {}
    for f in files:
        name = f.get("Name", "")
        if not name:
            continue
        base = name
        for ext in [".pdf", ".docx", ".txt", ".log", ".md"]:
            if base.endswith(ext):
                base = base[:-len(ext)]
                break
        entry = by_base.setdefault(base, {"base": base, "bytes": 0, "modificado": "", "documentos": []})
        entry["bytes"] += f.get("Size", 0)
        if f.get("ModTime", "") > entry["modificado"]:
            entry["modificado"] = f.get("ModTime", "")
        if name.endswith(".pdf"):
            entry["documentos"].append("pdf")
        elif name.endswith(".docx"):
            entry["documentos"].append("docx")
        elif name.endswith(".txt") or name.endswith(".log") or name.endswith(".md"):
            entry["documentos"].append("txt")
    return list(by_base.values())

def baixar_relatorio(base: str) -> bytes | None:
    """Texto do relatorio: mirror local primeiro, Drive como reserva.

    O Drive guarda SO o .docx (decisao de 27/09); o .txt que a janela REPORTS
    renderiza vive no mirror do relay. Ir direto no rclone fazia o app receber
    o .docx (binario) e a tela mostrava lixo.
    """
    try:
        for ext in [".txt", ".log", ".md", ".pdf", ".docx"]:
            local = _RELATORIOS_DIR / (base + ext)
            try:
                if local.exists() and local.stat().st_size > 0:
                    return local.read_bytes()
            except Exception:
                continue
        for ext in [".txt", ".log", ".pdf", ".docx", ".md"]:
            name = base + ext
            local = _RELATORIOS_DIR / name
            r = subprocess.run(
                ["rclone", "copyto", f"gdrive:SPOOK/relatorios/{name}", str(local)],
                capture_output=True, text=True, timeout=30
            )
            if r.returncode == 0 and local.exists():
                return local.read_bytes()
        return None
    except Exception:
        return None

def apagar_relatorios(bases: list[str]) -> bool:
    """Apaga relatórios do Drive (todos os arquivos com a base)."""
    try:
        files = _rclone_lsjson_relatorios()
        for base in bases:
            for f in files:
                name = f.get("Name", "")
                if name.startswith(base + ".") or name == base:
                    subprocess.run(
                        ["rclone", "deletefile", f"gdrive:SPOOK/relatorios/{name}"],
                        capture_output=True, timeout=30
                    )
                    local = _RELATORIOS_DIR / name
                    if local.exists():
                        local.unlink(missing_ok=True)
        return True
    except Exception:
        return False
