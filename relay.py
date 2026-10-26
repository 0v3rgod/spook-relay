import requests
import uuid
import io
from flask import Flask, request, send_file
from flask_socketio import SocketIO, emit
from pathlib import Path
import threading
import time
import os
import json
import hub_store
import fcntl
from datetime import datetime, timezone


# Load .env for Groq keys
def _carregar_env(p: Path):
    dados = {}
    if p.exists():
        for linha in p.read_text().splitlines():
            linha = linha.strip()
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            chave, _, valor = linha.partition("=")
            dados[chave.strip()] = valor.strip().strip('"').strip("'")
    return dados


_ENV = _carregar_env(Path(__file__).resolve().parent / ".env")
GROQ_API_KEY = _ENV.get("GROQ_API_KEY", "")
GROQ_MODEL = _ENV.get("GROQ_MODEL", "openai/gpt-oss-20b")
# Token de sessao: vem do .env (SPOOK_TOKEN=...). NUNCA hardcode aqui — o repo e publico.
TOKEN_SESSAO = _ENV.get("SPOOK_TOKEN", "")
if not TOKEN_SESSAO:
    raise SystemExit("SPOOK_TOKEN nao configurado: defina SPOOK_TOKEN=<token> no .env")

# Memoria da IA (armazenada na VM)
MEMORIA_DIR = Path("/home/ubuntu/spook-relay/data/memoria_ia")
MEMORIA_DIR.mkdir(parents=True, exist_ok=True)
MAX_HISTORICO = 20
N_PERGUNTAS_PRA_EXTRACAO = 3   # extrai fatos a cada N perguntas do usuario


USUARIO_PADRAO = "taylor"


def _safe(nome: str) -> str:
    s = "".join(c for c in str(nome) if c.isalnum() or c in "-_").strip()
    return s or "default"


def _mem_path(sessao_id: str) -> Path:
    """Historico curto de UMA conversa (por sessao/dispositivo)."""
    return MEMORIA_DIR / f"sessao-{_safe(sessao_id)}.json"


def _usuario_path(usuario: str) -> Path:
    """Memoria de LONGO PRAZO, compartilhada por todos os dispositivos."""
    return MEMORIA_DIR / f"usuario-{_safe(usuario or USUARIO_PADRAO)}.json"


def _load_mem(sessao_id: str) -> dict:
    path = _mem_path(sessao_id)
    if not path.exists():
        return {"historico": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        d.setdefault("historico", [])
        return d
    except Exception:
        return {"historico": []}


def _save_mem(sessao_id: str, data: dict):
    _escrever_json(_mem_path(sessao_id), data)


def _load_usuario(usuario: str) -> dict:
    path = _usuario_path(usuario)
    if not path.exists():
        return {"perfil": [], "instrucoes": ""}
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        d.setdefault("perfil", [])
        d.setdefault("instrucoes", "")
        return d
    except Exception:
        return {"perfil": [], "instrucoes": ""}


def _save_usuario(usuario: str, data: dict):
    data["atualizado"] = _agora_iso()
    _escrever_json(_usuario_path(usuario), data)


def _escrever_json(path: Path, data: dict):
    try:
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


def _agora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _migra_perfis_antigos():
    """Joga os perfis das sessoes antigas (pre-2026-09-25) na memoria do usuario."""
    destino = USUARIO_PADRAO
    u = _load_usuario(destino)
    mudou = False
    for p in MEMORIA_DIR.glob("*.json"):
        if p.name.startswith(("usuario-", "sessao-")):
            continue
        try:
            d = json.load(open(p, "r", encoding="utf-8"))
        except Exception:
            continue
        for f in d.get("perfil", []) or []:
            fs = str(f).strip()
            if fs and not any(fs.lower() in x.lower() or x.lower() in fs.lower() for x in u["perfil"]):
                u["perfil"].append(fs)
                mudou = True
    if mudou:
        _save_usuario(destino, u)
        print(f"[IA] perfil migrado -> {len(u['perfil'])} fato(s)")


# extracao de fatos: fora do caminho da resposta (thread), com cooldown
_ultima_extracao: dict = {}


def _extrair_fatos(usuario: str, sessao_id: str, perguntas: list):
    """Chama o Groq e funde fatos duraveis no perfil COMPARTILHADO do usuario."""
    u = _load_usuario(usuario)
    texto = "\n".join(perguntas)
    if not texto.strip():
        return
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": GROQ_MODEL,
                "messages": [{"role": "user", "content": (
                    "Extraia fatos uteis e DURADAVEIS sobre o usuario das mensagens abaixo.\n"
                    "Interessa: nome, preferencias, habitos, projetos, ferramentas, objetivos, "
                    "maquinas que ele usa, problemas recorrentes.\n"
                    "Ignore novidades temporarias e facts sem importancia. Se nao houver nada duravel, "
                    "retorne lista vazia.\n"
                    "Responda APENAS com JSON: lista de strings curtas em portugues.\n\n"
                    f"Mensagens:\n{texto}"
                )}],
                "max_tokens": 500,
                "temperature": 0.1,
            },
            timeout=30,
        )
        r.raise_for_status()
        bruto = r.json()["choices"][0]["message"]["content"].strip()
        if bruto.startswith("```"):
            bruto = bruto.strip("`")
            bruto = bruto.split("\n", 1)[1] if "\n" in bruto else bruto
        fatos = json.loads(bruto)
        if not isinstance(fatos, list):
            return
        perfil = u.setdefault("perfil", [])
        antes = len(perfil)
        for f in fatos:
            fs = str(f).strip()
            if fs and not any(fs.lower() in x.lower() or x.lower() in fs.lower() for x in perfil):
                perfil.append(fs)
        perfil[:] = perfil[-60:]
        if len(perfil) != antes:
            _save_usuario(usuario, u)
            print(f"[IA] perfil {usuario}: {antes} -> {len(perfil)} fato(s)")
    except Exception as e:
        print(f"[IA] extracao falhou: {e}")


def _talvez_extrair_async(usuario: str, sessao_id: str, pergunta: str):
    """Dispara a extracao em background, no maximo a cada N perguntas do usuario."""
    if not GROQ_API_KEY or not pergunta.strip():
        return
    chave = _safe(usuario)
    n = _ultima_extracao.get(chave, 0) + 1
    _ultima_extracao[chave] = n
    if n % N_PERGUNTAS_PRA_EXTRACAO != 0 and n != 1:
        return
    mem = _load_mem(sessao_id)
    perguntas = [m["content"] for m in mem.get("historico", []) if m.get("role") == "user"][-12:]
    if pergunta.strip():
        perguntas.append(pergunta)
    threading.Thread(target=_extrair_fatos, args=(usuario, sessao_id, perguntas), daemon=True).start()


def _pergunta_pendente(sessao_id):
    """Texto (str) da pergunta original. _perguntas_pendentes guarda um dict
    {pergunta, usuario} — enviar o dict inteiro quebra a continuacao da acao,
    porque o texto entra no historico da IA e precisa ser str."""
    pend = _perguntas_pendentes.get(sessao_id, "")
    if isinstance(pend, dict):
        return str(pend.get("pergunta", ""))
    return str(pend or "")


def _executar_tool_call(tool_call: dict, sessao_id: str) -> dict:
    """Executa uma acao solicitada pela IA. Retorna dict com resultado."""
    acao = tool_call.get("acao")
    params = tool_call.get("params", {})

    if acao == "executar_scan":
        scan_nome = params.get("scan")
        alvo = params.get("alvo", "")
        if not scan_nome:
            return {"ok": False, "erro": "scan nao especificado"}
        try:
            socketio.emit("ia_executar_scan", {"scan": scan_nome, "alvo": alvo, "sessao_id": sessao_id, "pergunta_original": _pergunta_pendente(sessao_id)})
            return {"ok": True, "status": "scan_enviado_para_execucao", "scan": scan_nome, "alvo": alvo}
        except Exception as e:
            return {"ok": False, "erro": f"falha ao enviar scan: {e}"}

    elif acao == "executar_comando_notebook":
        comando_nome = params.get("comando")
        if not comando_nome:
            return {"ok": False, "erro": "comando nao especificado"}
        try:
            socketio.emit("ia_executar_comando", {"comando": comando_nome, "sessao_id": sessao_id, "pergunta_original": _pergunta_pendente(sessao_id)})
            return {"ok": True, "status": "comando_enviado_para_execucao", "comando": comando_nome}
        except Exception as e:
            return {"ok": False, "erro": f"falha ao enviar comando: {e}"}

    elif acao == "obter_info_sistema":
        try:
            socketio.emit("ia_obter_info", {"sessao_id": sessao_id, "pergunta_original": _pergunta_pendente(sessao_id)})
            return {"ok": True, "status": "info_solicitada"}
        except Exception as e:
            return {"ok": False, "erro": f"falha: {e}"}

    return {"ok": False, "erro": f"acao desconhecida: {acao}"}


# Catalogos para instrucoes da IA
CATALOGO_SCANS = {
    "NETWORK": [
        ("LIVE HOST", "Ping sweep — verifica se hosts respondem."),
        ("NETWORK SCANNER", "Descobre hosts em um CIDR autorizado."),
        ("COMMON PORTS", "Top 100 portas TCP mais usadas."),
        ("FAST NMAP GATEWAY", "Portas comuns do gateway informado."),
        ("FULL PORTS", "Todas as 65535 portas TCP, min-rate mais rapido."),
        ("SERVICES", "Identifica servicos nas portas informadas."),
        ("OS DETECTION", "Fingerprint de servico; OS limitado sem root."),
        ("COMPLETE SCAN", "Scripts padrao + deteccao de versao."),
        ("VULN SCAN", "Scripts de vulnerabilidade do nmap contra o alvo."),
        ("VULN TARGETED", "Scripts vuln em 80,443,445,3389 (portas substituiveis)."),
        ("BANNER GRAB", "Captura o banner do servico com netcat."),
    ],
    "WEB": [
        ("HTTP HEADERS", "Status e headers de uma URL autorizada."),
        ("HTTP RESPONSE", "Primeiros 200 KB da pagina para inspecao local."),
        ("WHATWEB", "Identifica tecnologias web."),
        ("GOBUSTER DIRS", "Forca diretorios web comuns."),
        ("NIKTO", "Scan de vulnerabilidades de servidor web."),
    ],
    "ENUM": [
        ("DNS RECORDS", "Consulta registros DNS do dominio."),
        ("DNS ANY", "Consulta ANY contra um servidor DNS."),
        ("SUBDOMAINS", "Forca subdominios DNS."),
        ("SMB ENUM", "Enumera shares e usuarios SMB."),
        ("FTP ANON", "Verifica login FTP anonimo."),
        ("SSL/TLS INFO", "Certificado e protocolo TLS do host."),
        ("WHOIS", "Consulta registro do dominio."),
        ("TRACEROUTE", "Rota de rede ate o alvo."),
    ],
    "LOCAL": [
        ("MY NETWORK", "Detalhes Wi-Fi visiveis ao Termux e hostname."),
        ("LOCAL ENUM", "Identidade Termux, build Android, processos e bateria."),
        ("SUID FIND", "Arquivos SUID acessiveis pelo Termux."),
        ("CAPABILITIES", "Capabilities de arquivos acessiveis pelo Termux."),
        ("TERMUX SERVICES", "Servicos gerenciados pelo usuario no Termux."),
        ("WIFI INFO", "Conexao Wi-Fi ativa visivel ao Termux."),
        ("TCP CONNECT TEST", "Testa host e porta TCP autorizados."),
        ("LOG ANALYZER", "Ultimas linhas de log Android visiveis ao Termux."),
    ],
    "CRACK": [
        ("HASH ID", "Identifica o tipo do hash."),
        ("JOHN", "Quebra hashes com John the Ripper."),
        ("JOHN SHOW", "Mostra resultados ja quebrados pelo John."),
        ("HASHCAT MD5", "Quebra MD5 (-m 0) com wordlist."),
    ],
    "AUTO": [
        ("AUTO PENTEST", "recon.sh: descoberta -> portas -> servicos -> HTTP -> enum. Relatorio no File Browser."),
    ],
}

CATALOGO_COMANDOS = {
    "sistema": [
        {"nome": "Lock Screen", "comando": "hyprctl dispatch lock-screen"},
        {"nome": "Suspend", "comando": "systemctl suspend"},
        {"nome": "Shutdown", "comando": "systemctl poweroff"},
        {"nome": "Reboot", "comando": "systemctl reboot"},
        {"nome": "Reload WM", "comando": "hyprctl reload"},
        {"nome": "Airplane Mode", "comando": "nmcli radio all off"},
        {"nome": "Radio On", "comando": "nmcli radio all on"},
    ],
    "captura": [
        {"nome": "Screenshot", "comando": "screenshot"},
    ],
    "diagnostics": [
        {"nome": "Network Interfaces", "comando": "diagnostico_interfaces"},
        {"nome": "Local Routes", "comando": "diagnostico_rotas"},
        {"nome": "DNS Configuration", "comando": "diagnostico_dns"},
        {"nome": "Listening Ports", "comando": "diagnostico_portas_locais"},
        {"nome": "Tailscale Status", "comando": "diagnostico_tailscale"},
    ],
    "info": [
        {"nome": "Public IP", "comando": "ip_publico"},
    ],
    "midia": [
        {"nome": "Play/Pause", "comando": "media_toggle"},
        {"nome": "Proxima Faixa", "comando": "media_next"},
        {"nome": "Faixa Anterior", "comando": "media_prev"},
        {"nome": "Aumentar Volume", "comando": "volume_up"},
        {"nome": "Diminuir Volume", "comando": "volume_down"},
    ],
}


def montar_instrucoes_ia():
    linhas_scans = []
    for secao, itens in CATALOGO_SCANS.items():
        linhas_scans.append(f"Scans / {secao}:")
        for nome, descricao in itens:
            linhas_scans.append(f"- {nome}: {descricao}")
    linhas_notebook = []
    for categoria, itens in CATALOGO_COMANDOS.items():
        linhas_notebook.append(f"Notebook / {categoria}:")
        for item in itens:
            linhas_notebook.append(f"- {item['nome']}")
    catalogo = "\n".join(linhas_scans + [""] + linhas_notebook)
    return (
        "Voce e SPOOK, assistente geral do app SPOOK. Responda em portugues, de forma clara, objetiva e util. "
        "IMPORTANTE: responda em TEXTO SIMPLES. Nao use markdown (nada de **negrito**, #, _italico_ ou listas com -). "
        "Pode usar quebras de linha curtas para separar paragrafos. "
        "Voce pode ajudar com qualquer assunto: tecnologia, pentest, redes, Linux, programacao, e tambem com "
        "perguntas gerais do dia a dia. "
        "Quando o usuario pedir para executar um scan ou comando do proprio app, voce pode:\n"
        "1) Explicar como fazer\n"
        "2) Emitir um evento de acao para o sistema executar (scans no Termux, comandos no notebook)\n"
        "Sempre pergunte confirmacao antes de executar algo destrutivo.\n\n"
        "QUANDO O USUARIO PEDIR PARA EXECUTAR ACAO DO CATALOGO, responda em JSON:\n"
        '<system>\n{"tool_call": {"acao": "<acao>", "params": {"<nome_param>": "<valor>"}}}\n</system>\n'
        "onde acao pode ser: executar_scan (params: scan, alvo), executar_comando_notebook (params: comando), "
        "obter_info_sistema (sem params). Responda SO o JSON acima, sem texto extra, quando a acao for pedida. "
        "Fora de pedidos de acao, responda normalmente em texto.\n\n"
        "Catalogo de scans e comandos do app (referencia):\n"
        f"{catalogo}\n\n"
        "Wordlists no Termux (celular):\n"
        "- ~/wordlists/rockyou.txt  (senhas populares)\n"
        "- ~/wordlists/common.txt   (nomes de arquivos/diretorios web)\n"
        "- ~/wordlists/raft-small-words.txt  (dirs/arquivos comuns)\n"
        "- ~/wordlists/subdomains-top1million-5000.txt  (subdominios)\n"
        "Use como: hashcat -m 0 hash.txt ~/wordlists/rockyou.txt\n"
        "Exemplos de hash para testes:\n"
        "- MD5: 5f4dcc3b5aa765d61d8327deb882cf99  (password)\n"
        "- SHA1: 5baa61e4c9b93f3f0682250b6cf8331b7ee68fd8  (password)\n"
        "- SHA256: ef92b778bafe771e89245b89ecbc08a44a4e1dd1cdd2b54fb852e5f7c5b6f7f4  (test)\n"
        "Scans CRACK usam: HASH ID, JOHN, JOHN SHOW, HASHCAT MD5."
    )


# Flask + SocketIO
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", max_http_buffer_size=25 * 1024 * 1024)

# Presenca do CELULAR real: cliente que conecta de IP que NAO e o Arch
# (overlay/bridge ficam em 100.78.69.110 ou 127.0.0.1) = app no celular.
_REMOTO_IP_NAO_CEL = {"127.0.0.1", "100.78.69.110"}
_remoto_sids = set()


def _client_remoto():
    return str(request.environ.get("REMOTE_ADDR", "")) not in _REMOTO_IP_NAO_CEL

# Bridge para o Arch (chat/tasks/notificacoes/ia_acoes)
relay_bridge_sio = None

# Sessoes com tool_call em andamento: sessao_id -> pergunta_original
_perguntas_pendentes = {}


# IA namespace (/ia)
@socketio.on("connect", namespace="/ia")
def conectar_ia():
    print("[DEBUG] conectar_ia: GROQ_API_KEY=" + str(bool(GROQ_API_KEY)))
    print("[DEBUG] /ia namespace connect handler triggered")
    print("[DEBUG] conectar_ia called, GROQ_API_KEY set: " + str(bool(GROQ_API_KEY)))
    print("[DEBUG] conectar_ia called, GROQ_API_KEY=" + ("set" if GROQ_API_KEY else "EMPTY"))
    if not GROQ_API_KEY:
        return False


def _responder_ia_core(pergunta, sessao_id, historico_extra=None, usuario=USUARIO_PADRAO):
    """Core logic para responder IA, usado tanto pelo handler principal quanto pelo loop."""
    mem = _load_mem(sessao_id)
    u = _load_usuario(usuario)
    perfil = u.get("perfil", [])
    historico = mem.get("historico", [])[-MAX_HISTORICO:]

    system_content = montar_instrucoes_ia()
    instr = str(u.get("instrucoes", "")).strip()
    if instr:
        system_content += "\n\n[Instrucoes permanentes do usuario (obedeca sempre)]:\n" + instr
    if perfil:
        system_content += "\n\n[Perfil do usuario - fatos conhecidos:]\n" + "\n".join(f"- {p}" for p in perfil)
    if historico:
        system_content += "\n\n[Historico recente da conversa]:\n" + "\n".join(
            f"{'Usuario' if m['role'] == 'user' else 'SPOOK'}: {str(m.get('content', ''))[:200]}"
            for m in historico
        )
    if historico_extra:
        for m in historico_extra:
            role = "Usuario" if m['role'] == 'user' else 'SPOOK'
            system_content += f"\n{role}: {str(m.get('content', ''))[:200]}"

    mensagens = [{"role": "system", "content": system_content}, {"role": "user", "content": pergunta}]

    resposta = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
        json={
            "model": GROQ_MODEL,
            "messages": mensagens,
            "max_tokens": 1024,
            "temperature": 0.7,
        },
        timeout=45,
    )
    resposta.raise_for_status()
    dados_resposta = resposta.json()
    conteudo = dados_resposta["choices"][0]["message"]["content"].strip()
    if not conteudo:
        raise ValueError("Resposta vazia da IA")

    # Check for tool_call
    tool_call = None
    try:
        import re as _re
        idx = conteudo.find('{"tool_call')
        if idx == -1:
            idx = conteudo.find("{'tool_call'")
        if idx != -1:
            # extrai objeto JSON balanceado por chaves a partir de idx
            alvo = conteudo[idx:]
            profundidade = 0
            inicio = None
            fim = 0
            dentro_aspas = False
            escape = False
            for i, ch in enumerate(alvo):
                if dentro_aspas:
                    if escape:
                        escape = False
                    elif ch == "\\":
                        escape = True
                    elif ch == '"':
                        dentro_aspas = False
                    continue
                if ch == '"':
                    dentro_aspas = True
                elif ch == "{":
                    if profundidade == 0:
                        inicio = i
                    profundidade += 1
                elif ch == "}":
                    profundidade -= 1
                    if profundidade == 0:
                        fim = i + 1
                        break
            if inicio is not None and fim > inicio:
                bloco = alvo[inicio:fim]
                dados_tool = json.loads(bloco)
                tool_call = dados_tool.get("tool_call") or dados_tool
                conteudo = (conteudo[:idx] + conteudo[idx + fim:]).strip()
                conteudo = conteudo.replace("<system>", "").replace("</system>", "").strip()
            else:
                import re as _re2
                tc2 = _re2.search(r'\{"tool_call"\s*:\s*\{[^\}]*\}\}', conteudo)
                if tc2:
                    tc_dados = json.loads(tc2.group())
                    tool_call = tc_dados.get("tool_call") or tc_dados
                    conteudo = _re2.sub(r'\{"tool_call"\s*:\s*\{[^\}]*\}\}', '', conteudo).strip()
    except Exception:
        pass

    return conteudo, tool_call, mem, historico


@socketio.on("chat_ia_perguntar", namespace="/ia")
def responder_chat_ia(dados):
    pergunta = str((dados or {}).get("pergunta", "")).strip()
    sessao_id = str((dados or {}).get("sessao_id", "")).strip() or request.sid
    usuario = str((dados or {}).get("usuario", "")).strip() or USUARIO_PADRAO
    if not GROQ_API_KEY:
        emit("chat_ia_resposta", {"ok": False, "resposta": "IA nao configurada. Adicione GROQ_API_KEY no servidor."})
        return
    if not pergunta:
        emit("chat_ia_resposta", {"ok": False, "resposta": "Envie uma pergunta."})
        return
    if len(pergunta) > 4000:
        emit("chat_ia_resposta", {"ok": False, "resposta": "Pergunta muito longa (maximo: 4000 caracteres)."})
        return

    try:
        conteudo, tool_call, mem, historico = _responder_ia_core(pergunta, sessao_id, usuario=usuario)

        if tool_call:
            _perguntas_pendentes[sessao_id] = {"pergunta": pergunta, "usuario": usuario}
            resultado = _executar_tool_call(tool_call, sessao_id)
            if not resultado.get("ok"):
                _perguntas_pendentes.pop(sessao_id, None)
                emit("chat_ia_resposta", {"ok": False, "resposta": f"Erro ao executar acao: {resultado.get('erro')}", "sessao_id": sessao_id})
            return

        historico.append({"role": "user", "content": pergunta})
        historico.append({"role": "assistant", "content": conteudo})
        if len(historico) > MAX_HISTORICO:
            historico = historico[-MAX_HISTORICO:]
        mem["historico"] = historico

        _save_mem(sessao_id, mem)
        # memoria de longo prazo: extrai fatos em background (nao trava a resposta)
        _talvez_extrair_async(usuario, sessao_id, pergunta)

        emit("chat_ia_resposta", {"ok": True, "resposta": conteudo, "sessao_id": sessao_id,
                                  "usuario": usuario})

    except requests.RequestException as erro:
        emit("chat_ia_resposta", {"ok": False, "resposta": f"Falha na IA: {erro}"})
    except (KeyError, IndexError, TypeError, ValueError):
        emit("chat_ia_resposta", {"ok": False, "resposta": "Resposta invalida da IA."})


@socketio.on("ia_resultado_acao", namespace="/ia")
def ia_resultado_acao(dados):
    """Compat: resultado de acao via /ia, delega a continuação server-side."""
    _continuar_acao(
        (dados or {}).get("sessao_id"),
        (dados or {}).get("resultado", {}),
        (dados or {}).get("pergunta_original", ""),
    )


# Chat / Task sockets (default namespace)
@socketio.on("connect")
def connect(auth=None):
    token = (auth or {}).get("token")
    if token != TOKEN_SESSAO:
        print(f"[RELAY] Conexao negada: token {token}")
        return False
    print("[RELAY] Cliente conectado (token valido).")
    remoto = _client_remoto()
    mudou = remoto and not _remoto_sids
    if remoto:
        _remoto_sids.add(request.sid)
    if mudou:
        socketio.emit("celular_presenca", {"online": True})
    else:
        emit("celular_presenca", {"online": bool(_remoto_sids)})


@socketio.on("disconnect")
def ao_desconectar():
    print("[RELAY] Cliente desconectou.", flush=True)
    if request.sid in _remoto_sids:
        _remoto_sids.discard(request.sid)
        if not _remoto_sids:
            socketio.emit("celular_presenca", {"online": False})


@socketio.on("chat_listar")
def chat_listar(dados):
    emit("chat_lista", {"mensagens": hub_store.listar_mensagens()})


@socketio.on("chat_enviar")
def chat_enviar(dados):
    origem = (dados or {}).get("from") or "android"
    texto = (dados or {}).get("texto", "").strip()
    tipo = (dados or {}).get("tipo", "texto")
    arquivo = (dados or {}).get("arquivo", "")
    msg_id = (dados or {}).get("id")

    msg = hub_store.enviar_texto_id(texto, origem=origem, msg_id=msg_id, tipo=tipo, arquivo=arquivo)
    socketio.emit("chat_nova", msg)
    print(f"[RELAY] {origem.upper()} -> {texto[:50]}")


@socketio.on("chat_apagar")
def chat_apagar(dados):
    msg_id = (dados or {}).get("id")
    hub_store.apagar_mensagem(msg_id)
    socketio.emit("chat_apagada", {"id": msg_id})


@socketio.on("task_listar")
def task_listar(dados):
    emit("task_lista", {"tasks": hub_store.listar_tasks()})


@socketio.on("task_nova")
def task_nova(dados):
    titulo = (dados or {}).get("titulo", "").strip()
    nova, _ = hub_store.criar_task(titulo)
    socketio.emit("task_nova", nova)


@socketio.on("task_atualizada")
def task_atualizada(dados):
    tid = (dados or {}).get("id")
    feito = bool((dados or {}).get("feito", False))
    lista = hub_store.marcar_task(tid, feito)
    t = next((x for x in lista if x["id"] == tid), None)
    if t:
        socketio.emit("task_atualizada", t)


@socketio.on("task_apagada")
def task_apagar(dados):
    tid = (dados or {}).get("id")
    hub_store.apagar_task(tid)
    socketio.emit("task_apagada", {"id": tid})


@socketio.on("notificacao_celular")
def notificacao_recebida_relay(dados):
    """Recebe notificacao do Android (via Arch bridge) e broadcasta pro overlay."""
    socketio.emit("notificacao_nova", dados)
    print(f"[RELAY] Notificacao Android -> {dados.get('app')}: {dados.get('titulo')}")


# Handlers para bridge Arch -> IA actions
@socketio.on("ia_executar_scan")
def ia_executar_scan(dados):
    """Bridge chama isso quando scan completou no Termux."""
    sessao_id = (dados or {}).get("sessao_id")
    resultado = dados.get("resultado", {})
    pergunta_original = (dados or {}).get("pergunta_original", "")
    if sessao_id and resultado:
        _continuar_acao(sessao_id, resultado, pergunta_original)


@socketio.on("ia_executar_comando")
def ia_executar_comando(dados):
    """Bridge chama isso quando comando completou no Arch."""
    sessao_id = (dados or {}).get("sessao_id")
    resultado = dados.get("resultado", {})
    pergunta_original = (dados or {}).get("pergunta_original", "")
    if sessao_id and resultado:
        _continuar_acao(sessao_id, resultado, pergunta_original)


@socketio.on("ia_obter_info")
def ia_obter_info(dados):
    """Bridge chama isso quando info completou."""
    sessao_id = (dados or {}).get("sessao_id")
    resultado = dados.get("resultado", {})
    pergunta_original = (dados or {}).get("pergunta_original", "")
    if sessao_id and resultado:
        _continuar_acao(sessao_id, resultado, pergunta_original)


def _continuar_acao(sessao_id, resultado, pergunta_original):
    """Continua a conversa com o resultado da acao executada (server-side)."""
    if not sessao_id or not resultado:
        return
    if sessao_id not in _perguntas_pendentes:
        return
    _pend = _perguntas_pendentes.pop(sessao_id)
    usuario = _pend.get("usuario", USUARIO_PADRAO) if isinstance(_pend, dict) else USUARIO_PADRAO
    # a ponte (Arch) pode devolver a pergunta original como dict; o texto vai
    # para o historico da IA e precisa ser str
    if isinstance(pergunta_original, dict):
        pergunta_original = str(pergunta_original.get("pergunta", ""))
    else:
        pergunta_original = str(pergunta_original or "")

    try:
        historico_extra = [
            {"role": "assistant", "content": "Acao executada."},
            {"role": "user", "content": f"Resultado da acao: {json.dumps(resultado, ensure_ascii=False)}"},
            {"role": "assistant", "content": pergunta_original or "Continue."}
        ]
        conteudo, tool_call, mem, historico = _responder_ia_core(
            f"Resultado da acao: {json.dumps(resultado, ensure_ascii=False)}\nAgora responda ao usuario.",
            sessao_id,
            historico_extra,
            usuario=usuario,
        )

        historico.append({"role": "assistant", "content": conteudo})
        if len(historico) > MAX_HISTORICO:
            historico = historico[-MAX_HISTORICO:]
        mem["historico"] = historico
        _save_mem(sessao_id, mem)
        _talvez_extrair_async(usuario, sessao_id, pergunta_original or "")

        socketio.emit("chat_ia_resposta", {"ok": True, "resposta": conteudo,
                                           "sessao_id": sessao_id, "usuario": usuario}, namespace="/ia")

    except Exception as e:
        socketio.emit("chat_ia_resposta", {"ok": False, "resposta": f"Falha ao processar resultado: {e}"}, namespace="/ia")


# Media upload / serve
RELATORIOS_DIR = Path(os.environ.get("SPOOK_RELATORIOS_DIR", "/home/ubuntu/spook-relay/data/relatorios"))
RELATORIOS_DRIVE = "gdrive:SPOOK/relatorios"


ARCH_URL = os.environ.get("SPOOK_ARCH_URL", "http://100.78.69.110:5000").rstrip("/")


def _converter_no_arch(destino, nome):
    """Pede ao Arch para gerar o .docx e subir no Drive.

    So entra como RESERVA: o relay gera o .docx localmente e so
    chama o notebook se isso falhar. Conectando com o notebook desligado o
    Tailscale nao responde nada, e com timeout=300 o upload ficava 5 minutos
    parado antes de cair no .log cru — por isso o connect timeout e curtinho.
    Devolve a lista de documentos gerados, ou None se deu falha (aí o relay
    sobe o .log cru como reserva e o app ainda mostra o texto).
    """
    try:
        with open(str(destino), "r", encoding="utf-8", errors="replace") as arq:
            texto = arq.read()
        base, _ext = os.path.splitext(nome)
        r = requests.post(
            ARCH_URL + "/relatorio/ingressar",
            json={"nome": base, "texto": texto},
            headers={"Authorization": "Bearer " + TOKEN_SESSAO},
            timeout=(4, 120),
        )
        if r.status_code == 200:
            docs = (r.json() or {}).get("documentos") or []
            if docs:
                return docs
        print("[relay] arch recusou a conversao: %s %s" % (r.status_code, r.text[:120]), flush=True)
    except Exception as e:
        print("[relay] sem conversao no arch (%s) — subindo o .log cru" % e, flush=True)
    return None


def _documentos_no_arch(destino, nome):
    """Gera o .docx aqui no relay (OOXML na mao) e sobe pro Drive.

    O notebook pode estar desligado: a conversao tem que funcionar na nuvem.
    Devolve a lista de arquivos que foram para o Drive, ou None.
    """
    try:
        import relatorio_doc
    except Exception as e:
        print("[relay] relatorio_doc ausente (%s) — usando o arch como reserva" % e, flush=True)
        return None
    try:
        with open(str(destino), "r", encoding="utf-8", errors="replace") as arq:
            texto = arq.read()
        base = os.path.splitext(nome)[0]
        docs = relatorio_doc.gerar_documentos(str(RELATORIOS_DIR), base, texto)
        subidos = []
        # so o .docx sobe pro Drive: e o que o Taylor EDITA no Drive/Google Docs,
        # ja com a fonte mono e a cor por trecho da janela REPORTS. O .txt nao
        # sobe porque nao tem fonte nem cor nenhuma, e o app le o texto do
        # mirror local (que e persistente).
        for caminho in (docs.get("docx"),):
            if not caminho:
                continue
            p = hub_store._run(["copyto", str(caminho), RELATORIOS_DRIVE + "/" + Path(caminho).name],
                               check=False, timeout=180)
            if p.returncode == 0:
                subidos.append(Path(caminho).name)
        if subidos:
            return subidos
        print("[relay] docx local nao gerou documento: %s" % (docs.get("erros") or ["?"]), flush=True)
    except Exception as e:
        print("[relay] conversao local falhou (%s) — tentando no arch" % e, flush=True)
    return None


def _subir_relatorio(origem, nome, tentativas=4):
    """Espelha o relatorio no relay, gera o DOCX aqui mesmo e sobe pro Drive.
    Reserva: se a conversao falhar (nem local nem no arch), sobe o texto cru
    para o relatorio nao sumir. Devolve o nome do que foi para o Drive."""
    RELATORIOS_DIR.mkdir(parents=True, exist_ok=True)
    destino = RELATORIOS_DIR / nome
    try:
        if os.path.exists(str(destino)):
            base, ext = os.path.splitext(nome)
            destino = RELATORIOS_DIR / ("%s_%d%s" % (base, int(time.time()) % 100000, ext))
        import shutil
        shutil.copyfile(origem, str(destino))
    except Exception as e:
        return nome
    docs = _documentos_no_arch(destino, destino.name) or _converter_no_arch(destino, destino.name)
    if docs:
        return ", ".join(docs)
    # Sem DOCX nao sobe texto cru: o Taylor so quer o editavel no Drive (o
    # que abria "nao". O arquivo cru continua no mirror local).
    print("[relay] docx nao gerou para %s; texto cru so no mirror local" % destino.name, flush=True)
    return destino.name


@app.route("/upload", methods=["POST"])
def upload():
    auth = request.headers.get("Authorization", "")
    if auth.replace("Bearer ", "").strip() != TOKEN_SESSAO:
        return {"ok": False, "erro": "Unauthorized"}, 401

    if "file" not in request.files:
        return {"ok": False, "erro": "No file"}, 400

    file = request.files["file"]
    msg_id = request.form.get("id", "")
    tipo = request.form.get("tipo", "arquivo")
    texto = request.form.get("texto", "")
    pasta = (request.form.get("pasta", "") or "").strip().lower()

    nome = os.path.basename(file.filename).replace(" ", "_")
    # mesmo diretorio que o hub_store espelha (SPOOK_MEDIA_DIR no unit);
    # /tmp se perde no reboot e deixava o outbox sem arquivo de origem
    media_dir = Path(os.environ.get("SPOOK_MEDIA_DIR", "/tmp/spook_media_local"))
    media_dir.mkdir(exist_ok=True)
    caminho_final = media_dir / nome
    file.save(str(caminho_final))

    # pasta=relatorios: relatorio de scan vai para gdrive:SPOOK/relatorios,
    # que e a pasta que o botao REPORTS do app abre. Nao entra no chat.
    if pasta == "relatorios":
        nome = _subir_relatorio(str(caminho_final), nome)
        return {"ok": True, "filename": nome, "pasta": "relatorios"}

    msg = hub_store.enviar_midia_server(msg_id, texto, "android", tipo, nome, str(caminho_final))
    socketio.emit("chat_nova", msg)

    return {"ok": True, "filename": nome}


@app.route("/media/<path:filename>")
def servir_media(filename):
    nome = os.path.basename(filename)
    path = hub_store.baixar_midia(nome)
    if path and os.path.exists(path):
        return send_file(path)
    return "Not Found", 404


@app.route("/media")
def rota_listar_media():
    auth = request.headers.get("Authorization", "")
    if auth.replace("Bearer ", "").strip() != TOKEN_SESSAO:
        return {"ok": False, "erro": "Unauthorized"}, 401
    return {
        "items": [{"name": n} for n in hub_store.listar_media()],
        "url_drive": "https://drive.google.com/drive/search?q=%22SPOOK%22",
    }


# ====================== RELATORIOS (rota nova: 27/09) ==========================
# O botao REPORTS do app falhava com o notebook desligado porque falava com
# /relatorios do Arch. Agora quem responde e o relay, que fica na nuvem: o Drive
# (gdrive:SPOOK/relatorios) e a fonte da verdade e o mirror local so acelera a
# leitura do texto.


def _token_ok() -> bool:
    auth = request.headers.get("Authorization", "")
    return auth.replace("Bearer ", "").strip() == TOKEN_SESSAO


def _nome_seguro(nome: str) -> str:
    return Path(str(nome).rsplit("/", 1)[-1]).name


def _listar_drive(pasta):
    p = hub_store._run(["lsjson", pasta], check=False, timeout=120)
    if p.returncode != 0:
        return []
    try:
        return [it for it in json.loads(p.stdout.decode("utf-8", "replace")) if not it.get("IsDir")]
    except Exception as e:
        print("[relay] lsjson quebrou: %s" % e, flush=True)
        return []


def _apagar_drive(pasta, base):
    """deletefile (nao purge): some da listagem do Drive sem levar a pasta junto."""
    ok = 0
    for ext in ("log", "txt", "md", "pdf", "docx"):
        alvo = "%s/%s.%s" % (pasta, base, ext)
        p = hub_store._run(["deletefile", alvo], check=False, timeout=90)
        if p.returncode == 0:
            ok += 1
    return ok


def _base_sem_ext(nome):
    bruto = _nome_seguro(nome)
    for ext in (".log", ".txt", ".md", ".pdf", ".docx"):
        if bruto.lower().endswith(ext):
            return bruto[: -len(ext)]
    return bruto


@app.route("/relatorios")
def rota_listar_relatorios():
    auth = request.headers.get("Authorization", "")
    if auth.replace("Bearer ", "").strip() != TOKEN_SESSAO:
        return {"ok": False, "erro": "Unauthorized"}, 401
    try:
        entradas = _listar_drive(RELATORIOS_DRIVE)
        # agrupa por base: um relatorio pode ter .log/.txt + .docx
        grupos = {}
        for it in entradas:
            nome = str(it.get("Name") or it.get("Path") or "")
            if not nome:
                continue
            base = _base_sem_ext(nome)
            g = grupos.setdefault(base, {"nome": base, "bytes": 0, "modificado": "", "iso": "", "documentos": []})
            ext = nome[len(base):].lstrip(".").lower()
            if ext in ("pdf", "docx", "md"):
                g["documentos"].append(ext)
            # metadados vem da maior variante (o .log cru quando ele subiu junto)
            tamanho = int(it.get("Size") or 0)
            if tamanho >= g["bytes"]:
                g["bytes"] = tamanho
                g["modificado"] = _data_br(str(it.get("ModTime") or ""))
                g["iso"] = str(it.get("ModTime") or "")
        itens = sorted(grupos.values(), key=lambda g: g["modificado"], reverse=True)[:200]
        return {"ok": True, "items": itens, "total": len(grupos)}
    except Exception as e:
        return {"ok": False, "erro": str(e)}, 500


def _data_br(iso: str) -> str:
    """'2026-09-27T11:35:23.000Z' -> '2026-09-27 11:35:23' (o app mostra esse formato)."""
    if not iso:
        return ""
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso[:19].replace("T", " ")


@app.route("/relatorios/<path:filename>")
def rota_baixar_relatorio(filename):
    if not _token_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    nome = _nome_seguro(filename)
    if not nome or ".." in nome:
        return {"ok": False, "erro": "File not found"}, 404
    # o app pede pela BASE (sem extensao, como a lista agrupa): tenta o texto
    # cru primeiro, depois o que veio nomeado com extensao.
    candidatos = [nome]
    if _base_sem_ext(nome) == nome:
        candidatos = [nome + ".txt", nome + ".log", nome]
    # 1) mirror local (rapido), 2) texto cru que veio no upload, 3) Drive
    for cand in candidatos:
        for dir_atual in (RELATORIOS_DIR, Path(os.environ.get("SPOOK_MEDIA_DIR", "/tmp/spook_media_local"))):
            caminho = dir_atual / cand
            try:
                if caminho.exists() and caminho.stat().st_size > 0:
                    return send_file(str(caminho))
            except Exception:
                pass
        p = hub_store._run(["cat", RELATORIOS_DRIVE + "/" + cand], check=False, timeout=120)
        if p.returncode == 0 and p.stdout:
            return p.stdout, 200, {"Content-Type": "text/plain; charset=utf-8"}
    return {"ok": False, "erro": "File not found"}, 404


@app.route("/relatorios", methods=["DELETE"])
def rota_apagar_relatorios():
    """Apaga varios relatorios (multi-selecao da janela REPORTS) no Drive e no mirror."""
    if not _token_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    try:
        dados = request.get_json(silent=True) or {}
        nomes = dados.get("nomes") or []
        if isinstance(nomes, str):
            nomes = [nomes]
        apagados = []
        for nome in list(nomes)[:200]:
            base = _base_sem_ext(nome)
            if not base:
                continue
            apagados.append(base)
            try:
                (RELATORIOS_DIR / (base + ".log")).unlink(missing_ok=True)
                (RELATORIOS_DIR / (base + ".txt")).unlink(missing_ok=True)
            except Exception:
                pass
            _apagar_drive(RELATORIOS_DRIVE, base)
        return {"ok": True, "apagados": apagados, "total": len(apagados)}
    except Exception as e:
        return {"ok": False, "erro": str(e)}, 500

# ============================ IA: memoria compartilhada ============================
def _ia_token_ok() -> bool:
    auth = request.headers.get("Authorization", "")
    return auth.replace("Bearer ", "").strip() == TOKEN_SESSAO


@app.route("/ia/memoria")
def ia_memoria_http():
    """Ver/editar a memoria de longo prazo (a mesma nos dois dispositivos)."""
    if not _ia_token_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    usuario = (request.args.get("usuario") or "").strip() or USUARIO_PADRAO
    if request.method == "DELETE":
        u = {"perfil": [], "instrucoes": ""}
        _save_usuario(usuario, u)
        return {"ok": True, "perfil": [], "instrucoes": ""}
    if request.method == "POST":
        corpo = request.get_json(force=True, silent=True) or {}
        u = _load_usuario(usuario)
        if "instrucoes" in corpo:
            u["instrucoes"] = str(corpo.get("instrucoes") or "")[:4000]
        if isinstance(corpo.get("perfil"), list):
            u["perfil"] = [str(x).strip() for x in corpo["perfil"] if str(x).strip()][-60:]
        if isinstance(corpo.get("adicionar"), list):
            for x in corpo["adicionar"]:
                fs = str(x).strip()
                if fs and not any(fs.lower() in p.lower() or p.lower() in fs.lower() for p in u["perfil"]):
                    u["perfil"].append(fs)
            u["perfil"] = u["perfil"][-60:]
        _save_usuario(usuario, u)
        return {"ok": True, "usuario": usuario, "perfil": u["perfil"], "instrucoes": u["instrucoes"]}
    u = _load_usuario(usuario)
    return {"ok": True, "usuario": usuario, "perfil": u.get("perfil", []),
            "instrucoes": u.get("instrucoes", ""), "atualizado": u.get("atualizado")}


@socketio.on("ia_memoria", namespace="/ia")
def ia_memoria_socket(dados):
    """O app/overlay pede a memoria; se mandar 'instrucoes', edita."""
    d = dados or {}
    usuario = str(d.get("usuario", "")).strip() or USUARIO_PADRAO
    u = _load_usuario(usuario)
    if d.get("instrucoes") is not None:
        u["instrucoes"] = str(d.get("instrucoes") or "")[:4000]
        _save_usuario(usuario, u)
    if d.get("limpar"):
        u["perfil"] = []
        _save_usuario(usuario, u)
    emit("ia_memoria", {"ok": True, "usuario": usuario, "perfil": u.get("perfil", []),
                        "instrucoes": u.get("instrucoes", ""), "atualizado": u.get("atualizado")})


def _hook_chat_externo(msg):
    """Msg que chegou pelo sync do Drive (escrita no Arch/Android): reemite
    para os clientes do relay, senao o app so ve no proximo chat_listar."""
    try:
        socketio.emit("chat_nova", msg)
    except Exception:
        pass


try:
    hub_store.set_chat_hook(_hook_chat_externo)
except Exception:
    pass


@app.route("/healthz")
def health():
    return "OK", 200

if __name__ == "__main__":
    _migra_perfis_antigos()
    socketio.run(app, host="0.0.0.0", port=5000, allow_unsafe_werkzeug=True)
