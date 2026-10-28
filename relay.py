import requests
import uuid
from flask import Flask, request, send_file
from flask_socketio import SocketIO, emit
from pathlib import Path
import threading
import time
import os
import json
import hub_store
import pad_store
import fcntl


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
def _token_sessao() -> str:
    """Token de pareamento dos aparelhos.

    NUNCA deixar o valor no codigo: o repo e publico. Vem do .env
    (SPOOK_TOKEN) ou, na falta dele, de ~/.pentestlink/token_sessao.txt
    (o mesmo arquivo que o servidor do Arch usa).
    """
    t = _ENV.get("SPOOK_TOKEN", "").strip()
    if t:
        return t
    try:
        return (Path.home() / ".pentestlink" / "token_sessao.txt").read_text().strip()
    except Exception:
        return ""


TOKEN_SESSAO = _token_sessao()
if not TOKEN_SESSAO:
    print("[AVISO] SPOOK_TOKEN vazio: nenhum aparelho conseguira autenticar", flush=True)

# Track connected clients
CONNECTED_CLIENTS = {}  # sid -> {"device": "android|arch|overlay|unknown", "connected_at": time}


# Memoria da IA (armazenada na VM)
MEMORIA_DIR = Path("/home/ubuntu/spook-relay/data/memoria_ia")
MEMORIA_DIR.mkdir(parents=True, exist_ok=True)
MAX_HISTORICO = 20
CONSOLIDA_CADA = 10


def _mem_path(sessao_id: str) -> Path:
    safe = "".join(c for c in sessao_id if c.isalnum() or c in "-_").strip()
    if not safe:
        safe = "default"
    return MEMORIA_DIR / f"{safe}.json"


def _load_mem(sessao_id: str) -> dict:
    path = _mem_path(sessao_id)
    if not path.exists():
        return {"perfil": [], "historico": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"perfil": [], "historico": []}


def _save_mem(sessao_id: str, data: dict):
    path = _mem_path(sessao_id)
    try:
        with open(path, "w", encoding="utf-8") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            json.dump(data, f, ensure_ascii=False, indent=2)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass


def _maybe_consolidate(sessao_id: str, mem: dict):
    hist = mem.get("historico", [])
    if len(hist) % CONSOLIDA_CADA != 0:
        return
    if len(hist) < CONSOLIDA_CADA:
        return
    recentes = hist[-(CONSOLIDA_CADA * 2):]
    texto = "\n".join(f"{m['role']}: {m['content']}" for m in recentes)
    try:
        prompt = (
            "Extraia 3-5 fatos duraveis sobre o usuario desta conversa recente. "
            "Fatos = preferencias, nomes, projetos, contexto tecnico, objetivos. "
            "Retorne apenas uma lista JSON de strings, sem explicacao.\n\n"
            f"Conversa:\n{texto}"
        )
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": GROQ_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 500,
                "temperature": 0.2,
            },
            timeout=30,
        )
        r.raise_for_status()
        facts_text = r.json()["choices"][0]["message"]["content"].strip()
        try:
            facts = json.loads(facts_text)
            if isinstance(facts, list):
                perfil = mem.setdefault("perfil", [])
                for f in facts:
                    fs = str(f).strip()
                    if fs and not any(fs.lower() in p.lower() or p.lower() in fs.lower() for p in perfil):
                        perfil.append(fs)
                if len(perfil) > 30:
                    perfil[:] = perfil[-30:]
                _save_mem(sessao_id, mem)
        except json.JSONDecodeError:
            pass
    except Exception:
        pass


def _executar_tool_call(tool_call: dict, sessao_id: str) -> dict:
    """Executa uma acao solicitada pela IA. Retorna dict com resultado."""
    acao = tool_call.get("acao")
    params = tool_call.get("params", {})

    if acao == "executar_scan":
        scan_nome = params.get("scan")
        alvo = params.get("alvo", "")
        if not scan_nome:
            return {"ok": False, "erro": "scan nao especificado"}
        if relay_bridge_sio and relay_bridge_sio.connected:
            try:
                relay_bridge_sio.emit("ia_executar_scan", {"scan": scan_nome, "alvo": alvo, "sessao_id": sessao_id})
                return {"ok": True, "status": "scan_enviado_para_execucao", "scan": scan_nome, "alvo": alvo}
            except Exception as e:
                return {"ok": False, "erro": f"falha ao enviar scan: {e}"}
        return {"ok": False, "erro": "bridge_nao_conectado"}

    elif acao == "executar_comando_notebook":
        comando_nome = params.get("comando")
        if not comando_nome:
            return {"ok": False, "erro": "comando nao especificado"}
        if relay_bridge_sio and relay_bridge_sio.connected:
            try:
                relay_bridge_sio.emit("ia_executar_comando", {"comando": comando_nome, "sessao_id": sessao_id})
                return {"ok": True, "status": "comando_enviado_para_execucao", "comando": comando_nome}
            except Exception as e:
                return {"ok": False, "erro": f"falha ao enviar comando: {e}"}
        return {"ok": False, "erro": "bridge_nao_conectado"}

    elif acao == "obter_info_sistema":
        if relay_bridge_sio and relay_bridge_sio.connected:
            try:
                relay_bridge_sio.emit("ia_obter_info", {"sessao_id": sessao_id})
                return {"ok": True, "status": "info_solicitada"}
            except Exception as e:
                return {"ok": False, "erro": f"falha: {e}"}
        return {"ok": False, "erro": "bridge_nao_conectado"}

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
        "Voce pode ajudar com qualquer assunto: tecnologia, pentest, redes, Linux, programacao, e tambem com "
        "perguntas gerais do dia a dia. "
        "Quando o usuario pedir para executar um scan ou comando do proprio app, voce pode:\n"
        "1) Explicar como fazer\n"
        "2) Emitir um evento de acao para o sistema executar (scans no Termux, comandos no notebook)\n"
        "Sempre pergunte confirmacao antes de executar algo destrutivo.\n\n"
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

# Bridge para o Arch (chat/tasks/notificacoes/ia_acoes)
relay_bridge_sio = None


# IA namespace (/ia)
@socketio.on("connect", namespace="/ia")
def conectar_ia():
    print("[DEBUG] conectar_ia: GROQ_API_KEY=" + str(bool(GROQ_API_KEY)))
    print("[DEBUG] /ia namespace connect handler triggered")
    print("[DEBUG] conectar_ia called, GROQ_API_KEY set: " + str(bool(GROQ_API_KEY)))
    print("[DEBUG] conectar_ia called, GROQ_API_KEY=" + ("set" if GROQ_API_KEY else "EMPTY"))
    if not GROQ_API_KEY:
        return False


def _responder_ia_core(pergunta, sessao_id, historico_extra=None):
    """Core logic para responder IA, usado tanto pelo handler principal quanto pelo loop."""
    mem = _load_mem(sessao_id)
    perfil = mem.get("perfil", [])
    historico = mem.get("historico", [])[-MAX_HISTORICO:]

    system_content = montar_instrucoes_ia()
    if perfil:
        system_content += "\n\n[Perfil do usuario - fatos conhecidos:]\n" + "\n".join(f"- {p}" for p in perfil)
    if historico:
        system_content += "\n\n[Historico recente da conversa:]\n" + "\n".join(
            f"{'Usuario' if m['role'] == 'user' else 'SPOOK'}: {m['content'][:200]}"
            for m in historico
        )
    if historico_extra:
        for m in historico_extra:
            role = "Usuario" if m['role'] == 'user' else 'SPOOK'
            system_content += f"\n{role}: {m['content'][:200]}"

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
        tc_match = _re.search(r'\{"tool_call"\s*:\s*\{.*?\}\}', conteudo, _re.DOTALL)
        if tc_match:
            tool_call = json.loads(tc_match.group())
            conteudo = _re.sub(r'\{"tool_call"\s*:\s*\{.*?\}\}', '', conteudo, flags=_re.DOTALL).strip()
    except Exception:
        pass

    return conteudo, tool_call, mem, historico


@socketio.on("chat_ia_perguntar", namespace="/ia")
def responder_chat_ia(dados):
    pergunta = str((dados or {}).get("pergunta", "")).strip()
    sessao_id = str((dados or {}).get("sessao_id", "")).strip() or request.sid
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
        conteudo, tool_call, mem, historico = _responder_ia_core(pergunta, sessao_id)

        # Atualiza memoria com pergunta + resposta
        historico.append({"role": "user", "content": pergunta})
        historico.append({"role": "assistant", "content": conteudo})
        if len(historico) > MAX_HISTORICO:
            historico = historico[-MAX_HISTORICO:]
        mem["historico"] = historico

        _maybe_consolidate(sessao_id, mem)
        _save_mem(sessao_id, mem)

        emit("chat_ia_resposta", {"ok": True, "resposta": conteudo, "sessao_id": sessao_id})

        if tool_call:
            resultado = _executar_tool_call(tool_call, sessao_id)
            socketio.emit("ia_resultado_acao", {"sessao_id": sessao_id, "resultado": resultado, "pergunta_original": pergunta}, namespace="/ia")

    except requests.RequestException as erro:
        emit("chat_ia_resposta", {"ok": False, "resposta": f"Falha na IA: {erro}"})
    except (KeyError, IndexError, TypeError, ValueError):
        emit("chat_ia_resposta", {"ok": False, "resposta": "Resposta invalida da IA."})


@socketio.on("ia_resultado_acao", namespace="/ia")
def ia_resultado_acao(dados):
    """Recebe resultado de acao executada e continua a conversa com a IA."""
    sessao_id = (dados or {}).get("sessao_id")
    resultado = (dados or {}).get("resultado", {})
    pergunta_original = (dados or {}).get("pergunta_original", "")

    if not sessao_id or not resultado:
        return

    try:
        # Build follow-up messages: pergunta original + resultado
        historico_extra = [
            {"role": "assistant", "content": "Acao executada."},
            {"role": "user", "content": f"Resultado da acao: {json.dumps(resultado, ensure_ascii=False)}"}
        ]
        conteudo, tool_call, mem, historico = _responder_ia_core(
            f"Resultado da acao anterior: {json.dumps(resultado, ensure_ascii=False)}", 
            sessao_id, 
            historico_extra
        )

        # Atualiza memoria
        historico.append({"role": "assistant", "content": conteudo})
        if len(historico) > MAX_HISTORICO:
            historico = historico[-MAX_HISTORICO:]
        mem["historico"] = historico
        _maybe_consolidate(sessao_id, mem)
        _save_mem(sessao_id, mem)

        emit("chat_ia_resposta", {"ok": True, "resposta": conteudo, "sessao_id": sessao_id})

        if tool_call:
            novo_resultado = _executar_tool_call(tool_call, sessao_id)
            socketio.emit("ia_resultado_acao", {"sessao_id": sessao_id, "resultado": novo_resultado, "pergunta_original": pergunta_original}, namespace="/ia")

    except Exception as e:
        emit("chat_ia_resposta", {"ok": False, "resposta": f"Falha ao processar resultado: {e}"})


# Bridge para o Arch
relay_bridge_sio = None


# IA namespace (/ia)
def conectar_ia():
    if not GROQ_API_KEY:
        return False


# Chat / Task sockets (default namespace)
@socketio.on("connect")
def connect(auth=None):
    token = (auth or {}).get("token")
    if token != TOKEN_SESSAO:
        print(f"[RELAY] Conexao negada: token {token}")
        return False
    sid = request.sid
    # Determine device type from auth or default
    device = (auth or {}).get("device", "unknown")
    CONNECTED_CLIENTS[sid] = {"device": device, "connected_at": time.time()}
    print(f"[RELAY] Cliente conectado: {sid} ({device})")


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


# ================================ PAD (notas) ================================
@socketio.on("pad_listar")
def pad_listar(dados):
    emit("pad_lista", {"notas": pad_store.listar()})


@socketio.on("pad_salvar")
def pad_salvar(dados):
    d = dados or {}
    res = pad_store.salvar(
        d.get("id", ""),
        d.get("titulo", ""),
        d.get("texto", ""),
        int(d.get("em") or 0),
        d.get("base_hash", ""),
        d.get("device", "android"),
    )
    emit("pad_estado", res)
    if not res.get("conflito") and not res.get("antiga") and not res.get("igual"):
        socketio.emit("pad_atualizada", res["nota"])


@socketio.on("pad_apagar")
def pad_apagar_socket(dados):
    nid = (dados or {}).get("id")
    rec = pad_store.apagar(nid, (dados or {}).get("device", "android"))
    socketio.emit("pad_apagada", {"id": nid, "removido_em": rec.get("removido_em", 0)})


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
        socketio.emit("ia_resultado_acao", {"sessao_id": sessao_id, "resultado": resultado, "pergunta_original": pergunta_original}, namespace="/ia")


@socketio.on("ia_executar_comando")
def ia_executar_comando(dados):
    """Bridge chama isso quando comando completou no Arch."""
    sessao_id = (dados or {}).get("sessao_id")
    resultado = dados.get("resultado", {})
    pergunta_original = (dados or {}).get("pergunta_original", "")
    if sessao_id and resultado:
        socketio.emit("ia_resultado_acao", {"sessao_id": sessao_id, "resultado": resultado, "pergunta_original": pergunta_original}, namespace="/ia")


@socketio.on("ia_obter_info")
def ia_obter_info(dados):
    """Bridge chama isso quando info completou."""
    sessao_id = (dados or {}).get("sessao_id")
    resultado = dados.get("resultado", {})
    pergunta_original = (dados or {}).get("pergunta_original", "")
    if sessao_id and resultado:
        socketio.emit("ia_resultado_acao", {"sessao_id": sessao_id, "resultado": resultado, "pergunta_original": pergunta_original}, namespace="/ia")


# Media upload / serve
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

    nome = os.path.basename(file.filename).replace(" ", "_")
    media_dir = Path("/tmp/spook_media_local")
    media_dir.mkdir(exist_ok=True)
    caminho_final = media_dir / nome
    file.save(str(caminho_final))

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
    # O app (galeria do HUB) mostra tamanho e ordena por ele: sem "bytes" o
    # org.json estourava "No value for bytes" e a galeria caia em erro.
    itens = []
    for nome in hub_store.listar_media():
        item = {"name": nome, "bytes": 0, "mtime": 0}
        try:
            st = (hub_store._MEDIA_DIR / nome).stat()
            item["bytes"] = int(st.st_size)
            item["mtime"] = int(st.st_mtime * 1000)
        except Exception:
            pass
        itens.append(item)
    itens.sort(key=lambda i: i["mtime"], reverse=True)
    return {
        "items": itens,
        "url_drive": "https://drive.google.com/drive/search?q=%22SPOOK%22",
    }



@app.route("/reading/<path:filename>")
def rota_baixar_reading(filename):
    auth = request.headers.get("Authorization", "")
    if auth.replace("Bearer ", "").strip() != TOKEN_SESSAO:
        return {"ok": False, "erro": "Unauthorized"}, 401
    import urllib.request
    try:
        # Download from gdrive:SPOOK/reading
        import subprocess, tempfile, os
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        r = subprocess.run(["rclone", "copyto", f"gdrive:SPOOK/reading/{filename}", tmp_path], capture_output=True, timeout=60)
        if r.returncode != 0:
            os.unlink(tmp_path)
            return {"ok": False, "erro": "not_found"}, 404
        with open(tmp_path, "rb") as f:
            data = f.read()
        os.unlink(tmp_path)
        from flask import send_file
        import io
        return send_file(io.BytesIO(data), as_attachment=False, download_name=filename, mimetype="application/pdf")
    except Exception as e:
        return {"ok": False, "erro": str(e)}, 500

# ============================ LEITURA (sincronismo onde parei) ============================
@socketio.on("leitura_puxar")
def leitura_puxar(dados):
    try:
        d = dados or {}
        # Token already validated on connect, no need to re-check
        # but keep for backward compatibility with HTTP fallback
        token = d.get("token") or request.args.get("token")
        if token and token != TOKEN_SESSAO:
            emit("leitura_resposta", {"ok": False, "erro": "token_invalido"})
            return
        bookId = str(d.get("bookId") or "").strip()
        if not bookId:
            emit("leitura_resposta", {"ok": False, "erro": "bookId_obrigatorio"})
            return
        safe = "".join(c for c in bookId if c.isalnum() or c in "-_")
        res = hub_store.obter_leitura(safe)
        if not res:
            emit("leitura_resposta", {"ok": True, "bookId": safe, "vazio": True})
            return
        emit("leitura_resposta", {"ok": True, **res})
    except Exception as e:
        try:
            emit("leitura_resposta", {"ok": False, "erro": str(e)})
        except Exception:
            pass



@socketio.on("leitura_atualizar")
def leitura_atualizar(dados):
    try:
        d = dados or {}
        # Token already validated on connect
        token = d.get("token") or request.args.get("token")
        if token and token != TOKEN_SESSAO:
            emit("leitura_resposta", {"ok": False, "erro": "token_invalido"})
            return
        bookId = str(d.get("bookId") or "").strip()
        if not bookId:
            emit("leitura_resposta", {"ok": False, "erro": "bookId_obrigatorio"})
            return
        safe = "".join(c for c in bookId if c.isalnum() or c in "-_")
        res = hub_store.atualizar_leitura(safe, d)
        if res:
            payload = {"ok": True, **res}
            emit("leitura_resposta", payload)
            try:
                socketio.emit("leitura_estado", payload)
            except Exception:
                pass
        else:
            emit("leitura_resposta", {"ok": False, "erro": "falha"})
    except Exception as e:
        try:
            emit("leitura_resposta", {"ok": False, "erro": str(e)})
        except Exception:
            pass





@socketio.on("leitura_listar_drive")
def leitura_listar_drive(dados):
    try:
        livros = hub_store.listar_livros_drive()
        emit("leitura_listar_drive_resposta", {"ok": True, "livros": livros})
    except Exception as e:
        emit("leitura_listar_drive_resposta", {"ok": False, "erro": str(e)})

# ============================ LIVROS v2 (ficha por livro + prateleira) ============================
# Protocolo novo, com timestamps do cliente para funcionar offline:
#   prateleira_listar -> prateleira_lista {livros:[ficha]}
#   livro_puxar  {bookId}                -> livro_resposta {livro}
#   livro_atualizar {bookId, page, offsetY, percent, em, origem, done?, removido?}
#                                        -> livro_resposta + broadcast livro_estado
#   livro_apagar {bookId}                -> tombstone (arquivo fica no Drive)
#   livro_publicar {bookId, titulo, ext, tamanho, arquivo:{noDrive,nome}} -> anuncia

def _skip_self(evento, payload):
    try:
        socketio.emit(evento, payload, skip_sid=request.sid)
    except Exception:
        pass


@socketio.on("prateleira_listar")
def prateleira_listar(dados):
    try:
        emit("prateleira_lista", {"ok": True, "livros": hub_store.listar_livros()})
    except Exception as e:
        emit("prateleira_lista", {"ok": False, "erro": str(e)})


@socketio.on("livro_puxar")
def livro_puxar(dados):
    try:
        d = dados or {}
        bookId = str(d.get("bookId") or "").strip()
        if not bookId:
            emit("livro_resposta", {"ok": False, "erro": "bookId_obrigatorio"})
            return
        rec = hub_store.obter_livro(bookId)
        if not rec:
            emit("livro_resposta", {"ok": True, "vazio": True, "bookId": bookId, "livro": None})
            return
        emit("livro_resposta", {"ok": True, "vazio": False, "livro": rec})
    except Exception as e:
        emit("livro_resposta", {"ok": False, "erro": str(e)})


@socketio.on("livro_atualizar")
def livro_atualizar(dados):
    try:
        d = dados or {}
        bookId = str(d.get("bookId") or "").strip()
        if not bookId:
            emit("livro_resposta", {"ok": False, "erro": "bookId_obrigatorio"})
            return
        rec = hub_store.atualizar_livro(bookId, d)
        if not rec:
            emit("livro_resposta", {"ok": False, "erro": "bookId_invalido"})
            return
        emit("livro_resposta", {"ok": True, "livro": rec})
        _skip_self("livro_estado", {"ok": True, "livro": rec})
    except Exception as e:
        emit("livro_resposta", {"ok": False, "erro": str(e)})


@socketio.on("livro_apagar")
def livro_apagar(dados):
    try:
        d = dados or {}
        bookId = str(d.get("bookId") or "").strip()
        if not bookId:
            emit("livro_resposta", {"ok": False, "erro": "bookId_obrigatorio"})
            return
        rec = hub_store.apagar_livro(bookId, device=str(d.get("deviceId") or ""),
                                     apagar_arquivo=bool(d.get("apagarArquivo")))
        if not rec:
            emit("livro_resposta", {"ok": False, "erro": "nao_encontrado"})
            return
        emit("livro_resposta", {"ok": True, "livro": rec})
        _skip_self("livro_estado", {"ok": True, "livro": rec})
    except Exception as e:
        emit("livro_resposta", {"ok": False, "erro": str(e)})


@socketio.on("livro_publicar")
def livro_publicar(dados):
    try:
        d = dados or {}
        bookId = str(d.get("bookId") or "").strip()
        if not bookId:
            emit("livro_resposta", {"ok": False, "erro": "bookId_obrigatorio"})
            return
        patch = {k: d.get(k) for k in ("titulo", "ext", "tamanho", "deviceId") if d.get(k) is not None}
        patch["arquivo"] = d.get("arquivo") or {}
        patch["removido"] = False
        patch["removidoEm"] = int(time.time() * 1000)
        rec = hub_store.atualizar_livro(bookId, patch)
        if not rec:
            emit("livro_resposta", {"ok": False, "erro": "banco"})
            return
        emit("livro_resposta", {"ok": True, "livro": rec})
        _skip_self("livro_estado", {"ok": True, "livro": rec})
    except Exception as e:
        emit("livro_resposta", {"ok": False, "erro": str(e)})


@socketio.on("livros_puxar")
def livros_puxar(dados):
    try:
        d = dados or {}
        deviceId = str(d.get("deviceId") or "").strip()
        if not deviceId:
            emit("livros_resposta", {"ok": False, "erro": "deviceId_obrigatorio"})
            return
        # Return books from ALL other devices (merged, deduplicated by bookId)
        livros = hub_store.obter_todos_livros(excluir_deviceId=deviceId)
        emit("livros_resposta", {"ok": True, "deviceId": deviceId, "livros": livros})
    except Exception as e:
        emit("livros_resposta", {"ok": False, "erro": str(e)})

@socketio.on("livros_atualizar")
def livros_atualizar(dados):
    try:
        d = dados or {}
        deviceId = str(d.get("deviceId") or "").strip()
        livros = d.get("livros", [])
        if not deviceId:
            emit("livros_resposta", {"ok": False, "erro": "deviceId_obrigatorio"})
            return
        if hub_store.salvar_livros(deviceId, livros):
            payload = {"ok": True, "deviceId": deviceId, "livros": livros}
            emit("livros_resposta", payload)
            # Broadcast to other devices
            try:
                socketio.emit("livros_estado", payload)
            except Exception:
                pass
        else:
            emit("livros_resposta", {"ok": False, "erro": "falha"})
    except Exception as e:
        emit("livros_resposta", {"ok": False, "erro": str(e)})

# ============================ LIVROS (arquivo content-addressed) ============================
_MIME_LIVROS = {"pdf": "application/pdf", "epub": "application/epub+zip"}


def _auth_ok():
    auth = request.headers.get("Authorization", "")
    return auth.replace("Bearer ", "").strip() == TOKEN_SESSAO


@app.route("/livros", methods=["GET"])
def rota_listar_livros():
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    return {"ok": True, "livros": hub_store.listar_livros()}


@app.route("/livros", methods=["POST"])
def rota_publicar_livro():
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    if "file" not in request.files:
        return {"ok": False, "erro": "No file"}, 400
    f = request.files["file"]
    nome = os.path.basename(f.filename or "livro.pdf")
    ext = nome.rsplit(".", 1)[-1].lower() if "." in nome else ""
    if ext not in ("pdf", "epub"):
        return {"ok": False, "erro": "formato_invalido"}, 400
    import tempfile
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix="." + ext)
    tmp_path = tmp.name
    tmp.close()
    try:
        f.save(tmp_path)
        rec = hub_store.publicar_livro(
            tmp_path,
            book_id=request.form.get("bookId"),
            titulo=request.form.get("titulo") or nome.rsplit(".", 1)[0],
            ext=ext,
            device=request.form.get("deviceId") or request.form.get("device") or "desconhecido",
        )
        if not rec:
            return {"ok": False, "erro": "falha_publicar"}, 500
        try:
            socketio.emit("livro_estado", {"ok": True, "livro": rec})
            socketio.emit("prateleira_lista", {"ok": True, "livros": hub_store.listar_livros()})
        except Exception:
            pass
        return {"ok": True, "bookId": rec["bookId"], "livro": rec}
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


@app.route("/livros/<bookId>", methods=["GET"])
def rota_obter_livro(bookId):
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    rec = hub_store.obter_livro(bookId)
    if not rec:
        return {"ok": True, "vazio": True, "bookId": bookId, "livro": None}
    return {"ok": True, "vazio": False, "livro": rec}


@app.route("/livros/<bookId>", methods=["POST"])
def rota_atualizar_livro(bookId):
    """Atualiza posicao/done pelo HTTP (o app usa isso quando o socket esta
    morto - MIUI derruba a conexao em background). Mesma semantica LWW do
    evento livro_atualizar."""
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    d = request.get_json(silent=True) or {}
    d.pop("bookId", None)
    rec = hub_store.atualizar_livro(bookId, d)
    if not rec:
        return {"ok": False, "erro": "bookId_invalido"}, 400
    try:
        socketio.emit("livro_estado", {"ok": True, "livro": rec})
    except Exception:
        pass
    return {"ok": True, "livro": rec}


@app.route("/livros/<bookId>/arquivo")
def rota_arquivo_livro(bookId):
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    rec = hub_store.obter_livro(bookId)
    if not rec:
        return {"ok": False, "erro": "nao_encontrado"}, 404
    caminho = hub_store.baixar_arquivo_livro(rec["bookId"])
    if not caminho:
        return {"ok": False, "erro": "arquivo_indisponivel"}, 404
    mime = _MIME_LIVROS.get((rec.get("ext") or "").lower(), "application/octet-stream")
    return send_file(str(caminho), mimetype=mime, as_attachment=False,
                     download_name=(rec.get("arquivo") or {}).get("nome") or caminho.name,
                     conditional=True)


@app.route("/livros/<bookId>", methods=["DELETE"])
def rota_apagar_livro(bookId):
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    rec = hub_store.apagar_livro(bookId, device=request.args.get("device") or "http",
                                 apagar_arquivo=request.args.get("arquivo") == "1")
    if not rec:
        return {"ok": False, "erro": "nao_encontrado"}, 404
    try:
        socketio.emit("livro_estado", {"ok": True, "livro": rec})
    except Exception:
        pass
    return {"ok": True, "livro": rec}


# ============================ RELATORIOS HTTP ============================
@app.route("/relatorios")
def rota_listar_relatorios():
    rels = hub_store.listar_relatorios()
    return {"relatorios": rels}

@app.route("/relatorios/<path:filename>")
def rota_baixar_relatorio(filename):
    # filename vem sem extensão (base)
    data = hub_store.baixar_relatorio(filename)
    if data is None:
        return {"ok": False, "erro": "nao_encontrado"}, 404
    # detecta content-type pelo conteudo (o mirror tem o .txt; o Drive o .docx)
    ctype = "text/plain; charset=utf-8"
    if data[:2] == b"PK":
        ctype = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    return data, 200, {"Content-Type": ctype, "Content-Disposition": f'inline; filename="{filename}.txt"'}

@app.route("/relatorios", methods=["DELETE"])
def rota_apagar_relatorios():
    import json as _json
    try:
        data = request.get_json()
        if not data or "nomes" not in data:
            return {"ok": False, "erro": "nomes_obrigatorio"}, 400
        ok = hub_store.apagar_relatorios(data["nomes"])
        return {"ok": ok}
    except Exception as e:
        return {"ok": False, "erro": str(e)}, 500

# ================================ PAD (HTTP) ================================
@app.route("/pad", methods=["GET"])
def rota_listar_pad():
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    return {"ok": True, "notas": pad_store.listar()}


@app.route("/pad/<nid>", methods=["GET"])
def rota_obter_pad(nid):
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    rec = pad_store.obter(nid)
    if not rec:
        return {"ok": False, "erro": "nao_encontrada"}, 404
    return {"ok": True, "nota": rec}


@app.route("/pad", methods=["POST"])
def rota_criar_pad():
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    d = request.get_json(silent=True) or {}
    rec = pad_store.criar(
        d.get("titulo", ""), d.get("texto", ""), d.get("device", "android")
    )
    return {"ok": True, "nota": rec}


@app.route("/pad/<nid>", methods=["POST"])
def rota_salvar_pad(nid):
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    d = request.get_json(silent=True) or {}
    res = pad_store.salvar(
        nid,
        d.get("titulo", ""),
        d.get("texto", ""),
        int(d.get("em") or 0),
        d.get("base_hash", ""),
        d.get("device", "android"),
    )
    if "nota" not in res:
        return {"ok": False, "erro": "falha"}, 500
    res["ok"] = True
    return res


@app.route("/pad/<nid>", methods=["DELETE"])
def rota_apagar_pad(nid):
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    rec = pad_store.apagar(nid, request.args.get("device", "android"))
    return {"ok": True, "nota": rec}


@app.route("/pad/<nid>/historico", methods=["GET"])
def rota_historico_pad(nid):
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    return {"ok": True, "versoes": pad_store.historico(nid)}


@app.route("/pad/<nid>/restaurar", methods=["POST"])
def rota_restaurar_pad(nid):
    if not _auth_ok():
        return {"ok": False, "erro": "Unauthorized"}, 401
    d = request.get_json(silent=True) or {}
    res = pad_store.restaurar(nid, int(d.get("em") or 0), d.get("device", "android"))
    return res


@app.route("/healthz")
def health():
    return "OK"

@app.route("/api/connected")
def api_connected():
    import time
    now = time.time()
    result = {}
    for sid, info in CONNECTED_CLIENTS.items():
        result[sid] = {
            "device": info.get("device", "unknown"),
            "connected_at": info.get("connected_at", now),
            "duration_sec": int(now - info.get("connected_at", now))
        }
    return {"clients": result, "count": len(result)}


if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, allow_unsafe_werkzeug=True)