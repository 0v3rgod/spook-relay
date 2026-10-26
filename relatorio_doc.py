"""
Gera o .docx dos relatorios de scan: abre e EDITA no Drive (Google Docs) com a
fonte monoespacada e as cores semanticas da janela de reports do app.

O .typ e montado na mao (sem pandoc) porque so assim da pra reproduzir o
terminal: fundo quase preto, numero de linha em verde apagado e cor por
linha (prompt verde, cabecalho de tabela amarelo, porta fechada azul, coluna
VERSION laranja, erro vermelho, aviso ambar, separadores verde escuro).
Fonte: a propria mono.ttf do app (JetBrains Mono NFM), via --font-path.

O .docx e escrito na mao (OOXML dentro de um zip da stdlib) porque assim da
para por cor em CADA trecho da linha — o pandoc so aceitaria cor por estilo.

O .txt continua sendo escrito em disco porque e dele que a tela do app monta
o cabecalho e mostra a saida — mas ele NAO sobe para o Drive.

A geracao de .pdf (typst, fundo escuro igual a janela) continua no codigo em
_typst()/_pdf(), so nao e mais usada: no fundo escuro a ordem do layout nao
ficou igual a do app e o Taylor preferiu um formato que da para editar.
"""

from __future__ import annotations

import os
import re
import shutil
import socket
import subprocess
import tempfile
import time
import zipfile

TYPST = shutil.which("typst") or "/usr/bin/typst"
# a fonte monoespacada do app (typst so acha com --font-path, nao usa fontconfig)
FONTE_DIR = os.path.expanduser("~/.local/share/fonts/spook")
COLUNAS = 112  # caracteres por linha antes de quebrar (A4 com margem 11mm)
# a fonte do app aparece com nomes diferentes conforme a variante do nerd font
FAMILIA = '("JetBrainsMono NF", "JetBrainsMono NFM", "JetBrains Mono", "DejaVu Sans Mono")'

# paleta = Terminal.Fundo/Titulo/Aberto/Fechado/Versao/Erro/Aviso/Texto/Apagado
C = {
    "fundo": "#05070A",
    "cartao": "#0B1016",
    "borda": "#1E2A35",
    "barra": "#10161D",
    "prompt": "#00C853",
    "titulo": "#FFD400",
    "aberto": "#00C853",
    "fechado": "#1F6FB2",
    "versao": "#FF8A3D",
    "erro": "#FF3B3B",
    "aviso": "#FFC400",
    "texto": "#E8F1F5",
    "apagado": "#12664A",
    "numero": "#11483A",
}

# mesmas regras de estilizarLinha() do app
RE_PORTA = re.compile(r"^(\S+)/(tcp|udp)\s+(\S+)")
RE_ERRO = re.compile(
    r"\b(error|failed|failure|refused|denied|timeout|timed out|unreachable|not found|invalid)\b",
    re.I,
)
RE_TABELA = re.compile(r"^\s*(PORT|STATE|SERVICE|VERSION|NAME|TYPE|DOMAIN|CODE)\b")
RE_AVISO = re.compile(r"\b(warning|warn|deprecated|not shown|filtered)\b", re.I)
RE_RESUMO = re.compile(r"^Nmap (done|scan report)|scanned in|\d+ hosts? up", re.I)
RE_SECAO = re.compile(r"^\s*[=\-_*]{3,}\s*$")
RE_RUIDO = re.compile(r"^\s*(Starting Nmap|Nmap done|Service detection|WARNING:|Warning:)")


def _slug(txt: str, limite: int = 60) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", (txt or "").strip()).strip("_")[:limite] or "alvo"


def _campo(linhas, rotulo: str) -> str:
    """Le 'ROTULO : valor' do banner gerado pelo agente/IA."""
    for l in linhas:
        t = l.strip()
        if t.startswith(rotulo):
            return t.split(":", 1)[1].strip() if ":" in t else ""
    return ""


def _analisa(texto: str) -> dict:
    """Separa o banner de metadados do corpo de saida do scan."""
    linhas = (texto or "").splitlines()
    sep = re.compile(r"^[=\-_*]{3,}$")
    ini, meta = 0, {}
    while ini < len(linhas):
        t = linhas[ini].strip()
        m = re.match(r"^([A-Z]{2,12})\s*:\s*(.+)$", t)
        if m:
            meta[m.group(1)] = m.group(2).strip()
            ini += 1
            continue
        if not t or sep.match(t) or "SPOOK" in t:
            ini += 1
            continue
        if t.startswith("$"):
            meta.setdefault("COMMAND", t[1:].strip())
            ini += 1
            continue
        break
    fim = len(linhas)
    while fim > ini:
        t = linhas[fim - 1].strip()
        if not t or sep.match(t) or "SPOOK" in t or "END OF REPORT" in t:
            fim -= 1
            continue
        break
    return {"meta": meta, "corpo": "\n".join(linhas[ini:fim]).strip("\n")}


def _estiliza(linha: str):
    """Porta de estilizarLinha(): devolve [(cor, trecho), ...] da linha."""
    t = linha.strip()
    if t.startswith("$"):
        return [("prompt", linha)]
    if RE_SECAO.match(linha):
        return [("apagado", linha)]
    if RE_TABELA.search(linha):
        return [("titulo", linha)]
    if RE_ERRO.search(linha):
        return [("erro", linha)]
    if "Host is up" in linha:
        return [("aberto", linha)]
    m = RE_PORTA.match(linha)
    if m:
        estado = m.group(3)
        cor = "aberto" if estado.startswith("open") else (
            "fechado" if estado.startswith("closed") else (
                "aviso" if estado.startswith("filtered") else "texto"))
        # a coluna VERSION (4o bloco separados por 2+ espacos) fica laranja
        blocos = re.split(r"\s{2,}", linha)
        if len(blocos) >= 4 and " " in blocos[3]:
            i = linha.index(blocos[3])
            return [(cor, linha[:i]), ("versao", linha[i:])]
        return [(cor, linha)]
    if RE_RESUMO.search(linha):
        return [("aberto", linha)]
    if RE_AVISO.search(linha):
        return [("aviso", linha)]
    if not t:
        return [("texto", "")]
    if RE_RUIDO.match(linha):
        return [("apagado", linha)]
    return [("texto", linha)]


def _raw(s: str) -> str:
    """Texto verbatim para o typst (escapa as duas coisas que quebram string)."""
    return 'raw("%s", block: false)' % (s or "").replace("\\", "\\\\").replace('"', '\\"')


def _linhas_typst(corpo: str) -> str:
    linhas = (corpo or "").splitlines() or [""]
    grade = []
    for i, ln in enumerate(linhas):
        pedacos = [ln[j:j + COLUNAS] for j in range(0, len(ln), COLUNAS)] or [""]
        for k, ped in enumerate(pedacos):
            num = "%4d" % (i + 1) if k == 0 else "    "
            celula = []
            for cor, txt in _estiliza(ped if k == 0 else " " + ped):
                celula.append("#L(%s, %s)" % (cor.upper(), _raw(txt)))
            grade.append("  L(NUMERO, %s), [%s]," % (_raw(num), " ".join(celula)))
    return "#grid(columns: (3.4em, 1fr), row-gutter: 0.1em, align: left + top,\n" \
        + "\n".join(grade) + "\n)"


# ---------------------------------------------------------------- .docx ----
# OOXML escrito a mao (zipfile da stdlib): o pandoc so aceitaria cor por estilo,
# e aqui cada trecho da linha (estado da porta, coluna VERSION, erro) recebe a
# sua cor, como na janela do app. Fundo CLARO (documento normal e editavel);
# as cores sao as mesmas familias da paleta do terminal, escurecidas para
# ficarem legiveis no branco.
D = {
    "titulo": "A67900",   # ambar do terminal, escurecido
    "aberto": "0B8A3C",   # verde de open / "$ comando"
    "fechado": "1F6FB2",  # azul de closed
    "versao": "C25A0A",   # laranja da coluna VERSION
    "erro": "D32F2F",     # vermelho de error/refused/timeout
    "aviso": "B07400",    # ambar de warning/filtered
    "texto": "1A1F24",    # corpo
    "apagado": "12664A",  # verde escuro de separadores/linha de ruido
    "numero": "8A8F98",   # numero da linha
}
FONTE_DOCX = "JetBrains Mono"   # o Drive/Google Docs tambem tem essa fonte


def _xml(txt) -> str:
    return (str(txt).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _run(txt: str, cor: str, negrito=False, meio=17) -> str:
    return ('<w:r><w:rPr><w:rFonts w:ascii="%s" w:hAnsi="%s" w:cs="%s"/>%s'
            '<w:color w:val="%s"/><w:sz w:val="%d"/><w:szCs w:val="%d"/></w:rPr>'
            '<w:t xml:space="preserve">%s</w:t></w:r>'
            % (FONTE_DOCX, FONTE_DOCX, FONTE_DOCX, "<w:b/>" if negrito else "",
               cor, meio, meio, _xml(txt)))


def _par(*runs, fundo=None) -> str:
    sombra = ('<w:pPr><w:shd w:val="clear" w:color="auto" w:fill="%s"/>'
              '<w:spacing w:before="0" w:after="0" w:line="200" w:lineRule="exact"/></w:pPr>'
              % fundo) if fundo else ""
    return "<w:p>%s%s</w:p>" % (sombra, "".join(runs))


def _pacote_pandoc() -> dict | None:
    """Pacote .docx gerado pelo pandoc (docProps, settings, fontTable, theme,
    styles com default). O Drive so que aceita o que parece com isso; escrever
    as 5 partes a mao passava no pandoc mas o Google Docs recusava abrir."""
    pandoc = shutil.which("pandoc") or "/usr/bin/pandoc"
    if not os.path.exists(pandoc):
        return None
    try:
        with tempfile.TemporaryDirectory() as td:
            md = os.path.join(td, "b.md")
            out = os.path.join(td, "b.docx")
            with open(md, "w", encoding="utf-8") as f:
                f.write("spook\n")
            r = subprocess.run(
                [pandoc, "-f", "markdown", "-t", "docx", "-o", out, md],
                capture_output=True, timeout=120,
            )
            if r.returncode != 0 or not os.path.exists(out):
                return None
            with zipfile.ZipFile(out) as z:
                return {n: z.read(n) for n in z.namelist()}
    except Exception:
        return None


def _aplica_fonte(estilos: bytes) -> bytes:
    """Joga a fonte do app como default do documento."""
    txt = estilos.decode("utf-8")
    padrao = (
        "<w:rPrDefault><w:rPr>"
        '<w:rFonts w:ascii="%s" w:hAnsi="%s" w:cs="%s"/>'
        '<w:sz w:val="17"/><w:szCs w:val="17"/>'
        "</w:rPr></w:rPrDefault>" % (FONTE_DOCX, FONTE_DOCX, FONTE_DOCX)
    )
    if "<w:rPrDefault>" in txt:
        txt = re.sub(r"<w:rPrDefault>.*?</w:rPrDefault>", padrao, txt, count=1, flags=re.S)
    elif "<w:docDefaults>" in txt:
        txt = txt.replace("<w:docDefaults>", "<w:docDefaults>" + padrao, 1)
    return txt.encode("utf-8")


def _docx(dados: dict, destino: str) -> str:
    meta = dados["meta"]
    perfil = meta.get("PROFILE") or "SCAN"
    alvo = meta.get("TARGET", "")
    quando = meta.get("DATE") or time.strftime("%Y-%m-%d %H:%M:%S %z")
    codigo = meta.get("EXIT") or meta.get("CODE") or ""
    duracao = meta.get("DURATION") or meta.get("DURACAO") or ""
    titulo = "SPOOK // SCAN REPORT" if perfil.upper() in ("", "SCAN") else perfil.upper()

    # cabecalho na MESMA ordem da janela do app: titulo, alvo, meta
    corpo = [
        _par(_run(titulo, D["titulo"], negrito=True, meio=26)),
        _par(_run("alvo  %s" % (alvo or "-"), D["aberto"], meio=20)),
    ]
    esquerda = quando + ("   duracao %ss" % duracao if duracao else "")
    # exit != 0 fica vermelho, como o ponto do cabecalho na janela do app
    exit_txt = "   exit %s" % (codigo if codigo else "0")
    corpo.append(_par(_run(esquerda, D["apagado"], meio=18),
                      _run(exit_txt, D["erro"] if codigo and codigo != "0" else D["aberto"],
                           meio=18)))
    corpo.append(_par(_run("spook://scan-output", D["apagado"], meio=16), fundo="F4F5F6"))

    for i, ln in enumerate((dados["corpo"] or "").splitlines()):
        partes = [_run("%4d " % (i + 1), D["numero"], meio=16)]
        for cor, txt in _estiliza(ln):
            partes.append(_run(txt, D.get(cor, D["texto"])))
        corpo.append(_par(*partes))

    document = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        "<w:body>%s"
        '<w:sectPr><w:pgSz w:w="11906" w:h="16838"/>'
        '<w:pgMar w:top="1134" w:right="1134" w:bottom="1134" w:left="1134"'
        ' w:header="708" w:footer="708" w:gutter="0"/></w:sectPr>'
        "</w:body></w:document>" % "".join(corpo)
    )
    base = _pacote_pandoc()
    if base and "word/styles.xml" in base:
        base["word/styles.xml"] = _aplica_fonte(base["word/styles.xml"])
        base["word/document.xml"] = document.encode("utf-8")
        with zipfile.ZipFile(destino, "w", zipfile.ZIP_DEFLATED) as z:
            for nome, dados_parte in base.items():
                z.writestr(nome, dados_parte)
        return destino

    # sem pandoc: cai no pacote minimo (o Drive pode recusar, mas o arquivo
    # ainda e um .docx valido e editavel no LibreOffice/Word)
    estilos = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        "<w:docDefaults><w:rPrDefault><w:rPr>"
        '<w:rFonts w:ascii="%s" w:hAnsi="%s" w:cs="%s"/>'
        '<w:sz w:val="17"/><w:szCs w:val="17"/></w:rPr></w:rPrDefault>'
        '<w:pPrDefault><w:pPr><w:spacing w:before="0" w:after="0" w:line="200"'
        ' w:lineRule="exact"/></w:pPr></w:pPrDefault></w:docDefaults>'
        '<w:style w:type="paragraph" w:default="1" w:styleId="Normal">'
        '<w:name w:val="Normal"/><w:qFormat/></w:style></w:styles>'
        % (FONTE_DOCX, FONTE_DOCX, FONTE_DOCX)
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2003/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
        '<Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>'
        "</Types>"
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2003/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>'
        "</Relationships>"
    )
    rels_doc = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2003/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
        "</Relationships>"
    )
    with zipfile.ZipFile(destino, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types)
        z.writestr("_rels/.rels", rels)
        z.writestr("word/_rels/document.xml.rels", rels_doc)
        z.writestr("word/styles.xml", estilos)
        z.writestr("word/document.xml", document)
    return destino


def _typst(dados: dict) -> str:
    meta = dados["meta"]
    perfil = meta.get("PROFILE") or "SCAN"
    alvo = meta.get("TARGET", "")
    quando = meta.get("DATE") or time.strftime("%Y-%m-%d %H:%M:%S %z")
    host = meta.get("HOST") or socket.gethostname()
    origem = meta.get("SOURCE") or meta.get("ID") or "SPOOK"
    comando = meta.get("COMMAND", "")
    codigo = meta.get("EXIT") or meta.get("CODE") or ""
    duracao = meta.get("DURATION") or meta.get("DURACAO") or ""

    titulo = "SPOOK // SCAN REPORT"
    if perfil and perfil.upper() != "SCAN":
        titulo = "%s" % perfil.upper()
    linhas_meta = [
        ("TARGET", alvo or "-"),
        ("HOST", host),
        ("DATE", quando),
        ("ORIGIN", origem),
    ]
    if codigo or duracao:
        linhas_meta.append(("STATUS", ("duracao %s  " % duracao if duracao else "")
                            + ("exit %s" % codigo if codigo else "exit 0 (ok)")))
    grade_meta = []
    for rot, val in linhas_meta:
        grade_meta.append("    [ #L(TITULO, %s) ], [#L(ABERTO, %s) ],"
                          % (_raw(rot), _raw(val)))
    if comando:
        grade_meta.append("    [ #L(TITULO, %s) ], [#L(PROMPT, %s) ]," % (_raw("COMMAND"), _raw(comando)))

    cab = (
        '#block(fill: rgb("%s"), stroke: 0.5pt + rgb("%s"), radius: 3pt, inset: 8pt, width: 100%%)[\n'
        '  #grid(columns: (auto, 1fr), column-gutter: 6pt, row-gutter: 1.5pt,\n'
        '    [#rect(width: 5pt, height: 5pt, fill: rgb("%s"), radius: 2.5pt)\n'
        '     #h(4pt) #text(weight: "bold")[#L(TITULO, %s)]],\n'
        '    [%s]\n'
        '  )\n'
        ']\n'
    ) % (C["cartao"], C["borda"],
         C["aberto"] if not codigo or codigo == "0" else C["erro"],
         _raw(titulo),
         "\n".join(grade_meta))

    barra = (
        '#block(fill: rgb("%s"), radius: 3pt, inset: (x: 8pt, y: 5pt), width: 100%%)[\n'
        '  #rect(width: 6pt, height: 6pt, fill: rgb("%s"), radius: 3pt) #h(4pt)\n'
        '  #rect(width: 6pt, height: 6pt, fill: rgb("%s"), radius: 3pt) #h(4pt)\n'
        '  #rect(width: 6pt, height: 6pt, fill: rgb("%s"), radius: 3pt) #h(7pt)\n'
        '  #L(APAGADO, "  spook://scan-output")\n'
        ']\n'
    ) % (C["barra"], C["erro"], C["aviso"], C["aberto"])

    defs = "\n".join('#let %s = rgb("%s")' % (k.upper(), v) for k, v in C.items())
    return (
        '#set page(width: 210mm, height: 297mm, margin: 11mm, fill: rgb("%s"))\n'
        '#set text(font: %s, size: 7.6pt, fill: rgb("%s"))\n'
        # o raw() do typst ignora o font do #set text: precisa do show
        "#show raw: set text(font: %s)\n"
        "#set par(leading: 0.34em)\n"
        "#let L(cor, s) = text(fill: cor)[#s]\n"
        "%s\n"
        "%s\n"
        "#v(6pt)\n"
        "%s\n"
        "#v(3pt)\n"
        "%s\n"
    ) % (C["fundo"], FAMILIA, C["texto"], FAMILIA, defs, cab, barra,
         _linhas_typst(dados["corpo"]))


def gerar_documentos(
    destino_dir: str,
    nome_base: str,
    texto: str,
    manter_txt: bool = True,
) -> dict:
    """
    Escreve em destino_dir:
      <base>.docx  (editavel no Drive, fonte mono e cores por trecho)
      <base>.txt   (texto puro, fonte local da tela do app — NAO vai pro Drive)
    Devolve {"docx":..., "txt":..., "erros": [...]}
    """
    os.makedirs(destino_dir, exist_ok=True)
    base = _slug(nome_base, 80)
    txt_path = os.path.join(destino_dir, base + ".txt")
    docx_path = os.path.join(destino_dir, base + ".docx")

    if manter_txt:
        with open(txt_path, "w", encoding="utf-8") as arq:
            arq.write(texto or "")

    erros = []
    try:
        _docx(_analisa(texto), docx_path)
        os.chmod(docx_path, 0o600)
    except Exception as e:  # pragma: no cover
        erros.append("docx: %s" % str(e)[:200])
        docx_path = None

    return {"docx": docx_path,
            "txt": txt_path if manter_txt else None, "erros": erros}


if __name__ == "__main__":
    import sys

    origem = sys.argv[1]
    saida = sys.argv[2] if len(sys.argv) > 2 else "/tmp"
    with open(origem, encoding="utf-8", errors="replace") as arq:
        r = gerar_documentos(saida, os.path.basename(origem).rsplit(".", 1)[0], arq.read())
    print(r)
