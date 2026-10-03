"""Repository checks only; this is not an Apex compiler or a Spark integration test."""
from pathlib import Path
from urllib.parse import unquote, urlsplit
from html.parser import HTMLParser
import ast
import json
import re
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
errors = []
counts = {"python": 0, "xml": 0, "json": 0, "local_links": 0}

def local_link(source, target):
    if target.startswith(("http:","https:","mailto:","#","data:","javascript:")):
        return
    target = unquote(urlsplit(target).path)
    if not target:
        return
    path = (source.parent / target).resolve()
    counts["local_links"] += 1
    if not path.is_relative_to(ROOT):
        errors.append(f"Link escapes repository: {source.relative_to(ROOT)} -> {target}")
    elif not path.exists():
        errors.append(f"Missing local link: {source.relative_to(ROOT)} -> {target}")

class Links(HTMLParser):
    def __init__(self, source):
        super().__init__()
        self.source = source
    def handle_starttag(self, tag, attrs):
        for key, value in attrs:
            if key in ("href", "src") and value:
                local_link(self.source, value)

for source in sorted(ROOT.rglob("*")):
    if not source.is_file() or any(p in (".git","__pycache__",".sf",".sfdx","node_modules",".venv") for p in source.parts):
        continue
    try:
        if source.suffix == ".py":
            ast.parse(source.read_text(encoding="utf-8"),filename=str(source))
            counts["python"] += 1
        elif source.suffix == ".xml":
            ET.parse(source)
            counts["xml"] += 1
        elif source.suffix == ".json":
            if source.parent == ROOT / "data" / "stream_input":
                for line in source.read_text(encoding="utf-8").splitlines():
                    if line.strip():
                        json.loads(line)
            else:
                json.loads(source.read_text(encoding="utf-8"))
            counts["json"] += 1
        elif source.suffix == ".html":
            Links(source).feed(source.read_text(encoding="utf-8"))
        elif source.suffix == ".md":
            fence = chr(96) * 3
            text = re.sub(r"(?:" + fence + r"|~{3})[\s\S]*?(?:" + fence + r"|~{3})", "",source.read_text(encoding="utf-8"))
            for target in re.findall(r"!?\[[^\]]*\]\(([^\s)]+)\)",text):
                local_link(source,target)
    except (SyntaxError,ET.ParseError,json.JSONDecodeError) as error:
        errors.append(f"{source.relative_to(ROOT)}: {error}")

if (ROOT / "force-app").exists():
    meta = ROOT / "force-app/main/default"
    namespace = {"m":"http://soap.sforce.com/2006/04/metadata"}
    declared_objects = {p.name for p in (meta/"objects").iterdir() if p.is_dir()}
    declared_fields = {p.name.split(".")[0] for p in (meta/"objects").rglob("*.field-meta.xml")}
    for source in (meta/"classes").glob("*.cls"):
        if not Path(str(source)+"-meta.xml").exists():
            errors.append(f"Missing class metadata: {source.name}")
        for field in re.findall(r"\b([A-Za-z_]+__c)\b",source.read_text(encoding="utf-8")):
            if field not in declared_objects and field not in declared_fields:
                errors.append(f"Undeclared field in {source.name}: {field}")
    for source in (meta/"permissionsets").glob("*.xml"):
        tree = ET.parse(source)
        for entry in tree.findall("m:classAccesses/m:apexClass",namespace):
            if not (meta/"classes"/(entry.text+".cls")).exists():
                errors.append(f"Missing permission-set class: {entry.text}")

print(json.dumps({"checked":counts,"errors":errors},indent=2))
raise SystemExit(1 if errors else 0)
