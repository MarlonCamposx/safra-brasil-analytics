"""
claude_review.py
----------------
Lê os arquivos alterados no PR, seleciona o agente correto por tipo de arquivo,
chama a API do Claude e posta o resultado como comentário no Pull Request.

Usado pelo workflow claude-review.yml.
"""

import os
import subprocess
from pathlib import Path

import anthropic

AGENTS_DIR = Path(".github/agents")


def load_prompt(filename: str) -> str:
    path = AGENTS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Agent prompt not found: {path}")
    return path.read_text()


def get_changed_files() -> list[str]:
    with open("changed_files.txt") as f:
        return [line.strip() for line in f if line.strip()]


def read_file_content(filepath: str) -> str:
    try:
        with open(filepath) as f:
            return f.read()
    except Exception:
        return ""


def post_pr_comment(body: str) -> None:
    repo = os.environ["GITHUB_REPOSITORY"]
    pr_number = os.environ["PR_NUMBER"]
    subprocess.run(
        ["gh", "pr", "comment", pr_number, "--body", body, "--repo", repo],
        check=True,
        env={**os.environ},
    )


def review_files(
    client: anthropic.Anthropic,
    files: list[str],
    system_prompt: str,
    label: str,
) -> str:
    if not files:
        return ""

    file_contents = []
    for filepath in files:
        content = read_file_content(filepath)
        if content:
            file_contents.append(f"### {filepath}\n```\n{content[:3000]}\n```")

    if not file_contents:
        return ""

    prompt = "Revise os seguintes arquivos:\n\n" + "\n\n".join(file_contents)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=system_prompt,
        messages=[{"role": "user", "content": prompt}],
    )

    return f"### {label}\n\n{message.content[0].text}"


def main() -> None:
    changed_files = get_changed_files()
    if not changed_files:
        print("Nenhum arquivo Python ou SQL alterado.")
        return

    py_files = [f for f in changed_files if f.endswith(".py")]
    sql_files = [f for f in changed_files if f.endswith(".sql")]
    all_files = py_files + sql_files

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    sections = []

    if py_files:
        result = review_files(
            client, py_files, load_prompt("python_reviewer.md"), "Python — Docstrings e Qualidade"
        )
        if result:
            sections.append(result)

    if sql_files:
        result = review_files(
            client, sql_files, load_prompt("sql_reviewer.md"), "SQL — Queries e Convenções"
        )
        if result:
            sections.append(result)

    if all_files:
        result = review_files(
            client, all_files, load_prompt("naming_validator.md"), "Nomenclatura — Validação Geral"
        )
        if result:
            sections.append(result)

    if not sections:
        return

    comment_body = (
        "## Revisão automática — Claude\n\n"
        + "\n\n---\n\n".join(sections)
        + "\n\n---\n"
        + "*Revisão gerada automaticamente. Verifique também os checks de lint acima.*"
    )

    post_pr_comment(comment_body)
    print("Comentário postado no PR.")


if __name__ == "__main__":
    main()
