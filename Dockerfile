FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /usr/app

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen

COPY . . 

ENV PATH="/usr/app/.venv/bin:$PATH"

CMD ["dbt", "build"]

