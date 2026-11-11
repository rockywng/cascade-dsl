# Runs Python pipelines (and notebook Python cells) to produce their document, in the tenant's own
# namespace on the data plane: `python -m cascade_dsl.evaluate /work`. The control plane never runs
# user Python; it compiles the document this prints. Build from the repository root:
#   docker build -f images/builder/Dockerfile -t ghcr.io/cascade/cascade-builder:0.1.0 .
FROM python:3.12-slim
RUN useradd --uid 10001 --no-create-home builder
WORKDIR /opt/cascade-dsl
COPY pyproject.toml README.md ./
COPY cascade_dsl ./cascade_dsl
RUN pip install --no-cache-dir . && rm -rf /root/.cache
USER 10001
WORKDIR /work
ENTRYPOINT ["python", "-m", "cascade_dsl.evaluate"]
CMD ["/work"]
