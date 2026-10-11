# ClimaFlu-GNN modelling container: PyTorch Geometric + analysis stack.
# Packages are baked into the image so they survive container re-creation.
FROM python:3.11-slim

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --index-url https://download.pytorch.org/whl/cpu torch==2.4.1 \
 && pip install \
      torch_geometric==2.6.1 \
      numpy==1.26.4 \
      pandas==2.2.3 \
      pyarrow==17.0.0 \
      scipy==1.14.1 \
      scikit-learn==1.5.2 \
      matplotlib==3.9.2

WORKDIR /workspace
CMD ["tail", "-f", "/dev/null"]
