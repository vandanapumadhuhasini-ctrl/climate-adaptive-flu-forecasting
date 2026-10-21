# ClimaFlu dashboard: Streamlit app that reads ONLY result files (results/, docs/) mounted read-only.
FROM python:3.11-slim
ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN pip install streamlit==1.39.0 plotly==5.24.1 pandas==2.2.3 pyarrow==17.0.0 numpy==1.26.4
WORKDIR /app
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.headless=true", "--server.port=8501", "--browser.gatherUsageStats=false"]
