ARG PYTHON_IMAGE=python:3.13-slim-bookworm
FROM ${PYTHON_IMAGE}
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUTF8=1
COPY framework /app/framework
COPY student /app/student
USER 10001:10001
CMD ["python", "-m", "student.client", "--help"]
