# Imagen del plugin `mayusculas` para usarlo como contenedor.
#   docker build -t jaiba-plugin-mayusculas:0.1.0 examples/plugins/mayusculas
# Manifiesto que la usa: examples/plugins-container/mayusculas/plugin.yaml
FROM python:3.13-alpine
COPY mayusculas.py /plugin/mayusculas.py
USER 65534:65534
ENTRYPOINT ["python3", "-u", "/plugin/mayusculas.py"]
