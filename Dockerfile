FROM mysterysd/wzmlx:v3

WORKDIR /usr/src/app

RUN useradd -m -u 1000 wzmlx && chown -R wzmlx:wzmlx /usr/src/app && chmod 755 /usr/src/app

COPY --chown=wzmlx:wzmlx requirements.txt .
RUN uv pip install --python /wzvenv/bin/python --no-cache-dir -r requirements.txt

COPY --chown=wzmlx:wzmlx . .

USER wzmlx

ENTRYPOINT ["bash", "start.sh"]