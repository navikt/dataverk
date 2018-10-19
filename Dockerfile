# Pull a docker image with oracle client and python3.6 installed
FROM repo.adeo.no:5443/oracle18-3-python36:0.0.1

COPY ca-bundle.crt /usr/local/share/ca-certificates/ca-bundle.crt

COPY ./ /ai-lab-datapipe

RUN pip3.6 install --proxy http://webproxy-utvikler.nav.no:8088 --cert /usr/local/share/ca-certificates/ca-bundle.crt --no-cache-dir -r /ai-lab-datapipe/requirements.txt && \
    pip3.6 install --proxy http://webproxy-utvikler.nav.no:8088 --cert /usr/local/share/ca-certificates/ca-bundle.crt --no-cache-dir /ai-lab-datapipe && \
    rm -rvf /ai-lab-datapipe
