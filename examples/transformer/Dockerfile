FROM huggingface/transformers-cpu

RUN apt update \
    && apt install -y bash \
    && apt install -y build-essential \
        git \
        curl \
        ca-certificates \
        libffi-dev \
        python3 \
        python3-pip
    # rm -rf /var/lib/apt/lists

WORKDIR /workspace

COPY requirements.txt .

RUN python3 -m pip install --no-cache-dir --upgrade pip 

RUN pip install -r requirements.txt \
    && python3 -m spacy download en_core_web_sm

CMD ["/bin/bash"]
