FROM huggingface/transformers-cpu

LABEL maintainer="WattleFlow"

WORKDIR /workspace

COPY requirements.txt .

RUN apt-get update
RUN apt-get install \
	build-essential \
	git \
	curl \
	ca-certificates \
	python3 \
	python3-pip

RUN python3 -m pip install --no-cache-dir --upgrade pip && \
	 python3 -m pip install --no-cache-dir -r requirements.txt

RUN python3 -m spacy download en_core_web_sm

CMD ["/bin/bash"]
