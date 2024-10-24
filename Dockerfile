FROM golang

WORKDIR /opt/camus
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ENTRYPOINT [ "go", "run", ".", "start", "conf.json" ]