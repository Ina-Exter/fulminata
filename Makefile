.PHONY : build image

start:
	ROOT_FOLDER=data/ PORTS=38542 NICK="Fulminata|Dev" TLS_CERT_PATH="./secrets/fulminata.cert" TLS_KEY_PATH="./secrets/fulminata.key" go run .

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o fulminata

image: build
	docker rmi fulminata
	docker build . --tag fulminata
	docker save fulminata > fulminata.tar
	rm fulminata

push: 
	docker tag fulminata phaethon.local:5000/fulminata
	docker push phaethon.local:5000/fulminata
