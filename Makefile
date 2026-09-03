.PHONY : build image

clean:
	rm fulminata
	rm fulminata.tar

start:
	ROOT_FOLDER=data/ PORTS=38542 NICK="Fulminata|Dev" TLS_CERT_PATH="./secrets/fulminata.cert" TLS_KEY_PATH="./secrets/fulminata.key" go run .

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o fulminata

image: build
	docker rmi phaethon.local:5000/fulminata > /dev/null 2>&1 || true
	docker build . --tag phaethon.local:5000/fulminata
	rm fulminata

push: 
	docker push phaethon.local:5000/fulminata
