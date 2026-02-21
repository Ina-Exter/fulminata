.PHONY : build image

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
