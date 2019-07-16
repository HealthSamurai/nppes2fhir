.EXPORT_ALL_VARIABLES:

APP_IMAGE  = "us.gcr.io/aidbox2-205511/usnpi-loader:latest"
CLUSTER = cluster-production
BUILT_AT = $(shell date +%FT%T)

repl:
	clj -A:test:nrepl -e "(-main)" -r 

build:
	clojure -A:build

docker: build
	docker build -t ${APP_IMAGE} .

push: docker
	docker push ${APP_IMAGE}

deploy:
	cat deploy.tpl.yaml | envtpl  | kubectl apply -f -
