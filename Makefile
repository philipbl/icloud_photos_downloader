DOCKER2 = docker run -i --rm -v $(PWD):/app --user="$(shell id -u):$(shell id -g)" icloud_photos_downloader

DOCKER = docker run -i --rm -v $(PWD):/app icloud_photos_downloader

update_pip: Pipfile
	$(DOCKER) pipenv install
	docker build -t icloud_photos_downloader .

build:
	docker build -t icloud_photos_downloader .
	
run:
	$(DOCKER) python backup_photos.py