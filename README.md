# Nestroy Entities

Automagically export data from [Baserow](https://baserow.io/) 

## initial (local) set up

* create a virtual environment `python -m venv venv`
* update pip to latest version and install needed python packages `pip install -U pip && pip install -r requirements.txt`
* copy/rename `dummy.env` to `secret.env` and add your Baserow credentials

## export the data

run `./export_env_variables.sh` to set your Baserow credentials as environment variables.
* run `python dump_data.py`

## GitHub-Actions

* Create GitHub secrets called `BASEROW_USER`,  `BASEROW_PW` and `BASEROW_TOKEN` add your Baserow credentials
* Go to GitHub Actions and start the workflow -> the exported data will be checked into your repo

-----
created with [basrow-dump-cookiecutter](https://github.com/acdh-oeaw/transkribus-export-cookiecutter)