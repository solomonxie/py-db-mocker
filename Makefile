

localenv:
	[ -e .git ] && [ ! -e .git/venv ] && virtualenv -p python3 .git/venv ||true
	yes | .git/venv/bin/python -m pip install -r requirements.txt
	yes | .git/venv/bin/python -m pip install -r requirements-test.txt
