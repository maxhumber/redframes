test: 
	python -m unittest

format:
	isort redframes tests
	black redframes tests

types:
	mypy redframes
	pyright redframes