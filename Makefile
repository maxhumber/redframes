test: 
	python -m unittest tests

format:
	isort redframes tests
	black redframes tests

types:
	mypy redframes
	pyright redframes