test: 
	python -m unittest

format:
	isort redframes tests
	black redframes tests

types:
	mypy redframes
	pyright redframes

loc: 
	find redframes -name '*.py' | xargs wc -l | sort -nr
	find tests -name '*.py' | xargs wc -l | sort -nr
