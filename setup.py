from pathlib import Path
from pkg_resources import parse_requirements
from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

with Path('requirements.txt').open() as f:
    install_requires = [str(req) for req in parse_requirements(f)]

setup(
    name="redframes",
    version="0.0",
    url="https://github.com/maxhumber/redframes",
    description="[re]ctangular[d]ata[frames]",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Max Humber",
    author_email="max.humber@gmail.com",
    license="BSD 2",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=install_requires,
    setup_requires=["setuptools>=65.3.0"],
)
