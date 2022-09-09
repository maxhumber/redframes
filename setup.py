from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="redframes",
    version="1.0b1",
    url="https://github.com/maxhumber/redframes",
    description="[re]ctangular[d]ata[frames]",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Max Humber",
    author_email="max.humber@gmail.com",
    license="BSD 2",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=["pandas>=1.4.4"],
)
