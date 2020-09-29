"""Installation for AsyncFlow. """

import os

import setuptools

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()

setuptools.setup(
    name="asyncflow",
    version="0.1",
    license="MIT",
    author="Jacob Unna",
    author_email="jacob.unna@gmail.com",
    description="Concurrency made easy",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url="https://github.com/jacobunna/asyncflow",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Natural Language :: English ",
        "Typing :: Typed",
    ],
    keywords="concurrency thread async asyncio curio trio parallelism",
    py_modules=["asyncflow"],
    python_requires=">=3.8",
    extras_require={"dev": ["pytest", "sphinx>=3,<4", "curio", "trio"]},
)
