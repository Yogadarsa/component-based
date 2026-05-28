from setuptools import setup, find_packages

setup(
    name="flowforge",
    version="1.0.0",
    author="FlowForge Contributors",
    description="A Python Workflow Pipeline Engine for defining and executing multi-step workflows as DAGs",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/flowforge/flowforge",
    packages=find_packages(exclude=["tests*", "examples*"]),
    python_requires=">=3.9",
    install_requires=[],  # Zero external dependencies
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="workflow pipeline dag orchestration scheduler",
)
