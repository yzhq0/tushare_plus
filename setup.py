from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="tushare_plus",
    version="0.1.6",
    author="yzhq0",
    author_email="yangzhq0@live.com",
    description="增强版Tushare API客户端，提供自动分页、并发请求和频率限制功能",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yzhq0/tushare_plus",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Office/Business :: Financial",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.6",
    install_requires=[
        "pandas>=1.0.0",
    ],
    keywords="tushare, finance, stock, data, api",
)