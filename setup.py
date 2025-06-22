from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="hvdc-analysis-pipeline",
    version="1.0.0",
    author="HVDC Analysis Team",
    author_email="team@hvdc-project.com",
    description="HVDC 창고 데이터 분석을 위한 완전 자동화 파이프라인",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hvdc-project/analysis-pipeline",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "hvdc-analysis=HVDC_analysis:main",
        ],
    },
) 