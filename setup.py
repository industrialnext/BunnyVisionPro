import re
from pathlib import Path

from setuptools import setup, find_packages

_here = Path(__file__).resolve().parent
name = "bunny_teleop"

# Reference: https://github.com/kevinzakka/mjc_viewer/blob/main/setup.py
with open(_here / name / "__init__.py") as f:
    meta_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", f.read(), re.M)
    if meta_match:
        version = meta_match.group(1)
    else:
        raise RuntimeError("Unable to find __version__ string.")

core_requirements = [
    "pyzmq",
    "numpy",
    "tornado",
]

dev_requirements = [
    "pytest",
    "black",
    "isort",
    "mypy",
]

example_requirements = [
    "dex_retargeting",
    "tyro",
    "sapien==3.0.0b0",
    "opencv-python",
]

classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: MIT License",
    "Natural Language :: English",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Topic :: Scientific/Engineering :: Artificial Intelligence",
]


def setup_package():
    # Meta information of the project
    author = "BunnyVisionPro"
    author_email = ""
    description = "Bimanual Dexterous Teleoperation with Real-Time Retargeting using VisionPro."
    url = ""
    with open(_here / "README.md", "r") as file:
        readme = file.read()

    # Package data
    packages = find_packages(".")
    print(f"Packages: {packages}")

    setup(
        name=name,
        version=version,
        author=author,
        author_email=author_email,
        maintainer=author,
        maintainer_email=author_email,
        description=description,
        long_description=readme,
        long_description_content_type="text/markdown",
        url=url,
        license="MIT",
        license_files=("LICENSE",),
        packages=packages,
        python_requires=">=3.7,<=3.12",
        zip_safe=True,
        install_requires=core_requirements,
        extras_require={
            "dev": dev_requirements,
            "example": example_requirements,
        },
        classifiers=classifiers,
    )


setup_package()
