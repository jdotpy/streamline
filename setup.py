from setuptools import setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name = 'streamline',
    scripts=['bin/streamline'],
    packages=['streamline'],
    version = '0.9.19',
    description = 'CLI tool for doing async tasks and transformations',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author = 'KJ',
    author_email = 'jdotpy@users.noreply.github.com',
    url = 'https://github.com/jdotpy/streamline',
    download_url = 'https://github.com/jdotpy/streamline/tarball/master',
    keywords = ['tools'],
    classifiers = [
        "Programming Language :: Python :: 3",
    ],
)
