from distutils.core import setup

setup(
    name = 'crank',
    scripts=['bin/crank'],
    packages=[
        'crank',
    ],
    version = '0.1.0',
    description = 'CLI tool for doing async tasks and transformations',
    author = 'KJ',
    author_email = 'jdotpy@users.noreply.github.com',
    url = 'https://github.com/jdotpy/crank',
    download_url = 'https://github.com/jdotpy/crank/tarball/master',
    keywords = ['tools'],
    classifiers = [],
)
