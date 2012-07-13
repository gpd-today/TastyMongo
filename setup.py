import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()

requires = [
    'mongoengine',
	'pyramid'
]

setup(
    name = 'tastymongo',
    version = 0.1,
    description = '''REST-ful API layer for Pyramid on top of MongoEngine''',
    long_description=README,
    author = 'Marcel van den Elst - Progressive Company',
    author_email = 'marcel@progressivecompany.nl',
    url = 'http://github.com/ProgressiveCompany/TastyMongo',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    requires=requires,
    install_requires=requires,
    tests_require=requires,
    classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Pyramid',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Utilities'
    ],
)
