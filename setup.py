from setuptools import setup, find_packages

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()
    print(long_description)

setup(
    name='targeto',
    version='0.1.22',
    packages=find_packages(),
    install_requires=[
        'click',
        'pandas>=2.1.4',
        'requests>=2.31.0',
        'oblivious==7.0.0',
        'phonenumbers>=8.13.29',
        'urllib3==1.26.18',
        'pysodium>=0.7.17',
        'boto3>=1.28.72',
        'snowflake-connector-python>=3.0.0',
        'simple-salesforce>=1.12.1',
        'azure-storage-blob>=12.10.0',
        'firebase-admin>=6.0.1',
        'PyYAML>=6.0',
        'appdirs>=1.4.4',
        'libsodium',

    ],
    entry_points={
        'console_scripts': [
            'targeto=targeto_cli.cli:cli',
        ],
    },
    author='targeto.io',
    author_email='yl@logicode.in',
    description='Your CLI Tool Description',
    long_description=long_description,  # Include the contents of README.md
    long_description_content_type='text/markdown',
    url='',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
    ],
    # install_requires=['click',
    #                     "bson >= 0.5.10", 
    #                   "requests_toolbelt = 1.0.0",
    #                   "urllib3 = 2.1.0",],
   
    python_requires=">=3.8.0",
)

