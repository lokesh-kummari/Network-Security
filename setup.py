'''
The setup.py file is an essential part of packaging and 
distributing Python projects. It is used by setuptools 
(or distutils in older Python versions) to define the configuration 
of your project, such as its metadata, dependencies, and more
'''

from setuptools import find_packages, setup
from typing import List


def get_requirements() -> List[str]:
    '''
    returns a list of requirements from the given file path.
    '''
    requirements_list = []
    try:
        with open('requirements.txt','r') as file:
            lines=file.readlines()
            for line in lines:
                requirements=line.strip()
                ## ignore empty lines and -e .
                if requirements and requirements!= '-e .':
                    requirements_list.append(requirements)
    except FileNotFoundError:
        print(f"Warning: {file_path} not found. Returning an empty list.")
    return requirements_list

setup(
    name='NetworkSecurity',
    version='0.0.1',
    author='Lokesh Kummari',
    author_email='klokesh5401@gmail.com',
    packages=find_packages(),
    install_requires=get_requirements()
)