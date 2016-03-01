from setuptools import setup

setup(name='pythonSynapseUtils',
      version='0.1',
      description='Common functions for data curation with Synapse.',
      long_description=open('README.md').read(),
      url='https://github.com/Sage-Bionetworks/pythonSynapseUtils',
      author='Abhishek Pratap, Kenny Daily, Kristen Dang, Xindi Guo',
      license='Apache',
      packages=['pythonSynapseUtils'],
      install_requires=['synapseclient>1.2', 'PyYAML'],
      zip_safe=False)
