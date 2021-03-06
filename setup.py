from setuptools import setup

setup(name='synAnnotationUtils',
      version='0.3',
      description='Annotation management for Synapse.',
      long_description=open('README.md').read(),
      url='https://github.com/Sage-Bionetworks/synAnnotationUtils',
      author='Kenneth Daily, Kristen Dang, Xindi Guo',
      license='Apache',
      packages=['synAnnotationUtils'],
      install_requires=['synapseclient>1.2', 'PyYAML'],
      zip_safe=False,
      scripts=['bin/schema2text.py'])
