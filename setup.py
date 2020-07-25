from setuptools import setup, find_packages

setup (
       name='pyMirrorMMB',
       version='0.1',
       packages=find_packages(),

       # Declare your packages' dependencies here, for eg:
       install_requires=['foo>=3'],

       # Fill in these to make your Egg ready for upload to
       # PyPI
       author='gelpi',
       author_email='gelpi@ub.edu',

       #summary = 'Just another Python package for the cheese shop',
       url='',
       license='',
       long_description='Mirror MMB ETLs python version',

       # could also include long_description, download_url, classifiers, etc.

  
       )