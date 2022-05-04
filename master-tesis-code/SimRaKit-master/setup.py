from setuptools import setup

setup(
   name='SimRaKit',
   version='1.0',
   description='A Toolkit to explore the SimRa Dataset',
   author='Leonard Thomas',
   packages=['SimRaKit'], 
   install_requires=['gitpython', 'pandas', 'matplotlib', "geopandas", "geopy"],
)
