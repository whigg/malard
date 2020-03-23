import setuptools



with open("README.md", "r") as fh:

    long_description = fh.read()

setuptools.setup(

     name='MalardInterface',  

     version='0.5',

     scripts=['MalardInterface'] ,

     author="Jonathan Alford",

     author_email="jonathan@earthwave.co.uk",

     description="MalardInterface is a web-client package for GEO Data Analytics",

     long_description=long_description,

   long_description_content_type="text/markdown",

     url="https://github.com/earthwave/malard/tree/master/python/MalardInterface",

     packages=setuptools.find_packages(),
     py_modules=['MalardClient.MalardClient', 'MalardClient.DataSetQuery', 'MalardClient.AsyncDataSetQuery','MalardClient.DataSet','MalardClient.BoundingBox', 'MalardClient.Helpers','MalardClient.Projection'],
     classifiers=[

         "Programming Language :: Python :: 3",

         "License :: OSI Approved :: MIT License",

         "Operating System :: OS Independent",

     ],

 )
