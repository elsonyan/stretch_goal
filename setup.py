from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()
setup(name='Elson',
      version='0.0.1',
      author="Elson Yan",
      author_email="yansc_1996@163.com",
      description="A sample python application",
      long_description=long_description,
      long_description_content_type="text/markdown",
      url="",
      packages=['elson'],
      classifiers=[
          "Programming Language :: Python :: 3",
          "Operating System :: OS Independent",
      ],
      zip_safe=False)