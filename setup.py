
from setuptools import setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize(
        ["order_struct.pyx", "cython_engine.pyx"],
        language_level=3
    )
)
