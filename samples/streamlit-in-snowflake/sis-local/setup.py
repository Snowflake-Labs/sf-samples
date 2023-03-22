import setuptools

VERSION = "1.16.0rc0"  # PEP-440

NAME = "streamlit_in_snowflake"

INSTALL_REQUIRES = [
    "pyarrow[pandas]<10.1.0,>=10.0.1",
    "streamlit==1.16.0",
    "snowflake-snowpark-python[pandas]",
]


setuptools.setup(
    name=NAME,
    version=VERSION,
    description="Run Streamlit locally to emulate Streamlit-in-Snowflake.",
    url="https://streamlit.io",
    project_urls={
        "Source Code": "https://github.com/blackary/sis-tricks",
    },
    author="Snowflake Inc",
    author_email="hello@streamlit.io",
    license="Apache License 2.0",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    # Snowpark requires Python 3.8
    python_requires="==3.8.*",
    # Requirements
    install_requires=INSTALL_REQUIRES,
    packages=["streamlit_in_snowflake"]
)
