from setuptools import setup

setup(
    name='bloodhound_import',
    version='0.0.9',
    description='BloodHound import from python',
    license='MIT',
    classifiers=[
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
    ],
    author='Matthijs Gielen / Fox-IT',
    author_email='matthijs.gielen@fox-it.com',
    url='https://github.com/fox-it/bloodhound-import',
    packages=['bloodhound_import'],
    python_requires='>=3.8',
    install_requires=['neo4j>=5', 'ijson'],
    entry_points={
        'console_scripts': ['bloodhound-import=bloodhound_import.__main__:cli']
    }
)
