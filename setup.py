import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='tunecapsule',
    version='0.0.0',
    author='IdmFoundInHim',
    author_email='idmfoundinhim@gmail.com',
    description='Keep your music close and your favorites closer',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/IdmFoundInHim/streamsort',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3.10',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Development Status :: 2 - Pre-Alpha',
        'Natural Language :: English',
        'Topic :: Multimedia :: Sound/Audio',
        'Typing :: Typed',
    ],
    keywords='playlists music rating ranking backup library liked',
    python_requires='>=3.10',
    install_requires=[  # Licenses
        'streamsort>=0.0.2',  # MIT
        'spotipy~=2.15',  # MIT
        'more-itertools>=8.0.0',  # MIT
    ],
    package_data={
        'tunecapsule': ['README.md'],
    }
)