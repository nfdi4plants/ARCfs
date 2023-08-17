from setuptools import setup
setup(
    name='gitlab_arc_fs',  # Name in PyPi
    author="Julian Weidhase",
    author_email="julian.weidhase@rz.uni-freiburg.de",
    description="An experimental and GitLab filesystem extension "
                "for PyFilesystem2.!",
    install_requires=[
        "fs>=2.4.16",
        "urllib3>=1.26.13",
        "requests>=2.28.1",
        "aiohttp>=3.8.3"
    ],
    entry_points={
        'fs.opener': [
            'arcfs = gitlab_arc_fs.opener:ARCfsOpener',
        ]
    },
    license="MY LICENSE",
    packages=['gitlab_arc_fs'],
    package_dir={'gitlab_arc_fs': 'src/gitlab_arc_fs'},
    version="0.0.15.dev1",
    url="https://git.bwcloud.uni-freiburg.de/julian.weidhase/GitlabFS",
    python_requires='>=3.8'
)
