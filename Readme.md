 # ARCfs

ARCfs is a **experimental** implementation of a [PyFilesystem2](https://github.com/PyFilesystem/pyfilesystem2) filesystem abstraction for GitLab. <br>
Through this, ARCfs provides a filesystem-like view of [DataPLANT DataHUB ARCs](https://github.com/nfdi4plants/ARC-specification) in Python. 

## Installation

ARCfs is available as pip package.

    pip install gitlab-arc-fs

Alternatively:

1. Download / clone Repository
2. Change directory into ARCfs
3. Run `pip install .`


**NOTE:** For read acces, an API token with the scope "read_api" is sufficient. For write access the scope "api" is requiered. You can find more information [here](https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html#create-a-personal-access-token).

## Getting started

### Creating an ARCfs object

To create an ARCfs object directly, you can use the following Code.

```python
from gitlab_arc_fs.arc_fs import ARCfs

# To use the DataHUB GitLab Server: "https://git.nfdi4plants.org/"
server_url = "<URL_to_GitLab_Server>"
access_token = "<GitLab_API_access_token>"

arc_fs = ARCfs(access_token, server_url)
```

### **FS URLs**

Useful for creating filesystems from a configuration file or the command line. Creates an instance of ARCfs with a URL comparable to one you would use in a browser.

```python
from fs import open_fs

arc_fs = open_fs("arcfs://<GitLab_API_access_token>@<URL_to_GitLab_Server>")
```

The *open_fs()* function can also be used as context manager:

```python
from fs import open_fs

with open_fs("arcfs://<GitLab_API_access_token>@<URL_to_GitLab_Server>") as arc_fs:
    # Do something with arc_fs here.
```

### **Usage**

Bellow some examples how an ARCfs instance can be used.

**List directory content:**

```python
# list all ARCs / repos a user has access to (with the given API token).
arcs = arc_fs.listdir("/")

# list files/dirs inside a repository
files = arc_fs.listdir("<namespace>-<reponame>/<path_inside_repository>")

```
**NOTE:** In the \<namespace\> part, replace "." with "_", e.g. "firstname.lastname" -> "firstname_lastname".

**Open/Download a file**


```python
# using fs.open
with open("<local_path>", "wb") as local_file:
    with arc_fs.open("<remote_path>", mode="rb") as remote_file:
        for byte in iter(lambda: remote_file.read(4096), b""):
            local_file.write(byte)

# using fs.download
with open("<local_path>", "wb") as local_file:
    arc_fs.download("<remote_path>", local_file)
```

**Creating directories and uploading files**

```python
with open("<local_path>", "rb") as local_file:
    if not arc_fs.isdir("<remote_path>"):
        openfs.makedirs("<remote_path>")
    # It would also be possible to open a remote file in write modus,
    # but the upload function is preferable.
    arc_fs.upload("<remote_path>", local_file)
```
**NOTE:** Since GitLab has no real concept of directories, a newly created directory does not become permanent until a file is placed in that directory.

More information about the PyFilesystem API can be founde [here](https://docs.pyfilesystem.org/en/latest/interface.html).


## Further important information

ARCfs does not support setting file information nor the deletion of directories or files (*setinfo()*, *remove()*, *removedir()*).

Upon performing any write operation with ARCfs, the file in question is uploaded as LFS file, a new branch is created and pointer file is commited in the newly created branch.
Finally, a merge request into the main branch is created.
