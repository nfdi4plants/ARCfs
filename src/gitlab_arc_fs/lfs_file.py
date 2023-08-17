import requests
import urllib
from hashlib import sha256
from datetime import datetime
import io  # NOQA
import tempfile
from json import JSONDecodeError

try:
    from .gitlab_filestream import FileStreamHandler
except ImportError:
    from gitlab_filestream import FileStreamHandler


class LFSFile(tempfile.SpooledTemporaryFile):
    """
    A class representing a Gitlab LFS file ressource.
    """
    def __init__(self,
                 path: str,
                 token: str,
                 host: str,
                 namespace: str,
                 repo_id: str,
                 ref: str = None):
        """
        Constructor for the LSF file class.

        Args:
            path (str):         The path to the ressource on Gitlab inside a
                                repository.
            token (str):        A Gitlab access token with the scope "api".
            host (str):         Hostname of the GitLab serer
            namespace (str):    The "path" to the repository on GitLab
            repo_id (str):      The id of the repository on GitLab
            local_path:         Path of a local file to be uploaded.
            (str, optional)     Defaults to None.
            ref:                The commit-sha / branch name / tag of the
            (str, optional):    repository to use. Defaults to "main".

            # TODO: In case of an URL error, also try "ref=master"
        """
        super().__init__(max_size=1024*1024*100,
                         mode="w+b")
        # self.local_path = local_path

        self.path = path
        self.token = token
        self.host = host
        self.namespace = namespace
        self.repo_id = repo_id

        # Create a sha256 Object to compute the SHAsum during writing into
        # the file. This is necessary for uploading an LFS file.
        self.shasum = sha256()

        # Get the default branch.
        server_url = "https://" + self.host
        sh = FileStreamHandler(server_url, self.token)

        self.ref = sh._get_default_branch(self.repo_id,
                                          self.token,
                                          server_url)\
            if ref is None else ref

    def write(self, data) -> None:
        """
        Writes data into the tempfile associated with the LFS file ressource.
        Also updates the shasum.

        Args:
            data (bytes): Binary data to be written.
        """
        self.shasum.update(data)
        super().write(data)

    def close(self) -> None:
        """
        Closes the LFS file resource.

        NOTE: Besides the closes of the tempfile and the LFS file source
        itself, a new branch is created on the specified GitLab repository.
        Then the LFS file is uploaded and a pointerfile is commited into the
        newly crated branch. At last, the .gitattributes file is updated and a
        merge request into the 'main' branch is ceated.
        """
        size = self._get_size()
        branch = self._create_branch(self.token, self.ref, self.repo_id)
        self._upload_lfs_file(size)
        try:
            self._commit_pointer_file(self.path, self.shasum, self.repo_id,
                                      self.token, size, branch)
            self._modify_gitattributes(self.repo_id,
                                       self.token,
                                       branch,
                                       self.path)
            self._create_merge_request(self.repo_id,
                                       self.token,
                                       self.ref,
                                       branch)
        except JSONDecodeError:
            # If we got this error here, most definetly we tried
            # to commit a pointer file with the same filename as
            # an already existing one, within one second.
            # It is highly unlikely that this is wanted, so we just
            # skip this pointer file here.
            pass

        self.path = None
        self.token = None
        self.host = None
        self.namespace = None
        self.ref = None
        self.shasum = None

        super().close()

    def _get_size(self) -> int:
        """
        Gets the current size of the (data in the) LFS file.
        (or self.tempfile)
        """
        # Seek to the file end.
        super().seek(0, 2)
        # Tell the current position in the file.
        file_size = super().tell()

        return file_size

    def _upload_lfs_file(self,
                         file_size):
        """
        Uploads a LFS file tp a GitLab repository.

        Args:
            file_size (int): The site of the file contents
        """
        sha256 = self.shasum.hexdigest()

        lfs_object_request_json = {
            "operation": "upload",
            "objects": [
                {
                    "oid": f"{sha256}",
                    "size": f"{file_size}"
                }
            ],
            "transfers": [
                "lfs-standalone-file",
                "basic"
            ],
            "ref": {
                "name": "refs/heads/" + self.ref
            },
            "hash_algo": "sha256"
        }

        headers = {'Accept': 'application/vnd.git-lfs+json',
                   'Content-type': 'application/vnd.git-lfs+json'}

        # construct the download URL for the lfs resource
        download_url = "".join([
            "https://oauth2:",
            f"{self.token}",
            f"@{self.host}/",
            f"{self.namespace}.git/info/lfs/objects/batch"
        ])

        r = requests.post(download_url, json=lfs_object_request_json,
                          headers=headers)
        result = r.json()

        try:
            header_upload = result["objects"][0]["actions"]["upload"]["header"]
            url_upload = result["objects"][0]["actions"]["upload"]["href"]
            header_upload.pop("Transfer-Encoding")
            self.seek(0, 0)
            res = requests.put(url_upload,              # NOQA
                               headers=header_upload,
                               data=iter(lambda: self.read(4096*4096), b""))
        except KeyError:
            pass

    @staticmethod
    def _create_branch(token,
                       ref,
                       repo_id,
                       new_branch: str = None) -> str:
        """_summary_

        Args:
            new_branch (str, optional): _description_. Defaults to None.
        Returns:
            new_branch (str): _description
        """
        if new_branch is None:
            date_time = datetime.now()
            year = date_time.year
            month = date_time.month
            day = date_time.day
            hour = date_time.hour
            minute = date_time.minute
            second = date_time.second
            new_branch = ("Galaxy_Result_"
                          f"{year}-{month}-{day}_{hour}.{minute}.{second}")

        headers = {
            'PRIVATE-TOKEN': f'{token}',
        }

        params = {
            'branch': f'{new_branch}',
            'ref': f'{ref}',
        }

        url = (f"https://git.nfdi4plants.org/api/v4/projects/{repo_id}"
               "/repository/branches")
        response = requests.post(url, params=params, headers=headers) # NOQA

        return new_branch

    @staticmethod
    def _commit_pointer_file(path: str,
                             shasum,
                             repo_id,
                             token,
                             file_size: str,
                             branch: str) -> None:
        """
        Commits a pointer file to the specified branch.
        The content of this pointer file is the given size as well as the
        shasum of the LFS file content.

        Args:
            branch (str): The branch to commit the pointer file to.
            file_size (int): The size of the file content.
        """
        repopath_encoded = urllib.parse.quote(path, safe="")
        sha256sum = shasum.hexdigest()
        pointer_file_content = (f"version https://git-lfs.github.com/spec/v1\n"
                                f"oid sha256:{sha256sum}\nsize {file_size}\n")

        post_url = ("https://git.nfdi4plants.org/api/v4/projects/"
                    f"{repo_id}/repository/files/{repopath_encoded}")

        headers = {
            'PRIVATE-TOKEN': f'{token}',
            'Content-Type': 'application/json',
        }

        json_data = {
            'branch': f'{branch}',
            'content': f'{pointer_file_content}',
            'commit_message': 'create a new lfs pointer file',
        }

        response = requests.post(
            post_url,
            headers=headers,
            json=json_data,
        )
        res = response.json()  # NOQA

    @staticmethod
    def _create_merge_request(repo_id,
                              token,
                              ref,
                              source_branch: str,
                              target_branch: str = None):
        """_summary_

        Args:
            id (str): _description_
            source_branch (str): _description_
            target_branch (str): _description_
        """

        target_branch = ref if target_branch is None else target_branch

        headers = {
            'PRIVATE-TOKEN': f'{token}',
        }

        params = {
            'source_branch': f'{source_branch}',
            'target_branch': f'{target_branch}',
            'title': 'Galaxy run'
        }

        url = (f"https://git.nfdi4plants.org/api/v4/projects/{repo_id}"
               "/merge_requests")
        response = requests.post(url, params=params, headers=headers)  # NOQA

    @staticmethod
    def _modify_gitattributes(repo_id, token, ref, path):
        """

        """
        headers = {
            'PRIVATE-TOKEN': f"{token}"
        }

        # Get the .gitattributes file.
        file_path_gitattributes = ".gitattributes"
        download_url = (f"https://git.nfdi4plants.org/api/v4/projects/"
                        f"{repo_id}"
                        f"/repository/files/"
                        f"{file_path_gitattributes}"
                        f"/raw?ref="
                        f"{ref}")

        action = "update"
        response = requests.get(download_url, headers=headers)

        new_line = f"{path} filter=lfs diff=lfs merge=lfs -text\n"

        try:
            response.raise_for_status()
            content = response.text + "\n" + new_line
        except requests.HTTPError:
            action = "create"
            content = new_line

        data = [
            ('branch', f'{ref}'),
            ('commit_message', 'Modify .gitattributes'),
            ('actions[][action]', f'{action}'),
            ('actions[][file_path]', ".gitattributes"),
            ('actions[][content]', f"{content}")]
        url = (f'https://git.nfdi4plants.org/api/v4/projects/{repo_id}'
               f'/repository/commits')
        response = requests.post(url,
                                 headers=headers,
                                 data=data)

    # TODO:
    # - inherit from tempfile (done)
    # - implement/handle fileno(), isatty(), readline()
    #   readlines(), seek(), seekable(), tell(), truncate()
    #   writelines(), __del__()
