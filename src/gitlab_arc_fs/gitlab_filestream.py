import aiohttp
import asyncio
import urllib
import urllib3
import requests
import logging
from datetime import datetime

now = datetime.now()
logging.basicConfig(filename="output.log",
                    level=logging.ERROR)


class FileStreamHandler():
    """
    A Class for handling requests and url construction for the retrievment of
    files and metadata from gitlab.
    """
    def __init__(self,
                 server_url,
                 token):
        """

        """
        self.server_url = server_url
        self.token = token

    def construct_url(self, path: str, repo_id: int, repo_path: str):
        """
        Construct urls fot the retrievment of files from gitlab.
        More information: https://docs.gitlab.com/ee/api/repository_files.html

        Args:
            path (str):         The path to the repository file.
            repo_id (int):      The id of the repository.
            repo_path (str):    The path to the repository.

        Returns:
            download_url (str): The download url for the requested file.
        """
        path = path.replace(repo_path, "")
        if path.startswith('/'):
            path = path[1:]

        ref = self._get_default_branch(repo_id,
                                       self.token,
                                       self.server_url)

        path = urllib.parse.quote(path, safe="")
        download_url = (f"{self.server_url}/api/v4/projects/"
                        f"{repo_id}"
                        f"/repository/files/"
                        f"{path}"
                        f"/raw?ref="
                        f"{ref}&lfs=true")
        return download_url

    @staticmethod
    def _get_default_branch(repo_id,
                            token,
                            server_url: str) -> str:
        """
        Get the default branch for a given repo repo.

        Args:
            TODO: update here

        Returns:
            default_branch (str): The default branch for this repo.

        Raises:
            Some kind of Request error when the repo is not fount or in
            case of a timeout error.
        """
        # Get a json response of all the branches in the given repository.
        download_url = (f"{server_url}/api/v4/projects/"
                        f"{repo_id}/repository/branches")
        try:
            r = requests.get(download_url,
                             headers={"PRIVATE-TOKEN": token})
            r.raise_for_status()
        except requests.HTTPError as e:
            print("Bad status code:", r.status_code)
            print("Exiting program")
            raise SystemExit(e)
        except requests.exceptions.Timeout:
            print("Timout error")
            # TODO: Add some retry functionality here.
        except requests.exceptions.RequestException as e:
            print("Recieved request exception:")
            raise SystemExit(e)

        branches = r.json()
        next_references = True

        # Ahha. While not documented, it looks like the brnaches API
        # also uses pagination. Sneaky.
        while next_references:
            try:
                download_url = r.links["next"]["url"]
            except KeyError:
                next_references = False
                continue
            try:
                r = requests.get(download_url,
                                 headers={"PRIVATE-TOKEN":
                                          token})
                r.raise_for_status()
            except requests.HTTPError as e:
                print("Bad status code:", r.status_code)
                print("Exiting program")
                raise SystemExit(e)
            except requests.exceptions.Timeout:
                print("Timout error")
                # TODO: Add some retry functionality here.
            except requests.exceptions.RequestException as e:
                print("Recieved request exception:")
                raise SystemExit(e)
            branches.extend(r.json())

        # Check which branch is the default branch.
        for branch in reversed(branches):
            if branch["default"]:
                # break after the default branch is identified.
                default_branch = branch["name"]
                break
        # if not default_branch:
        #     default_branch = "main"

        return default_branch

    async def _get_gitlab_metadata(self,
                                   url: str,
                                   path,
                                   session: aiohttp.client.ClientSession,
                                   semaphore: asyncio.locks.BoundedSemaphore):
        """
        Probe (asynchronously) wheter the file specified by an url is a lfs
        file or not.
        This is done by sending a request to the gitlab server and reading and
        the evaluating the first 1024 Byte of the response.

        Since we need a request for each file, we also collect all other
        metadata here to avoid doing all of the requests again.

        Args:
            url (str): The url to the file to probe.
            session (aiohttp.client.Session): The session to use for the
                                              request.
            semaphore (aiohttp.client.Semaphore): A semaphore object to
                                                  limit the concurrency.
        Returns:
            True (bool):    if the file is a lfs file
            False (bool):   if the file is not a lfs file
            None:           if the file is not found
            TODO: Annotate the new return type here. Aso use it everywhere.
        """
        async with semaphore, session.get(url,
                                          allow_redirects=False,
                                          headers={"PRIVATE-TOKEN":
                                                   self.token,
                                                   }) as resp:
            headers = resp.headers
            status = resp.status
            redirected = False
        if status in {301, 302}:
            redirect_url = headers.get('Location')
            async with semaphore, session.get(redirect_url,
                                              allow_redirects=False,
                                              headers={"PRIVATE-TOKEN":
                                                       self.token,
                                                       }) as resp:

                status = resp.status
                headers = resp.headers
                redirected = True
        logging.debug(f"Begining download of {url}\n")
        # await asyncio.sleep(10)
        if status >= 302:
            if status == 404:
                logging.warning(f"Attention: File with URL {url} "
                                f"was not found")
                return None
        if not redirected:
            size = headers['X-Gitlab-Size']
        else:
            try:
                size = headers['Content-Length']
            # For some reason unkown to men, I only get the content
            # length in some cases. To retrieve the filesize anyway,
            # read the size from the content of the header files
            # (since we got redirected before, it must be a pointer file)
            except KeyError:
                url = url.replace("&lfs=true", "")
                async with semaphore, session.get(url,
                                                  allow_redirects=False,
                                                  headers={"PRIVATE-TOKEN":
                                                           self.token,
                                                           }) as resp:
                    try:
                        content = await resp.text()
                        lines = content.split("\n")
                        for line in lines:
                            try:  # empty lines
                                (key, value) = line.split(" ", 2)
                                if key == "size":
                                    size = value
                            except Exception as ex:  # NOQA
                                pass
                    except aiohttp.ClientError:
                        size = 0
                        # TODO: log stuff here

        fileinfo = {"name": path.parts[-1],
                    "size": size}
        return fileinfo

    def get_file_stream(self,
                        path: str,
                        repo_id: int,
                        repo_path) -> urllib3.response.HTTPResponse:
        """
        Provides a file stream from a gitlab server.

        Args:
            path: The path to a (file) resource on the Gitlab Server.

        Raises:
            requests.HTTPError : If the server returns an HTTP error.

        Returns:
            requests.Response :  response object containing either
                                 Data/Metadata of a resource or the
                                 referencedata of a lfs resource.
        """
        # construct the dowloadURL to retriev information about a resource
        if path.startswith('/'):
            path = path[1:]

        download_url = self.construct_url(path, repo_id, repo_path)

        try:
            r = requests.get(download_url,
                             headers={"PRIVATE-TOKEN": self.token},
                             stream=True)
            r.raise_for_status()
        except requests.HTTPError as e:
            print("Bad status code:", r.status_code)
            print("Exiting program")
            raise SystemExit(e)
        except requests.exceptions.Timeout:
            print("Timout error")
            # TODO: Add some retry functionality here.
        except requests.exceptions.RequestException as e:
            print("Recieved request exception:")
            raise SystemExit(e)

        return r
