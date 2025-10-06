import vcr
import pytest
import os
from fs.errors import ResourceNotFound, DirectoryExpected, FileExpected,\
                      NoSysPath, NoURL
from six import text_type

from gitlab_arc_fs.arc_fs import ARCfs

current_directory = os.path.dirname(os.path.realpath(__file__))
relative_path = "vcr_cassettes/"
full_path = str(os.path.join(current_directory, relative_path))
record_mode = "once"


@vcr.use_cassette(record_mode=record_mode, path=full_path+"test_listdir")
def test_listdir():
    server_url = "https://git.nfdi4plants.org/"
    token = ""
    fs = ARCfs(token, server_url)

    # Test listdir for root directory.
    # (a list of all public repositories)
    path = "/"
    expected_content = ['Martin_Kuhl-samplearc_rnaseq',
                        'Martin_Kuhl-SampleARC_Proteomics',
                        'Dominik_Brilhaus-SampleARC_metabolomics',
                        'Dominik_Brilhaus-SampleARC_RNAseq',
                        'Benedikt_Venn-LPA2_ComplexomeProfiling_Chlamy',
                        'Kevin_Schneider-yeast-structure-proportions',
                        'Oliver_Maus-TestRepo',
                        'Benedikt_Venn-Ru_ChlamyHeatstress',
                        'Adrian_Zimmer-SampleARC_RNAseq',
                        'Oliver_Maus-RedoxNetworkTopologyProteomics',
                        'Dominik_Brilhaus-CEPLAS_RNASeq_Workshop_2022',
                        'Jonathan_Hochsticher-SampleARC_RNAseq',
                        'Vincent_Leon_Gotsmann-Gotsmann_et_al_2023',
                        'Angela_Kranz-DeNovo_Assembly_Solanum_pennellii',
                        'Angela_Kranz-Genome_Sequencing_Gluconobacter_oxydans',
                        'Timo_Mühlhaus-ArcPrototype',
                        'Felix_Jung-deepSTABp',
                        'Louisa_Perelo-sampleARC_nfcore']

    dir_content = fs.listdir(path)
    print(dir_content)
    assert dir_content.sort() == expected_content.sort()

    # Check aliases for root
    assert fs.listdir(".").sort() == expected_content.sort()
    assert fs.listdir("./").sort() == expected_content.sort()

    # Check paths are unicode strings.
    for name in fs.listdir("/"):
        assert isinstance(name, text_type)

    # Test listdir for the root of a public repository.
    path = "/Timo_Mühlhaus-ArcPrototype"
    expected_content = ['.arc', 'assays', 'runs', 'studies', 'workflows',
                        '.gitattributes', 'isa.investigation.xlsx']
    dir_content = fs.listdir(path)
    assert dir_content.sort() == expected_content.sort()

    # Check paths are unicode strings.
    for name in fs.listdir(path):
        assert isinstance(name, text_type)

    # Test listdir for a directory, which is not the root of the repository.
    path = "/Louisa_Perelo-sampleARC_nfcore/studies"
    expected_content = ['YamDataset', 'YamFuncscan', '.gitkeep']
    dir_content = fs.listdir(path)
    assert dir_content.sort() == expected_content.sort()

    # Check paths are unicode strings.
    for name in fs.listdir(path):
        assert isinstance(name, text_type)

    # Check listing directory that doesn't exist
    path = "non_existing_directory"
    with pytest.raises(ResourceNotFound):
        fs.listdir(path)

    # Test lisdir with a file (Error "DirectoryExpected" is expected).
    path = "/Louisa_Perelo-sampleARC_nfcore/studies/YamDataset/isa.study.xlsx"
    with pytest.raises(DirectoryExpected):
        fs.listdir(path)

    # Test lisdir with a LFS file (Error "DirectoryExpected" is expected).
    path = ("Dominik_Brilhaus-SampleARC_RNAseq/assays/"
            "Talinum_RNASeq_minimal/dataset/"
            "DB_097_CAMMD_CAGATC_L001_R1_001.fastq.gz")
    with pytest.raises(DirectoryExpected):
        fs.listdir(path)


@vcr.use_cassette(record_mode=record_mode, path=full_path+"test_exists")
def test_exists():
    server_url = "https://git.nfdi4plants.org/"
    token = ""
    fs = ARCfs(token, server_url)

    # Test exists method.
    # Check root directory always exists
    assert fs.exists("/")
    assert fs.exists("")

    # Check files/directories don't exist.
    assert not fs.exists("/Louisa_Perelo-sampleARC_nfcore/studies/YamDataset/"
                         "isaa.study.xlsx")
    assert not fs.exists("/Louisa_Perelo-sampleARC_nfcore/YamDataset/"
                         "isaa.study.xlsx")
    assert not fs.exists("/test/test")

    # Check for existing files/directories.
    # repo root dir
    assert fs.exists("Timo_Mühlhaus-ArcPrototype")
    # file in repo root dir
    assert fs.exists("Timo_Mühlhaus-ArcPrototype/isa.investigation.xlsx")
    # dir in repo root dir
    assert fs.exists("Timo_Mühlhaus-ArcPrototype/assays")
    # file in dir inside repo
    assert fs.exists("Timo_Mühlhaus-ArcPrototype/assays/measurement1/"
                     "isa.assay.xlsx")


@vcr.use_cassette(record_mode=record_mode, path=full_path+"test_root_dir")
def test_root_dir():
    server_url = "https://git.nfdi4plants.org/"
    token = ""
    fs = ARCfs(token, server_url)

    with pytest.raises(FileExpected):
        fs.open("/")
    with pytest.raises(FileExpected):
        fs.openbin("/")


@vcr.use_cassette(record_mode=record_mode, path=full_path+"test_basic")
def test_basic():
    server_url = "https://git.nfdi4plants.org/"
    token = ""
    fs = ARCfs(token, server_url)
    #  Check str and repr don't break
    repr(fs)
    assert isinstance(text_type(fs), text_type)


@vcr.use_cassette(record_mode=record_mode, path=full_path+"test_getmeta")
def test_getmeta():
    server_url = "https://git.nfdi4plants.org/"
    token = ""
    fs = ARCfs(token, server_url)
    # Get the meta dict
    meta = fs.getmeta()

    # Check default namespace
    assert meta == fs.getmeta(namespace="standard")

    # Must be a dict
    assert isinstance(meta, dict)

    no_meta = fs.getmeta("__nosuchnamespace__")
    assert isinstance(no_meta, dict)
    assert not no_meta


@vcr.use_cassette(record_mode=record_mode, path=full_path+"test_isfile")
def test_isfile():
    server_url = "https://git.nfdi4plants.org/"
    token = ""
    fs = ARCfs(token, server_url)

    # Check files which do not exist
    assert not fs.isfile("foo.txt")
    assert not fs.isfile("Timo_Mühlhaus-ArcPrototype/isaaa.investigation.xlsx")
    assert not fs.isfile("Timo_Mühlhaus-ArcPrototype/assays/measurement1/"
                         "isaaa.assay.xlsx")

    # Check files which exist
    assert fs.isfile("Timo_Mühlhaus-ArcPrototype/isa.investigation.xlsx")
    assert fs.isfile("Timo_Mühlhaus-ArcPrototype/assays/measurement1/"
                     "isa.assay.xlsx")

    # Check directories
    assert not fs.isfile("bar")
    assert not fs.isfile("/")
    assert not fs.isfile("/Louisa_Perelo-sampleARC_nfcore")
    assert not fs.isfile("/Louisa_Perelo-sampleARC_nfcore/assays")


@vcr.use_cassette(record_mode=record_mode, path=full_path+"test_islink")
def test_islink():
    """
    There are no symlinks on this filesystem.
    """
    server_url = "https://git.nfdi4plants.org/"
    token = ""
    fs = ARCfs(token, server_url)

    # Expect "false" for existing directories and files.
    assert not fs.islink("")
    assert not fs.islink("Timo_Mühlhaus-ArcPrototype/isa.investigation.xlsx")
    assert not fs.islink("Timo_Mühlhaus-ArcPrototype/assays/measurement1/"
                         "isa.assay.xlsx")

    # Check for correct error message if the files does not exist.
    with pytest.raises(ResourceNotFound):
        fs.islink("bar")


@vcr.use_cassette(record_mode=record_mode, path=full_path+"test_getsize")
def test_getsize():
    """
    At the moment, a directory has a size of None.
    Could be changed to 0. THis size describes the overhead
    size described by (directory_size - sum_files ind directory)
    """
    server_url = "https://git.nfdi4plants.org/"
    token = ""
    fs = ARCfs(token, server_url)

    assert fs.getsize("/") == (0 or None)
    assert fs.getsize("Dominik_Brilhaus-SampleARC_RNAseq") == (0 or None)
    assert fs.getsize("Dominik_Brilhaus-SampleARC_RNAseq/"
                      "assays") == (0 or None)
    # Test normal (and LSF) file.
    assert fs.getsize("Dominik_Brilhaus-SampleARC_RNAseq"
                      "/README.md") == 464
    # assert fs.getsize("brilator-samplearc_rnaseq/assays/"
    #                   "Talinum_RNASeq_minimal/dataset/" f
    #                   "DB_097_CAMMD_CAGATC_L001_R1_001.fastq.gz")\
    #     == 1451886904

    with pytest.raises(ResourceNotFound):
        fs.getsize("doesnotexist")


@vcr.use_cassette(record_mode=record_mode, path=full_path+"test_getsyspath")
def test_getsyspath():
    server_url = "https://git.nfdi4plants.org/"
    token = ""
    fs = ARCfs(token, server_url)

    try:
        syspath = fs.getsyspath("Dominik_Brilhaus-SampleARC_RNAseq/assays")
    except NoSysPath:
        assert not fs.hassyspath("Dominik_Brilhaus-SampleARC_RNAseq/assays")
    else:
        assert isinstance(syspath, text_type)
        assert isinstance(fs.getospath("Dominik_Brilhaus-SampleARC_RNAseq/"
                                       "assays"),
                          bytes)
        assert fs.hassyspath("Dominik_Brilhaus-SampleARC_RNAseq/assays")
    # Should not throw an error
    fs.hassyspath("a/b/c/Dominik_Brilhaus-SampleARC_RNAseq/assays/bar")


@vcr.use_cassette(record_mode=record_mode, path=full_path+"test_geturl")
def test_geturl():
    server_url = "https://git.nfdi4plants.org/"
    token = ""
    fs = ARCfs(token, server_url)
    try:
        fs.geturl("Timo_Mühlhaus-ArcPrototype")
    except NoURL:
        assert not fs.hasurl("Timo_Mühlhaus-ArcPrototype")
    else:
        assert fs.hasurl("Timo_Mühlhaus-ArcPrototype")
    # Should not throw an error
    fs.hasurl("a/b/c/Timo_Mühlhaus-ArcPrototype/bar")


@vcr.use_cassette(record_mode=record_mode,
                  path=full_path+"test_geturl_purpose")
def test_geturl_purpose():
    """Check an unknown purpose raises a NoURL error."""
    server_url = "https://git.nfdi4plants.org/"
    token = ""
    fs = ARCfs(token, server_url)
    with pytest.raises(NoURL):
        fs.geturl("Timo_Mühlhaus-ArcPrototype", purpose="__nosuchpurpose__")
