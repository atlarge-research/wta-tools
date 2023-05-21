#!/usr/bin/env python2
# This script (The DAS configuration script) is based on the work of Tim Hegeman, Chris Lemaire, and Laurens Versluis 
# The github repo for the original script: https://github.com/lfdversluis/das-bigdata-deployment

from . import util
from collections import namedtuple
import os.path
import shutil
import tarfile
import tempfile
import urllib2

class DownloadFailedError(Exception): pass
class MissingArchiveError(Exception): pass
class InstallFailedError(Exception): pass

class Framework(object):
    def __init__(self, identifier, name):
        self.__identifier = identifier
        self.__name = name
        self.__versions = {}

    @property
    def identifier(self):
        return self.__identifier

    @property
    def name(self):
        return self.__name

    @property
    def versions(self):
        return self.__versions.copy()

    def version(self, version_no):
        if version_no in self.__versions:
            return self.__versions[version_no]
        else:
            raise KeyError("Version %s of %s has not been registered." % (version_no, self.__name))

    def version_identifier(self, version_no):
        return "%s-%s" % (self.__identifier, version_no)

    def add_version(self, framework_version):
        self.__versions[framework_version.version] = framework_version

    def deploy(self, install_dir, framework_version, machines, settings, log_fn=util.log):
        raise NotImplementedError()

    def get_supported_deployment_settings(self, framework_version):
        return []

    def __repr__(self):
        return "Framework{identifier=%s,name=%s}" % (self.identifier, self.name)

class FrameworkVersion(object):
    def __init__(self, version, archive_url, archive_extension, archive_root_dir):
        self.__version = version
        self.__archive_url = archive_url
        self.__archive_extension = archive_extension.lstrip('.')
        self.__archive_root_dir = archive_root_dir

    @property
    def version(self):
        return self.__version

    @property
    def archive_url(self):
        return self.__archive_url

    @property
    def archive_extension(self):
        return self.__archive_extension

    @property
    def archive_root_dir(self):
        return self.__archive_root_dir

    def __repr__(self):
        return self.version

class FrameworkRegistry:
    def __init__(self):
        self.__frameworks = {}

    def register_framework(self, framework):
        self.__frameworks[framework.identifier] = framework

    @property
    def frameworks(self):
        return self.__frameworks.copy()

    def framework(self, framework_identifier):
        if framework_identifier in self.__frameworks:
            return self.__frameworks[framework_identifier]
        else:
            raise KeyError("Framework %s has not been registered." % framework_identifier)

__FrameworkRegistry_singleton = FrameworkRegistry()
def get_framework_registry():
    return __FrameworkRegistry_singleton

class FrameworkManager:
    def __init__(self, framework_registry, framework_dir, temp_dir=None):
        self.__framework_dir = framework_dir
        self.__framework_registry = framework_registry
        self.__temp_dir = temp_dir

    @property
    def framework_registry(self):
        return self.__framework_registry

    @property
    def framework_dir(self):
        return self.__framework_dir

    @property
    def archive_dir(self):
        return os.path.join(self.framework_dir, "archives")

    def __archive_file(self, framework, framework_version):
        return os.path.join(self.archive_dir, "%s.%s" % (framework.version_identifier(framework_version.version), framework_version.archive_extension))

    def __check_if_archive_present(self, framework, framework_version):
        """Checks if an archive is already present."""
        archive_file = self.__archive_file(framework, framework_version)
        return os.path.exists(archive_file) and os.path.isfile(archive_file)

    def __install_dir(self, framework, framework_version):
        return os.path.join(self.framework_dir, framework.version_identifier(framework_version.version))

    def download(self, framework_identifier, version, force_redownload=False, log_fn=util.log):
        """Fetches a Big Data framework distribution."""
        framework = self.framework_registry.framework(framework_identifier)
        framework_version = framework.version(version)
        log_fn(0, "Obtaining %s version %s distribution..." % (framework.name, version))
        
        # Check if a previous download of the framework already exists
        # If so, either remove for a forced redownload, or complete
        log_fn(1, "Checking if archive for %s version %s is present..." % (framework.name, version))
        archive_file = self.__archive_file(framework, framework_version)
        if self.__check_if_archive_present(framework, framework_version):
            if force_redownload:
                log_fn(2, "Found previously downloaded %s archive. Removing to do a forced redownload." % framework.name)
                os.remove(archive_file)
            else:
                log_fn(2, "Found previously downloaded %s archive. Skipping download." % framework.name)
                return
        else:
            log_fn(2, "%s archive not present." % framework.name)
            if not os.path.exists(self.archive_dir):
                try:
                    os.makedirs(self.archive_dir)
                except Exception as e:
                    raise DownloadFailedError("Cannot create directory \"%s\" to store the %s archive due to an unknown error: %s." % (self.archive_dir, framework.name, e))

        # Download the framework distribution
        dist_url = framework_version.archive_url
        log_fn(1, "Downloading %s version %s from \"%s\"..." % (framework.name, version, dist_url))
        try:
            with open(archive_file, "wb") as archive_stream:
                download_stream = urllib2.urlopen(dist_url, timeout=1000)
                shutil.copyfileobj(download_stream, archive_stream)
                log_fn(2, "Download complete.")
        except urllib2.HTTPError as e:
            raise DownloadFailedError("Failed to download %s from \"%s\" with HTTP status %d." % (framework.name, dist_url, e.getcode()))
        except Exception as e:
            raise DownloadFailedError("Failed to download %s from \"%s\" with unknown error: %s." % (framework.name, dist_url, e))

    def install(self, framework_identifier, version, force_reinstall=False, download_if_missing=True, log_fn=util.log):
        """Installs a Big Data framework distribution."""
        framework = self.framework_registry.framework(framework_identifier)
        framework_version = framework.version(version)
        log_fn(0, "Installing %s version %s..." % (framework.name, version))

        # Check if a previous installation of the framework already exists
        # If so, either remove for a forced reinstall, or return
        log_fn(1, "Checking if previous installation of %s version %s is present..." % (framework.name, version))
        target_dir = self.__install_dir(framework, framework_version)
        if os.path.exists(target_dir):
            if force_reinstall:
                log_fn(2, "Found previous installation of %s. Removing to do a forced reinstall." % framework.name)
                shutil.rmtree(target_dir)
            else:
                log_fn(2, "Found previous installation of %s." % framework.name)
                return
        else:
            log_fn(2, "Found no previous installation of %s." % framework.name)

        # Check if the archive file is already present
        if self.__check_if_archive_present(framework, framework_version):
            log_fn(1, "Found %s version %s archive." % (framework.name, version))
        elif download_if_missing:
            self.download(framework_identifier, version, log_fn = util.create_log_fn(1, log_fn))
        else:
            raise MissingArchiveError("Archive for %s version %s is not present in \"%s\"." % (framework.name, version, self.archive_dir))

        # Extract the distribution to a temporary directory
        log_fn(1, "Extracting %s version %s archive..." % (framework.name, version))
        try:
            extract_tmp_dir = tempfile.mkdtemp(dir = self.__temp_dir)
        except Exception as e:
            raise InstallFailedError("Failed to create temporary directory to extract %s with unknown error: %s." % (framework.name, e))
        try:
            with tarfile.open(self.__archive_file(framework, framework_version)) as archive_tar:
                archive_tar.extractall(extract_tmp_dir)
            log_fn(2, "Extraction to temporary directory complete. Moving to framework directory...")
            shutil.move(os.path.join(extract_tmp_dir, framework_version.archive_root_dir), target_dir)
            log_fn(3, "Move complete.")
        except Exception as e:
            raise InstallFailedError("Failed to extract %s archive \"%s\" with unknown error: %s." % (framework.name, self.__archive_file(framework, framework_version), e))
        finally:
            shutil.rmtree(extract_tmp_dir)

        log_fn(1, "%s version %s is now available at \"%s\"." % (framework.name, version, target_dir))

    def deploy(self, framework_identifier, version, machines, settings, log_fn=util.log):
        """Deploys a Big Data framework distribution."""
        framework = self.framework_registry.framework(framework_identifier)
        framework_version = framework.version(version)
        log_fn(0, "Deploying %s version %s to cluster of %d machines..." % (framework.name, version, len(machines)))

        install_dir = self.__install_dir(framework, framework_version)
        framework.deploy(install_dir, framework_version, machines, settings, log_fn=util.create_log_fn(1, log_fn))

    def get_supported_deployment_settings(self, framework_identifier, version):
        """Retrieves a list of supported deployment settings and their descriptions for a given Big Data framework and version."""
        framework = self.framework_registry.framework(framework_identifier)
        framework_version = framework.version(version)
        return framework.get_supported_deployment_settings(framework_version)

