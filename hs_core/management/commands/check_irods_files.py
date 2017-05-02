# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
import os.path

from django.conf import settings
from django.core.exceptions import ValidationError
from django_irods.icommands import SessionException
from django_irods.storage import IrodsStorage
from django.core.management.base import BaseCommand

from hs_core.models import BaseResource

# This is a clone of the migration code from 1.9.* to 1.10.0, turned into a file checker.
# It is a bit more careful than the migrations, in the sense that Django is running and
# it can afford to make multiple calls to exists() to check whether paths are correct.
# However, in testing, I have never come across a situation in which there was ambiguity.
# Let's see if this code uncovers any.

# Clones of routines present in BaseResource for 1.10.0


def is_federated(resource):
    """ Clone of new BaseResource.is_federated """
    return resource.resource_federation_path is not None and \
        resource.resource_federation_path != ''


def get_path(rfile, filename, folder=None):
    """
    Clone of new hs_core.models.get_path (without model method references)

    Get a path from a ResourceFile, filename, and folder

    :param rfile: instance of ResourceFile to use
    :param filename: file name to use (without folder)
    :param folder: folder for file

    The filename is only a single name. This routine converts it to an absolute
    path that can be federated or local.  The instance points to the Resource record,
    which contains the federation path. Folder is not optional, but the default is None.
    """
    return get_resource_file_path(rfile.resource, filename, folder)


def get_resource_file_path(resource, filename, folder=None):
    """
    Clone of new hs_core.models.get_resource_file_path (without model method references)

    Dynamically determine storage path for a FileField based upon whether resource is federated

    :param resource: resource containing the file.
    :param filename: name of file without folder.
    :param folder: folder of file

    The filename is only a single name. This routine converts it to an absolute
    path that can be federated or local. The resource contains information on how
    to do this.

    """
    if folder is not None:
        # use subfolder
        return os.path.join(file_path(resource), folder, filename)
    else:
        # use root folder
        return os.path.join(file_path(resource), filename)


def root_path(resource):
    """
    Clone of new BaseResource.root_path (without model method references)

    Return the root folder of the iRODS structure containing resource files

    Note that this folder doesn't directly contain the resource files;
    They are contained in ./data/contents/* instead.
    """
    if is_federated(resource):
        return os.path.join(resource.resource_federation_path, resource.short_id)
    else:
        return resource.short_id


def file_path(resource):
    """
    Clone of new BaseResource.file_path (without model method references)

    Return the file path of the resource. This is the root path plus "data/contents".

    This is the root of the folder structure for resource files.
    """
    return os.path.join(root_path(resource), "data", "contents")


def get_path_from_storage_path(rfile, path, test_exists=True):
    """
    Clone of new ResourceFile.set_storage_path (without model method references)

    Bind this ResourceFile instance to an existing file.

    :param path: the path of the object to bind to.
    :param test_exists: if True, test for path existence in iRODS

    Path can be absolute or relative.

        * absolute paths contain full irods path to local or federated object.
        * relative paths start with anything else and can start with optional folder

    :raises ValidationError: if the pathname is inconsistent with resource configuration.
    It is rather important that applications call this rather than simply calling
    resource_file = "text path" because it takes the trouble of making that path
    fully qualified so that IrodsStorage will work properly.

    This records file_folder for future possible uploads and searches.

    The heavy lifting in this routine is accomplished via path_is_acceptable and get_path,
    which together normalize the file name.  Regardless of whether the internal file name
    is qualified or not, this makes it fully qualified from the point of view of the
    IrodsStorage module.

    """
    folder, base = path_is_acceptable(rfile, path, test_exists=test_exists)
    return get_path(rfile, base, folder=folder)


def get_path_from_short_path(rfile, path, test_exists=True):
    """
    Clone of new ResourceFile.set_short_path (without model method references)

    Set a path to a given path, relative to resource root

    There is some question as to whether the short path should be stored explicitly or
    derived as in short_path above. The latter is computationally expensive but results
    in a single point of truth.
    """
    folder, base = os.path.split(path)
    if folder == "":
        folder = None
    path = get_path(rfile, base, folder=folder)
    if test_exists:
        resource = rfile.resource
        istorage = get_irods_storage(resource)
        if not istorage.exists(path):
            raise ValidationError("path {} does not exist in iRODS".format(path))
    return path


def path_is_acceptable(rfile, path, test_exists=True):
    """
    Clone of new ResourceFile.path_is_acceptable (without model method references)

    Determine whether a path is acceptable for this resource file

    Called inside ResourceFile objects to check paths

    :param path: path to test
    :param test_exists: if True, test for path existence in iRODS

    """
    return resource_path_is_acceptable(rfile.resource, path, test_exists)


def resource_path_is_acceptable(resource, path, test_exists=True):
    """
    Determine whether a path is acceptable for this resource file

    Called outside ResourceFile objects or before such an object exists

    :param path: path to test
    :param test_exists: if True, test for path existence in iRODS

    This has the side effect of returning the short path for the resource
    as a folder/filename pair.
    """
    if test_exists:
        storage = get_irods_storage(resource)
    locpath = os.path.join(resource.short_id, "data", "contents") + "/"
    relpath = path
    fedpath = resource.resource_federation_path
    if fedpath and relpath.startswith(fedpath + '/'):
        if test_exists and not storage.exists(path):
            raise ValidationError("Federated path does not exist in irods")
        plen = len(fedpath + '/')
        relpath = relpath[plen:]  # omit /

        # strip resource id from path
        if relpath.startswith(locpath):
            plen = len(locpath)
            relpath = relpath[plen:]  # omit /
        else:
            raise ValidationError("Malformed federated resource path")
    elif path.startswith(locpath):
        # strip optional local path prefix
        if test_exists and not storage.exists(path):
            raise ValidationError("Local path does not exist in irods")
        plen = len(locpath)
        relpath = relpath[plen:]  # strip local prefix, omit /

    # now we have folder/file. We could have gotten this from the input, or
    # from stripping qualification folders. Note that this can contain
    # misnamed header content misinterpreted as a folder unless one tests
    # for existence
    if '/' in relpath:
        folder, base = os.path.split(relpath)
        abspath = get_resource_file_path(resource, base, folder=folder)
        if test_exists and not storage.exists(abspath):
            raise ValidationError("Local path does not exist in irods")
    else:
        folder = None
        base = relpath
        abspath = get_resource_file_path(resource, base, folder=folder)
        if test_exists and not storage.exists(abspath):
            raise ValidationError("Local path does not exist in irods")

    return folder, base


def get_irods_storage(resource):
    """ Clone of new BaseResource.get_irods_storage """
    if is_federated(resource):
        return IrodsStorage("federated")
    else:
        return IrodsStorage()


# TODO: should cache all encountered effective paths somewhere
def get_effective_path(file):
    """
    Return the storage path found for a file.
    """
    resource = file.resource
    istorage = get_irods_storage(resource)

    # First, normalize state of each potential file name.
    # In some cases, the word "None" was used instead of the symbol None!
    file_resource_file_name = file.resource_file.name
    file_fed_resource_file_name = file.fed_resource_file.name
    file_fed_resource_file_name_or_path = file.fed_resource_file_name_or_path
    file_file_folder = file.file_folder

    # convert meaningless values to None
    if file_resource_file_name == "" or \
       file_resource_file_name == "None":
        file_resource_file_name = None
    if file_fed_resource_file_name == "" or \
       file_fed_resource_file_name == "None":
        file_fed_resource_file_name = None
    if file_fed_resource_file_name_or_path == "" or \
       file_fed_resource_file_name_or_path == "None":
        file_fed_resource_file_name_or_path = None

    nnames = []
    if file_resource_file_name is not None:
        nnames.append(file_resource_file_name)
    if file_fed_resource_file_name is not None:
        nnames.append(file_fed_resource_file_name)
    if file_fed_resource_file_name_or_path is not None:
        nnames.append(file_fed_resource_file_name_or_path)

    if len(nnames) > 1:
        print("ERROR: more than one name for file:")
        for n in nnames:
            print("   {}".format(n))
        return None

    # if I find the proper path, this is set.
    inferred_path = None
    orig_path = None

    # go through the options for defining a file
    # check that the file is defined properly according to one of these.

    # part 1: unfederated name can be qualified in several ways
    if file_resource_file_name is not None:
        path = file_resource_file_name
        orig_path = path
        # it is an error for the file to differ from the declared kind of resource
        if is_federated(resource):
            print("ERROR: unfederated file declared for federated resource {} ({}): {}"
                  .format(resource.short_id, resource.resource_type, file_resource_file_name))
            # none of these have been found in the database; no action need be taken
        elif path.startswith(file_path(resource)):
            # fully qualified unfederated name
            try:
                folder, base = path_is_acceptable(file, path, test_exists=True)
                inferred_path = path  # The above step checks that it is correct
            except ValidationError:
                print("ERROR: existing path {} is not conformant for {} ({})"
                      .format(path, resource.short_id, resource.resource_type))
            # print("Found unfederated path {}".format(inferred_path))

        else:  # not fully qualified, for whatever reason. Strip headers and qualify
            if path.startswith("data/contents/"):  # partially qualified path
                # strip data/contents from name
                print("INFO stripping data/contents/ from unfederated path {}".format(path))
                plen = len("data/contents/")
                path = path[plen:]
                # print("INFO result is {}".format(path))

            # unqualified unfederated name
            folder, base = os.path.split(path)
            if folder == "":
                folder = None
            if folder is not None and file_file_folder != folder:
                print("WARNING: declared folder {} is not path folder {} for {} ({})"
                      .format(str(file_file_folder), str(folder), resource.short_id,
                              resource.resource_type))
                print("INFO: Assuming folder is {}".format(str(file_file_folder)))

            if file_file_folder is None:
                inferred_path = get_path_from_short_path(file, base, test_exists=False)
            else:
                inferred_path = get_path_from_short_path(file,
                                                         os.path.join(file_file_folder, base),
                                                         test_exists=False)
            # if we get here, we have a valid inferred path
            if not istorage.exists(inferred_path):
                print("ERROR: inferred path {} does not exist".format(inferred_path))
            else:
                print("INFO: found unqualified unfederated name '{}' with folder {}" +
                      " qualified to '{}'"
                      .format(path, str(file_file_folder), inferred_path))

    if file_fed_resource_file_name is not None:
        path = file_fed_resource_file_name
        orig_path = path
        if not is_federated(resource):
            print("ERROR: federated file declared for unfederated resource {} ({}): {}"
                  .format(resource.short_id, resource.resource_type, path))
            # no action taken
        if path.startswith(file_path(resource)):
            # fully qualified federated name
            # print("INFO: found fully qualified federated name '{}'".format(path))
            try:
                folder, base = path_is_acceptable(file, path, test_exists=True)
                # At this point, path exists
                inferred_path = path
                if folder == "":
                    folder = None
                if folder is not None and file_file_folder != folder:
                    print("WARNING: declared folder {} is not path folder {} for {} ({})"
                          .format(str(file_file_folder), str(folder), resource.short_id,
                                  resource.resource_type))
                    file_file_folder = folder  # we just found the file there
            except ValidationError:
                print("ERROR: existing path {} is not conformant for {} ({})"
                      .format(path, resource.short_id,
                              resource.resource_type))
        elif path.startswith(resource.short_id):
            print("ERROR: unfederated path {} used for federated resource for {} ({})"
                  .format(path, resource.short_id, resource.resource_type))
            # mediation only required if instances pop up during testing.
        elif path.startswith("/"):
            print("ERROR: non-conformant full path {} for federated resource {} ({})"
                  .format(path, resource.short_id, resource.resource_type))
            # no instances so far.
        else:
            if path.startswith("data/contents/"):
                print("WARNING: path {} starts with extra data header for {} ({})"
                      .format(path, resource.short_id, resource.resource_type))
                # strip header
                plen = len("data/contents/")
                path = path[plen:]

            # next thing should be folder
            folder, base = os.path.split(path)
            if folder is not None and folder != file_file_folder:
                print("ERROR: inferred folder {} != folder of record {}, using folder of record"
                      .format(folder, file_file_folder))
            print("INFO: Assuming folder is {}".format(str(file_file_folder)))

            # set fully qualified path
            if file_file_folder is None:
                inferred_path = get_path_from_short_path(file, base, test_exists=False)
            else:
                inferred_path = get_path_from_short_path(file,
                                                         os.path.join(file_file_folder, base),
                                                         test_exists=False)
            # if we get here, we have a valid inferred path
            if not istorage.exists(inferred_path):
                print("ERROR: inferred path {} does not exist".format(inferred_path))
            else:
                print("INFO: found unqualified federated name '{}'" +
                      " with folder '{}' qualified to '{}'"
                      .format(path, str(file_file_folder), inferred_path))

    elif file_fed_resource_file_name_or_path is not None:
        path = file_fed_resource_file_name_or_path
        orig_path = path
        if not is_federated(resource):
            print("WARNING: federated file name or path" +
                  " declared for unfederated resource {} ({}): {}"
                  .format(resource.short_id, resource.resource_type, path))
        if path.startswith('data/contents/'):
            plen = len('data/contents/')
            path = path[plen:]
            print("INFO: data/contents/ stripped from fed name or path: {} for {} ({})"
                  .format(path, resource.short_id, resource.resource_type))
        if path.startswith(file_path(resource)):
            inferred_path = get_path_from_storage_path(file, path, test_exists=False)
        else:
            inferred_path = get_path_from_short_path(file, path, test_exists=False)

    if inferred_path is not None:
        # This existence test is fouled up by a mangled resource name.
        # Invalid istorage object. A name consisting of spaces is the
        # most likely culprit.
        istorage = get_irods_storage(resource)
        if not istorage.exists(inferred_path):
            print("ERROR: inferred path '{}' does not exist".format(inferred_path))
        else:
            pass
            # print("INFO: inferred path '{}' exists".format(inferred_path))

    if inferred_path is None:
        print("ERROR: no valid file name defined for {} ({})"
              .format(resource.short_id, resource.resource_type))

    return inferred_path, file_file_folder, orig_path


def resource_check_irods_files(self, stop_on_error=False, log_errors=True,
                               echo_errors=False, return_errors=False):
    """
    Check whether files in self.files and on iRODS agree

    :param stop_on_error: whether to raise a ValidationError exception on first error
    :param log_errors: whether to log errors to Django log
    :param echo_errors: whether to print errors on stdout
    :param return_errors: whether to collect errors in an array and return them.
    """

    logger = logging.getLogger(__name__)
    istorage = get_irods_storage(self)
    errors = []
    ecount = 0

    if is_federated(self):
        msg = "check_irods_files: federation prefix is {}"\
            .format(self.resource_federation_path)
        if echo_errors:
            print(msg)
        if log_errors:
            logger.info(msg)

    # skip federated resources if not configured to handle these
    if is_federated(self) and not settings.REMOTE_USE_IRODS:
        msg = "check_irods_files: skipping check of federated resource {}"\
            .format(self.short_id)
        if echo_errors:
            print(msg)
        if log_errors:
            logger.info(msg)

    else:

        all_paths = {}
        # Step 1: does every file here refer to an existing file in iRODS?
        for f in self.files.all():
            path, folder, orig = get_effective_path(f)
            if path is not None:
                all_paths[path] = 1  # all_paths is utilized as set
            if path is None or not istorage.exists(path):
                ecount += 1
                msg = ("check_irods_files: django file {} in folder {}," +
                       " resolved to {}, does not exist in iRODS").format(orig, folder, path)
                if echo_errors:
                    print(msg)
                if log_errors:
                    logger.error(msg)
                if return_errors:
                    errors.append(msg)
                if stop_on_error:
                    raise ValidationError(msg)

        # Step 2: does every iRODS file correspond to a record in files?
        error2, ecount2 = __resource_check_irods_directory(self, file_path(self), logger,
                                                           all_paths,
                                                           stop_on_error=stop_on_error,
                                                           log_errors=log_errors,
                                                           echo_errors=echo_errors,
                                                           return_errors=return_errors)
        # TODO: At this point, errors contains reports of Django files that don't exist in iRODS,
        # TODO: while errors2 contains iRODS files that don't exist in Django.
        # Here is where corrections would be made
        errors.extend(error2)
        ecount += ecount2

    # Step 3: does resource exist at all?
    if is_federated(self):
        rpath = os.path.join(self.resource_federation_path, self.short_id)
    else:
        rpath = self.short_id
    if not istorage.exists(rpath):
        ecount += 1
        msg = "iRODS directory for resource {} does not exist at all".format(self.short_id)
        if echo_errors:
            print(msg)
        if log_errors:
            logger.error(msg)
        if return_errors:
            errors.append(msg)

    if ecount > 0:  # print information about the affected resource (not really an error)
        msg = "check_irods_files: affected resource {} type is {}, title is '{}'"\
            .format(self.short_id, self.resource_type, self.metadata.title.value)
        if log_errors:
            logger.error(msg)
        if echo_errors:
            print(msg)
        if return_errors:
            errors.append(msg)

    return errors, ecount  # errors is empty unless return_errors=True


def __resource_check_irods_directory(self, dir, logger, all_paths,
                                     stop_on_error=False, log_errors=True,
                                     echo_errors=False, return_errors=False):
    """
    list a directory and check files there for conformance with django ResourceFiles

    :param all_paths: a set of paths; keys of a dict are the paths.
    :param stop_on_error: whether to raise a ValidationError exception on first error
    :param log_errors: whether to log errors to Django log
    :param echo_errors: whether to print errors on stdout
    :param return_errors: whether to collect errors in an array and return them.

    """
    errors = []
    ecount = 0
    istorage = get_irods_storage(self)
    try:

        listing = istorage.listdir(dir)
        for fname in listing[1]:  # files
            fullpath = os.path.join(dir, fname)
            if fullpath not in all_paths:
                ecount += 1
                msg = "check_irods_files: file {} in iRODs does not exist in Django"\
                    .format(fullpath)
                if echo_errors:
                    print(msg)
                if log_errors:
                    logger.error(msg)
                if return_errors:
                    errors.append(msg)
                if stop_on_error:
                    raise ValidationError(msg)

        for dname in listing[0]:  # directories
            error2, ecount2 = __resource_check_irods_directory(self, os.path.join(dir, dname),
                                                               logger, all_paths,
                                                               stop_on_error=stop_on_error,
                                                               echo_errors=echo_errors,
                                                               log_errors=log_errors,
                                                               return_errors=return_errors)
            errors.extend(error2)
            ecount += ecount2

    except SessionException:
        ecount += 1
        msg = "check_irods_files: listing of iRODS directory {} failed".format(dir)
        if echo_errors:
            print(msg)
        if log_errors:
            logger.error(msg)
        if return_errors:
            errors.append(msg)
        if stop_on_error:
            raise ValidationError(msg)

    return errors, ecount  # empty unless return_errors=True


def resource_dump(resource):
    """ dump the status of a resource to stdout """
    print("contents of {}".format(resource.short_id))
    print("According to Django:")
    print(" resource_federation_path is {}", str(resource.resource_federation_path))
    print(" Files are:")
    for f in resource.files.all():
        print("  f.resource_file.name is {}".format(str(f.resource_file.name)))
        print("  f.fed_resource_file.name is {}".format(str(f.fed_resource_file.name)))
        print("  f.fed_resource_file_name_or_path is {}"
              .format(str(f.fed_resource_file_name_or_path)))
    print("According to iRODS, files are:")
    istorage = get_irods_storage(resource)
    resource_list(istorage, os.path.join(root_path(resource), 'data', 'contents'), depth=1)


def resource_list(istorage, path, depth=0):
    """ recursively list all files in resource """
    prefix = ""
    for i in range(depth):
        prefix += " "
    try:
        contents = istorage.listdir(path)
        print("{}{}: (directory)".format(prefix, path))
        for f in contents[1]:  # files
            print("{} {} (file)".format(prefix, f))
        for f in contents[0]:  # directories
            resource_list(istorage, os.path.join(path, f), depth + 1)

    except SessionException as ex:
        print("{}{}: (directory CANNOT BE LISTED)".format(prefix, path))
        print("{}stdout: {}".format(prefix, ex.stdout))
        print("{}stderr: {}".format(prefix, ex.stderr))


# -*- coding: utf-8 -*-

"""
Check synchronization between iRODS and Django

This checks that:

1. every ResourceFile corresponds to an iRODS file
2. every iRODS file in {short_id}/data/contents corresponds to a ResourceFile

* By default, prints errors on stdout.
* Optional argument --log instead logs output to system log.
"""


class Command(BaseCommand):
    help = "Check synchronization between iRODS and Django."

    def add_arguments(self, parser):

        # a list of resource id's, or none to check all resources
        parser.add_argument('resource_ids', nargs='*', type=str)

        # Named (optional) arguments
        parser.add_argument(
            '--log',
            action='store_true',  # True for presence, False for absence
            dest='log',           # value is options['log']
            help='log errors to system log',
        )

    def handle(self, *args, **options):
        if len(options['resource_ids']) > 0:  # an array of resource short_id to check.
            for rid in options['resource_ids']:
                try:
                    resource = BaseResource.objects.get(short_id=rid)
                except BaseResource.DoesNotExist:
                    msg = "Resource with id {} not found in Django Resources".format(rid)
                    print(msg)

                print("LOOKING FOR ERRORS FOR RESOURCE {}".format(rid))
                resource_dump(resource)
                # resource_check_irods_files(resource, stop_on_error=False,
                #                            echo_errors=not options['log'],
                #                            log_errors=options['log'],
                #                            return_errors=False)

        else:  # check all resources
            print("LOOKING FOR ERRORS FOR ALL RESOURCES")
            for r in BaseResource.objects.all():
                istorage = get_irods_storage(r)
                if not istorage.exists(root_path(r)):
                    print("ERROR: {} does not exist".format(root_path(r)))
                # resource_check_irods_files(r, stop_on_error=False,
                #                            echo_errors=not options['log'],
                #                            log_errors=options['log'],
                #                            return_errors=False)
