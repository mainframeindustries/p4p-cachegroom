#! /bin/env python

from __future__ import print_function
import os
import re
import os.path
import argparse
import datetime
import logging
from bisect import bisect_left

logger = logging.getLogger(__name__)


"""
This program scans a perforce versioned file repository as
maintained by a Perforce proxy, in order to keep its size
under control.  It will delete the files with the least
recent access times to bring the number of files or the
total disk space used down to a given limit.
Can be run once a day by a cron script
"""


def find_versioned_files(root):
    """
    Look for versioned files in the folder, those ending with known
    Perforce endings for such files.  Return them as a list of
    access time, filesize, pathname, tuples.
    """
    entries = []
    for dirpath, dirnames, filenames in os.walk(root):
        for filename in filenames:
            if not is_versioned(filename):
                continue
            filepath = os.path.join(dirpath, filename)
            s = os.stat(filepath)
            entries.append((s.st_atime, s.st_size, filepath))
    return entries

# text files end with ,v.
# individual binary files are in folders ending with ,d and
# have names like 1.n where n is the version number.
# all can have a .gz tacked on.
BIN = re.compile(r"1\.\d+")
def is_versioned(filename):
    if filename.endswith(",v") or filename.endswith(".gz"):
        return True
    if BIN.match(filename):
        return True
    return False

def sum_size(files):
    """ convenience function to add up the sizes of files """
    return sum(t[1] for t in files)


def find_size_limit(files, limit_size):
    """
    Find the place in the list where the size limit is exceeded
    """
    size = sum_size(files)
    for i, f in enumerate(files):
        if size <= limit_size:
            return i  # we found the cut-off point
        size -= f[1]
    return i


def find_atime_limit(files, atime):
    """
    Find the place in the list where the access time is equal or higher than given
    If everything is older than time (lower access time) then return len(files)
    """
    target = (atime, 0, "")
    return find_ge(files, target)


# this comes from bisect documentation
def find_ge(a, x):
    "Find leftmost item greater than or equal to x"
    i = bisect_left(a, x)
    return i


def unlink_files(files):
    """
    Unlinks files from the list of files until criteria are met regarding
    number of files or filesize.  Relies on the order of the list, so that
    to delete the oldest files, they should be sorted by access time
    """
    n = 0
    for f in files:
        try:
            os.unlink(f[2])
            n += 1
        except OSError:
            logger.warning("could not remove file %r", f[2])
    return n


def format_size(s):
    """
    helper to format large numbers as powers of 1024
    """
    pre = ["Ki", "Mi", "Gi", "Ti"]
    prefix = ""
    ss = float(s)
    while ss >= 1024 and pre:
        prefix = pre[0]
        del pre[0]
        ss /= 1024
    return ss, prefix


def format_size2(size):
    return "%.2f %sB" % format_size(size)


def timestamp_from_datetime(dt):
    timestamp = (dt - datetime.datetime(1970, 1, 1)).total_seconds()
    return timestamp


def main():
    def unitsize(a):
        """
        helper to parse sizes such as 100G or 1T
        """
        size = a
        for i, unit in enumerate("KMGT"):
            if a.endswith(unit):
                size = a[:-1]
                break
        else:
            i = -1
        try:
            size = float(size)
        except ValueError:
            raise argparse.ArgumentTypeError(
                "%s must be a number, optionally followed by K, M or G" % a
            )
        return size * 1024 ** (i + 1)

    parser = argparse.ArgumentParser(
        description="clean a perforce proxy cache directory"
    )
    parser.add_argument("root", help="root of versioned files")

    parser.add_argument("--dry-run", action="store_true", help="do not delete anything")
    parser.add_argument("--max-size", type=unitsize, help="maximum total size of files")
    parser.add_argument("--max-count", type=int, help="maximum total number of files")
    parser.add_argument(
        "--min-age", type=int, help="don't throw away anything younger than this (days)"
    )
    parser.add_argument(
        "--max-age", type=int, help="don't keep anything older than this (days)"
    )

    args = parser.parse_args()
    if not (args.max_size or args.max_count or args.max_age):
        parser.error("At least one of --max-size, --max-count, --max-age required")

    print("Trimming P4 versioned file folder %r:" % args.root)

    files = find_versioned_files(args.root)
    size = sum_size(files)
    print("Found %d files, %s" % (len(files), format_size2(size)))

    # sort files according to access time, oldest first (lowest timestamp)
    files.sort()

    # now apply the criteria
    i_keep = 0  # the index of the oldest (lowest timestamp) file we keep
    now = datetime.datetime.utcnow()

    # max-size, max-count and min age, all work to create a lower bound on number of
    # files to store.  That is, we take the largest index (youngest) file that these produce.
    if args.max_count is not None:
        i = i_keep
        i_keep = max(i_keep, len(files) - args.max_count)
        if i_keep != i:
            print("--max-count=%r limiting kept files to %d" % (args.max_count, len(files) - i_keep))
        else:
            print("--max-count=%r not limiting kept files" % (args.max_count,))

    if args.max_size is not None:
        i = i_keep
        i_keep = max(i_keep, find_size_limit(files, args.max_size))
        if i_keep != i:
            print("--max-size=%s limiting kept files to %d" % (format_size2(args.max_size), len(files) - i_keep))
        else:
            print("--max-size=%r not limiting kept files" % (format_size2(args.max_size),))

    if args.max_age is not None:
        i = i_keep
        limit = now - datetime.timedelta(days=args.max_age)
        i_keep = max(i_keep, find_atime_limit(files, timestamp_from_datetime(limit)))
        if i_keep != i:
            print("--max-age=%r limiting kept files to %d" % (args.max_age, len(files) - i_keep))
        else:
            print("--max-age=%r not limiting kept files" % (args.max_age,))

    # But now, for min age, it overrides all this.  We don't throw away anything younger
    # than min age
    if args.min_age:
        i = i_keep
        limit = now - datetime.timedelta(days=args.min_age)
        i_keep = min(i_keep, find_atime_limit(files, timestamp_from_datetime(limit)))
        if i_keep != i:
            print("--min-age=%r forcing kept files to %d" % (args.min_age, len(files) - i_keep))
        else:
            print("--min-age=%r not forcing kept files" % (args.min_age,))

    # perform the action
    if not args.dry_run:
        print("deleting %d files" % i_keep)
        n_removed = unlink_files(files[:i_keep])
    else:
        print("not deleting %d files (dry run)" % i_keep)
        n_removed = i_keep

    # output some statistics
    for what, part in ("Unlinked", files[:n_removed]), ("Remaining", files[n_removed:]):
        size = sum_size(part)
        count = len(part)
        print("%s:" % what)
        print("  %6d files, %s" % (count, format_size2(size)))
        if count:
            oldest = datetime.datetime.utcfromtimestamp(part[0][0])
            youngest = datetime.datetime.utcfromtimestamp(part[-1][0])
            print("  atime: %s to %s" % (oldest.isoformat(), youngest.isoformat()))


if __name__ == "__main__":
    logging.basicConfig()
    main()
