#!/usr/bin/env python

from __future__ import print_function

import argparse
import datetime
import logging
import os
import os.path
import random
import re
import sys
from bisect import bisect_left
from collections import defaultdict

logger = logging.getLogger(__name__)


"""
This program scans a perforce versioned file repository as
maintained by a Perforce proxy, in order to keep its size
under control.  It will delete the files with the least
recent access times to bring the number of files or the
total disk space used down to a given limit.
Can be run once a day by a cron script
"""


class DirNode:
    # a node in a directory tree, to keep track of paths
    # and store them in a more efficient way than full strings.
    def __init__(self):
        self.name = None
        self.children = defaultdict(DirNode)
        self.parent = None

    def insert(self, path):
        node = self
        for part in os.path.split(path):
            child = node.children[part]
            child.name = part
            child.parent = node
            node = child
        return node

    def path(self):
        parts = []
        node = self
        while node:
            if node.name:
                parts.append(node.name)
            node = node.parent
        return os.path.join(*reversed(parts))

    def __str__(self):
        return self.path()

    def __lt__(self, other):
        # make it sortable
        return self.path() < other.path()

rootnode = DirNode()


def fake_timestamp(days):
    """
    Create a fake timestamp that is days old
    """
    now = datetime.datetime.utcnow()
    return (now - datetime.timedelta(days=random.randint(0, days))).timestamp()


def find_versioned_files(root, fake=False):
    """
    Look for versioned files in the folder, those ending with known
    Perforce endings for such files.  Return them as a list of
    access time, filesize, pathname, tuples.
    """
    if fake:
        # return fake files for testing
        return [
            (
                fake_timestamp(30),
                random.randint(10, 10000000),
                rootnode.insert("foo"),
                "fakefile%d" % i,
            )
            for i in range(1000)
        ]

    entries = []
    for dirpath, dirnames, filenames in os.walk(root):
        for filename in filenames:
            if not is_versioned(filename):
                continue
            filepath = os.path.join(dirpath, filename)
            s = os.stat(filepath)
            entries.append((s.st_atime, s.st_size, rootnode.insert(dirpath), filename))
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
    """convenience function to add up the sizes of files"""
    return sum(t[1] for t in files)


def cumulative_sum(values):
    """create a cumulative sum, e.g. of file sizes"""
    it = iter(values)
    last = next(it)
    yield last
    for v in it:
        last += v
        yield last


def find_size_limit_old(files, limit_size):
    """
    Find the place in the list where the size limit is exceeded
    """
    size = sum_size(files)
    for i, f in enumerate(files):
        if size <= limit_size:
            return i  # we found the cut-off point
        size -= f[1]
    return i


def find_size_limit(cumulative_sizes, limit_size):
    """
    Using cumulative sizes, find the place of old files to throw away
    Return the first index that we keep
    """
    # what is the cumulative target to throw away?
    target = cumulative_sizes[-1] - limit_size

    # special case for zero or negative values: we keep everything
    if target <= 0:
        return 0

    # find index where cumulative size is greater or equal to what is needed to throw away
    return find_ge(cumulative_sizes, target) + 1


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
            dirnode, filename = f[2], f[3]
            filename = os.path.join(dirnode.path(), filename)
            os.unlink(filename)
            n += 1
        except OSError:
            logger.warning("could not remove file %r", filename)
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
    parser.add_argument(
        "--max-size", type=unitsize, default=0, help="maximum total size of files"
    )
    parser.add_argument(
        "--max-size-hard",
        type=unitsize,
        default=0,
        help="maximum total size of files, overriding --min-age",
    )
    parser.add_argument(
        "--max-count", type=int, default=0, help="maximum total number of files"
    )
    parser.add_argument(
        "--min-age", type=int, help="don't throw away anything younger than this (days)"
    )
    parser.add_argument(
        "--max-age", type=int, help="don't keep anything older than this (days)"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="be more verbose")
    parser.add_argument("--fake", action="store_true", help="dry run with fake files")

    args = parser.parse_args()
    if not (args.max_size or args.max_count or args.max_age):
        parser.error("At least one of --max-size, --max-count, --max-age required")
    if args.fake:
        args.dry_run = True

    print("Trimming P4 versioned file folder %r:" % args.root)
    if args.verbose:
        print("  max-size: %s" % format_size2(args.max_size))
        print("  max-count: %r" % args.max_count)
        print("  max-age: %r" % args.max_age)
        print("  min-age: %r" % args.min_age)
        print("  max-size-hard: %s" % format_size2(args.max_size_hard))

    files = find_versioned_files(args.root, args.fake)

    size = sum_size(files)
    print("Found %d files, %s" % (len(files), format_size2(size)))

    if not files:
        return  # without any files, just quit

    # sort files according to access time, oldest first (lowest timestamp)
    files.sort(key=lambda x: x[0])

    # and create the cumulative sum
    cumulative_sizes = list(cumulative_sum([f[1] for f in files]))

    # now apply the criteria
    i_keep = 0  # the index of the oldest (lowest timestamp) file we keep
    now = datetime.datetime.utcnow()

    # max-size, max-count and max-age, all work to create a lower bound on number of
    # files to store.  That is, we take the largest index (youngest) file that these produce.
    if args.max_count > 0:
        i = i_keep
        i_keep = max(i_keep, len(files) - args.max_count)
        if i_keep != i:
            print(
                "--max-count=%r limiting kept files to %d"
                % (args.max_count, len(files) - i_keep)
            )
        elif args.verbose:
            print("--max-count=%r not limiting kept files" % (args.max_count,))

    if args.max_size > 0:
        i = i_keep
        i_keep = max(i_keep, find_size_limit(cumulative_sizes, args.max_size))
        if i_keep != i:
            print(
                "--max-size=%s limiting kept files to %d"
                % (format_size2(args.max_size), len(files) - i_keep)
            )
        elif args.verbose:
            print(
                "--max-size=%r not limiting kept files" % (format_size2(args.max_size),)
            )

    if args.max_age is not None:
        i = i_keep
        limit = now - datetime.timedelta(days=args.max_age)
        i_keep = max(i_keep, find_atime_limit(files, timestamp_from_datetime(limit)))
        if i_keep != i:
            print(
                "--max-age=%r limiting kept files to %d"
                % (args.max_age, len(files) - i_keep)
            )
        elif args.verbose:
            print("--max-age=%r not limiting kept files" % (args.max_age,))

    # But now, for min age, it overrides all this.  We don't throw away anything younger
    # than min age
    if args.min_age:
        i = i_keep
        limit = now - datetime.timedelta(days=args.min_age)
        i_keep = min(i_keep, find_atime_limit(files, timestamp_from_datetime(limit)))
        if i_keep != i:
            print(
                "--min-age=%r forcing kept files to %d"
                % (args.min_age, len(files) - i_keep)
            )
        elif args.verbose:
            print("--min-age=%r not forcing kept files" % (args.min_age,))

    # Even with all this, particularly with min-age, we might still have too many files.
    # So, if we have a hard limit
    if args.max_size_hard > 0:
        i = i_keep
        i_keep = max(i_keep, find_size_limit(cumulative_sizes, args.max_size_hard))
        if i_keep != i:
            print(
                "--max-size-hard=%s limiting kept files to %d"
                % (format_size2(args.max_size_hard), len(files) - i_keep)
            )
        elif args.verbose:
            print(
                "--max-size-hard=%r not limiting kept files"
                % (format_size2(args.max_size_hard),)
            )

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


def test():
    # create a list of files with access times and sizes
    files = [
        (1, 100, "dir", "a"),
        (2, 200, "dir", "b"),
        (3, 300, "dir", "c"),
        (4, 400, "dir", "d"),
        (5, 500, "dir", "e"),
    ]
    files.sort(key=lambda x: x[0])
    cumulative_sizes = list(cumulative_sum([f[1] for f in files]))

    assert cumulative_sizes[-1] == sum(f[1] for f in files)

    # Files are sorted by access time, oldest is first.  When throwing away files, we work with the index
    # of the first file we keep.

    # test find_size_limit().  The method returns the first index that we keep.

    # for size 0, we won't keep anything, so the first index to keep is 5 (the first index past the last)
    limit = find_size_limit(cumulative_sizes, 0)
    assert limit == 5
    # for size 10000, we keep all files, so the first index to keep is 0
    assert find_size_limit(cumulative_sizes, 10000) == 0
    # for size 900, we keep the last two files, so the first index to keep is 3
    assert find_size_limit(cumulative_sizes, 900) == 3
    # for size 950, we still only keep the last two files: 3
    assert find_size_limit(cumulative_sizes, 950) == 3
    # only when we reach 1200, we keep the last three: 2
    assert find_size_limit(cumulative_sizes, 1200) == 2

    # check time limit
    # for time 0, we keep all files, so the first index to keep is 0
    assert find_atime_limit(files, 0) == 0
    # for time 1, we keep all files, so the first index to keep is 0
    assert find_atime_limit(files, 1) == 0
    # for time 2, we throw away the oldest file, so the first index to keep is 1
    assert find_atime_limit(files, 2) == 1
    # for time 5, we throw away all but the last file, so the first index to keep is 4
    assert find_atime_limit(files, 5) == 4
    # for time 6, we throw away all files, so the first index to keep is 5
    assert find_atime_limit(files, 6) == 5

    # test the dirnode, by walking some paths
    found = []
    if len(sys.argv) > 2:
        for dirpath, _, filenames in os.walk(sys.argv[2]):
            for filename in filenames:
                name = os.path.join(dirpath, filename)
                node = rootnode.insert(name)
                assert node.path() == name
                found.append(node)
            if len(found) > 100:
                break
    print("walked %d paths" % len(found))
    # make sure all nodes are unique
    assert len(found) == len(set(found))


if __name__ == "__main__":
    if len(sys.argv) and sys.argv[1] == "--test":
        test()
        sys.exit(0)

    logging.basicConfig()
    main()
