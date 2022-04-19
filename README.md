# p4p-cachegroom
Scripts to manage the size of a Perforce Proxy cache.

The Perforce Proxy stores versioned files in a local cache directory but performs no lifetime or size policy
housekeeping.  The script provided will perform that.  It can be usefully called once a day by `cron`
to set limits on the cache.

## Usage

```sh
python cachegroom.py <cache-root> [--dry-run] [--max-size=M] [--max-count=C] [--min-age=m] [--max-age=M]
```

This will look for versioned perforce file in the `cache-root` folder and apply the provide policy.

- `--max-size` and `max-count` set the baseline limit of the number of items to keep.
  Size can be specified in bytes, or as `K` (kilobytes), `M` (megabytes), `G` (gigabytes) or `T` (terabytes), e.g. `--max-size=500G`  
- `--max-age` can be used to discard anything that hasn't been accessed for the specified number of days.
- `--min-age` can require files to have been left untouched for at least the given number of days before being removed.  This overrides the baseline limits given.

Using set notation, if `min-age`, `max-size` , `max-count` and `max-age` represents the files being kept if that option
is specified alone, then the total (assuming all options are specified) can be found by:

`Union(min_age, Intersection(max-size, max-count, max-age))`
