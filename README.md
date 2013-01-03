# fuse-leveldb

**EXPERIMENTAL**

I am a [LevelDB](http://code.google.com/p/leveldb/)-backed
[FUSE](http://fuse.sourceforge.net/) filesystem.  I am slow, especially with
large files, as each partial read/write requires that the entire contents be
loaded into memory. Assuming LevelDB supports partial reads of values, support
would need to be added to [LevelUP](https://github.com/rvagg/node-levelup).

The idea was that this might perform well with lots of small files and that
indexed tracking of `atime` could be done, making cache trimming more
efficient. Probably not, but it was a fun exercise nonetheless.

**WARNING** There are still race conditions present. Also, `rename` isn't
implemented.

## Installation

### OS X

1. Install [FUSE for OS X](http://osxfuse.github.com/).
2. `npm install`
