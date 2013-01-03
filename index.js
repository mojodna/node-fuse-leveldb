var constants = require("constants"),
    _path = require("path");

var async = require("async"),
    f4js = require("fuse4js"),
    levelup = require("levelup");

var S_IFREG = 0100000,
    S_IFDIR = 0040000;

var S_ISREG = function(m) {
  return (m & S_IFREG) == S_IFREG;
};

var S_ISDIR = function(m) {
  return (m & S_IFDIR) == S_IFDIR;
};

var db;

var stat = function(path, callback) {
  db.get("@" + path, { valueEncoding: "json" }, function(err, stat) {
    if (err) {
      callback(err);
      return;
    }

    stat.atime = new Date(stat.atime);
    stat.mtime = new Date(stat.mtime);
    stat.ctime = new Date(stat.ctime);

    callback(null, stat);
    return;
  });
};

var ls = function(path, callback) {
  // TODO use an in-memory cache to avoid collisions
  db.get("#" + path, { valueEncoding: "json" }, function(err, val) {
    if (err) {
      callback(err);
      return;
    }

    callback(null, val);
    return;
  });
};

// TODO there is a giant race condition around value manipulation (files,
// directory listings), as values are not changed in-place, but rather loaded,
// modified, and saved.

/**
 * getattr() system call handler.
 */
var getattr = function(path, callback) {
  stat(path, function(err, stat) {
    if (err) {
      callback(-constants.ENOENT);
      return;
    }

    callback(0, stat);
    return;
  });
};

/**
 * readdir() system call handler.
 */
var readdir = function(path, callback) {
  stat(path, function(err, stat) {
    if (err) {
      callback(-constants.ENOENT);
      return;
    }

    if (!S_ISDIR(stat.mode)) {
      callback(-constants.EINVAL);
      return;
    }

    ls(path, function(err, val) {
      if (err) {
        callback(-constants.EINVAL);
        return;
      }

      callback(0, val);
      return;
    });
  });
};

/**
 * open() system call handler.
 */
var open = function(path, flags, callback) {
  console.log("Open %s (%j)", path, flags);
  // TODO implement flags
  stat(path, function(err, stat) {
    if (err) {
      callback(-constants.ENOENT);
      return;
    }

    callback(0);
    return;
  });
};

/**
 * read() system call handler.
 */
var read = function(path, offset, len, buf, fh, callback) {
  console.log("Read %s", path);
  stat(path, function(err, stat) {
    if (err) {
      callback(-constants.ENOENT);
      return;
    }

    if (!S_ISREG(stat.mode)) {
      callback(-constants.EPERM); // not a file
      return;
    }

    // TODO will LevelDB allow partial reads of values?
    // potentially using Slices
    // TODO this may also produce corrupt values when the file changes
    // underneath, so it may be sensible to associate a snapshot in open() with
    // a filehandle
    db.get(path, { valueEncoding: "binary" }, function(err, data) {
      if (err) {
        console.warn("stat succeeded, but no data for path '%s':", path, err);
        callback(-constants.ENOENT);
        return;
      }

      var errNo = 0;
      var maxBytes;

      if (offset < data.length) {
        maxBytes = data.length - offset;

        if (len > maxBytes) {
          len = maxBytes;
        }

        data.copy(buf, 0, offset, offset + len);
        errNo = len;
      }

      // TODO update file's atime

      callback(errNo);
      return;
    });
  });
};

/**
 * write() system call handler.
 */
var write = function(path, offset, len, buf, fh, callback) {
  console.log("Write %s", path);
  stat(path, function(err, stat) {
    if (err) {
      callback(-constants.ENOENT);
      return;
    }

    if (!S_ISREG(stat.mode)) {
      callback(-constants.EPERM);
      return;
    }

    db.get(path, { valueEncoding: "binary" }, function(err, data) {
      if (err) {
        console.warn("stat succeeded, but no data for path '%s':", path, err);
        callback(-constants.ENOENT);
        return;
      }

      var target = new Buffer(offset + len);

      if (offset > 0) {
        // fill in the beginning of the file
        data.copy(target, 0, 0, offset - 1);
      }

      buf.copy(target, offset, 0, len);

      // update attributes
      stat.atime = new Date();
      stat.mtime = new Date();
      stat.size = target.length;

      var ops = [
        { type: "put", key: path, value: target },
        { type: "put", key: "@" + path, value: new Buffer(JSON.stringify(stat)) }
      ];

      db.batch(ops, { valueEncoding: "binary" }, function(err) {
        if (err) {
          console.warn("Error during write():", err);
          // TODO correct this error code
          callback(-constants.EINVAL);
          return;
        }

        callback(len)
      });
    });
  });
};

/**
 * release() system call handler.
 */
var release = function(path, fh, callback) {
  callback(0);
  return;
};

/**
 * create() system call handler.
 */
var create = function(path, mode, callback) {
  console.log("Create %s", path);
  async.parallel([
    function(callback) {
      stat(path, function(err, stat) {
        // swallow the error message since we don't care
        callback(null, stat);
      });
    },
    function(callback) {
      ls(_path.dirname(path), function(err, list) {
        list = list || [];

        // swallow the error, since that just means it's an empty directory
        callback(null, list);
      });
    }
  ], function(err, results) {
    var stat = results[0];
    var list = results[1];

    if (stat) {
      console.log("%s already exists.", path);
      callback(-constants.EEXIST);
      return;
    }

    if (!list) {
      list = [];
    }

    var now = new Date();

    // set the attributes of the new file
    stat = {
      mode: mode | S_IFREG,
      atime: now,
      ctime: now,
      mtime: now,
      size: 0
    };

    // add this file to the directory listing
    list.push(_path.basename(path));

    var ops = [
      { type: "put", key: "@" + path, value: stat },
      // "" should be truly blank, but we're forced into a single encoding here
      { type: "put", key: path, value: "" },
      { type: "put", key: "#" + _path.dirname(path), value: list }
    ];

    db.batch(ops, { valueEncoding: "json" }, function(err) {
      if (err) {
        console.warn("Error during create():", err);
        // TODO correct this error code
        callback(-constants.EINVAL);
        return;
      }

      console.log("Create completed.");
      callback(0);
      return;
    });
  });
};

/**
 * unlink() system call handler
 */
var unlink = function(path, callback) {
  stat(path, function(err, stat) {
    if (err) {
      callback(-constants.ENOENT);
      return;
    }

    if (!S_ISREG(stat.mode)) {
      callback(-console.EPERM);
      return;
    } else {
      var ops = [
        { type: "del", key: "@" + path },
        { type: "del", key: path }
      ];

      db.batch(ops, function(err) {
        if (err) {
          console.warn("Error during unlink():", err);
          // TODO correct this error code
          callback(-constants.EINVAL);
          return;
        }

        callback(0);
        return;
      });
    }
  });
};

/**
 * mkdir() system call handler
 */
var mkdir = function(path, mode, callback) {
  console.log("mkdir %s (%d)", path, mode);
  stat(path, function(err, exists) {
    if (exists) {
      callback(-constants.EPERM);
      return;
    }

    stat(_path.dirname(path), function(err, stat) {
      if (err) {
        callback(-constants.ENOENT);
        return;
      }

      if (!S_ISDIR(stat.mode)) {
        callback(-constants.EPERM);
        return;
      }

      ls(_path.dirname(path), function(err, list) {
        list = list || [];

        var now = new Date();

        stat = {
          mode: 0777 | S_IFDIR, // TODO respect mode (cp -R doesn't work if it is respected somehow)
          atime: now,
          ctime: now,
          mtime: now,
          size: 4096 // TODO constant if this is correct, otherwise...?
        };

        list.push(_path.basename(path));

        var ops = [
          { type: "put", key: "@" + path, value: stat },
          { type: "put", key: "#" + _path.dirname(path), value: list }
        ];

        // TODO updating the list entry represents a race
        db.batch(ops, { valueEncoding: "json" }, function(err) {
          if (err) {
            console.warn("Error during mkdir():", err);
            // TODO correct this error code
            callback(-constants.EINVAL);
            return;
          }

          callback(0);
          return;
        });
      });
    });
  });
};

/**
 * rmdir() system call handler
 */
var rmdir = function(path, callback) {
  stat(path, function(err, stat) {
    if (err) {
      callback(-constants.ENOENT);
      return;
    }

    if (!S_ISDIR(stat.mode)) {
      callback(-console.EPERM);
      return;
    } else {
      ls(_path.dirname(path), function(err, list) {
        list.splice(list.indexOf(_path.basename(path)), 1);

        var ops = [
          { type: "del", key: "@" + path },
          { type: "del", key: path },
          { type: "put", key: "@" + _path.dirname(path), value: list }
        ];

        // TODO removing the list entry represents a race
        db.batch(ops, {valueEncoding: "json" }, function(err) {
          if (err) {
            console.warn("Error during unlink():", err);
            // TODO correct this error code
            callback(-constants.EINVAL);
            return;
          }
        });
      });
    }
  });
};

var init = function(callback) {
  db = levelup("./fuse.db", {
    createIfMissing: true,
    errorIfExists: false
  }, function(err, db) {
    if (err) {
      console.error("Error while initializing:", err);
      process.exit(1);
    }

    var now = new Date();

    var root = {
      mode: 0777 | S_IFDIR,
      atime: now,
      mtime: now,
      ctime: now,
      size: 4096
    };

    db.put("@/", root, { valueEncoding: "json" }, function(err) {
      console.log("fuse-leveldb initialized.");

      callback();
    });
  });
};

var destroy = function(callback) {
  db.close(callback);
};

var handlers = {
  getattr: getattr,
  readdir: readdir,
  open: open,
  read: read,
  write: write,
  release: release,
  create: create,
  unlink: unlink,
  // rename: rename, // TODO implement
  mkdir: mkdir,
  rmdir: rmdir,
  init: init,
  destroy: destroy
};

f4js.start("/tmp/leveldb", handlers, false);
