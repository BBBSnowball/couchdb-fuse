import os, time
from pytest import fixture, yield_fixture

#NOTE there must be a CouchDB on localhost that grants public
#     access to the "xyz" database (which must exist, of course)

@yield_fixture(scope="module")
def couchfs():
  os.system("fusermount -u x")
  assert os.system("mkdir -p x") == 0
  assert os.system("touch x/not-a-couchfs") == 0
  assert os.system("python couchmount.py -ononempty -d 'D:http://localhost:5984/xyz' x >log 2>&1 &") == 0
  print "mounted"
  time.sleep(0.3)
  os.system("rm -rf x/*")
  try:
    yield "x"
  finally:
    print "unmounted"
    os.system("fusermount -u x")

class TestCouchFSDatabase(object):
  def test_this_is_a_couchfs(self, couchfs):
    # CouchFS has a know limitation: The root directory
    # always has a ctime of 0. We use this to make sure
    # that we are testing a CouchFS and not some arbitrary
    # filesystem. This is a safe guard in case the mount
    # fails.
    assert os.stat(couchfs).st_ctime == 0

    # We also create a file that should only be visible, if the mount doesn't fail.
    assert "not-a-couchfs" not in os.listdir(couchfs)

  def test_touch(self, couchfs):
    assert os.system("touch %s/a" % couchfs) == 0
    assert "a" in os.listdir(couchfs)

  def test_mkdir(self, couchfs):
    assert os.system("mkdir %s/y" % couchfs) == 0
    assert "y" in os.listdir(couchfs)
    assert os.listdir("%s/y" % couchfs) == []

  def test_read_and_write(self, couchfs):
    filename = "%s/b" % couchfs
    with open(filename, "w") as f:
      f.write("abc\n")
      f.write("def\n")

    with open(filename) as f:
      content = f.read()

    assert content == "abc\ndef\n"

  def test_reading_nonexistent_file(self, couchfs):
    filename = "%s/does_not_exist" % couchfs
    try:
      open(filename)
      assert False, "exception expected"
    except IOError, e:
      assert e.args == (2, 'No such file or directory')

  def test_work_with_nested_directory(self, couchfs):
    for d in ("n", "n/a", "n/b"):
      assert os.system("mkdir %s/%s" % (couchfs, d)) == 0
    for f in ("n/a/1", "n/a/2", "n/b/3", "n/c", "n/b/1"):
      assert os.system("touch %s/%s" % (couchfs, f)) == 0

    assert "n" in os.listdir(couchfs)
    assert sorted(os.listdir("%s/n"   % couchfs)) == ["a", "b", "c"]
    assert sorted(os.listdir("%s/n/a" % couchfs)) == ["1", "2"]
    assert sorted(os.listdir("%s/n/b" % couchfs)) == ["1", "3"]
    assert sorted(os.listdir("%s/n/b" % couchfs)) == ["1", "3"]
    try:
      os.listdir("%s/n/c" % couchfs)
      assert False, "exception expected"
    except OSError, e:
      assert e.args == (20, "Not a directory",)
