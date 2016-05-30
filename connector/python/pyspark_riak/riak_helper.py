_helper = None

def helper(ctx):
    global _helper

    if not _helper:
        _helper = load_class(ctx, "com.basho.riak.spark.util.python.RiakPythonHelper").newInstance()

    return _helper

def load_class(ctx, name):
    return ctx._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass(name)


