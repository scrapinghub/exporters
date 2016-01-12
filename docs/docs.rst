.. _docs:

Documentation
=============


Documentation uses `Sphinx`_.

.. _Sphinx: http://sphinx-doc.org/tutorial.html


Policies
--------

Don’t use docstrings for documenting classes, or methods which are already documented in
the official documentation.
Do use docstrings for documenting functions not present in the official documentation.


Editing the docs
----------------

1. Install requirements (``watchdog`` and ``sphinx``)::

    pip install -r requirements/docs.txt

2. Compile the docs::

    make docs

It will compile the docs with sphinx, open the compiled HTML docs in your browser and start watching for changes in the doc files (\*.rst) and recompiling the docs.

3. Edit the doc files.

Read more about `reStructuredText`_ syntax.

.. _reStructuredText: http://sphinx-doc.org/rest.html

