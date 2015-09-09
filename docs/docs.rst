.. _docs:

Documentation
=============


Documentation uses `Sphinx`_.

.. _Sphinx: http://sphinx-doc.org/tutorial.html

Editing the docs
----------------------

1. Install requirements (``watchdog`` and ``sphinx``)::

    pip install -r requirements/docs.txt

2. Compile the docs::

    make servedocs

It will compile the docs with sphinx, open the compiled HTML docs in your browser and start watching for changes in the doc files (\*.rst) and recompiling the docs.

3. Edit the doc files.

Read more about `reStructuredText`_ syntax.

.. _reStructuredText: http://sphinx-doc.org/rest.html

